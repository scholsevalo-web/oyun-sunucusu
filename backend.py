import asyncio
import json
import os
import random
import secrets
import string
import time

import websockets

ROOM_CAPACITY = 10          # toplam slot (mavi + kırmızı)
TEAM_MAX = 5                # takım başına en fazla slot
GAMES = {"prison", "fps"}

rooms = {}    # oda_kodu -> oda sözlüğü
clients = {}  # websocket -> {"room": kod, "id": oyuncu_id}

# ------------------------------------------------------- sunucu geri sayımı
# freeze (hazırlık) süresini sunucu sayar: herkes aynı süreyi görür.
# timeLeft > 5 iken ekranı açık olmayan (heartbeat göndermeyen) oyuncu varsa
# sayım durur; 5 ve altında kalan son kısım her durumda sayılır.
HEARTBEAT_TIMEOUT = 6.0     # bu kadar saniye heartbeat gelmeyen oyuncu "kapalı" sayılır
COUNTDOWN_FLOOR = 5.0       # bu değerin altında ekran kapalı olsa da sayım devam eder
FREEZE_TIME = 5.0           # her raund öncesi hazırlık süresi (istemci ile aynı olmalı)


def screen_open(room):
    """Kalp atışı bilgisi olmayanlar 'açık' sayılır; sadece açıkça 'kapalı'
    bildiren (open=False heartbeat'i gelen ve süresi henüz geçmemiş) oyuncular bloklar."""
    now = time.monotonic()
    for p in room["players"]:
        b = p.get("beat", -1)
        if b == 0.0:
            return False   # açıkça kapalı bildirmiş
        if b > 0.0 and now - b > HEARTBEAT_TIMEOUT:
            return False   # açık bildirmişti ama süresi çok eskidi (yeni open gelmedi)
    return True


async def countdown_task(room):
    """playing fazında freeze süresini sunucu tarafında ilerletir."""
    try:
        while True:
            await asyncio.sleep(0.25)
            if room["phase"] != "playing" or not room["players"]:
                continue
            gs = room.get("gameState")
            if not gs or gs.get("phase") != "freeze":
                continue
            t = gs.get("timeLeft", 0)
            if t <= 0:
                continue
            if t > COUNTDOWN_FLOOR and not screen_open(room):
                continue  # ekranı kapalı oyuncu var, bekle
            gs["timeLeft"] = max(0.0, t - 0.25)
            await broadcast(room, {"type": "countdown", "phase": gs["phase"], "timeLeft": gs["timeLeft"], "round": gs.get("round", 0)})
    except asyncio.CancelledError:
        raise


countdowns = {}  # kod -> task


# ---------------------------------------------------------------- yardımcılar
def generate_room_code():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if code not in rooms:
            return code


def clean_name(raw, room=None):
    name = str(raw or "").strip()[:14] or "Misafir"
    if room:
        taken = {p["name"] for p in room["players"]}
        base, n = name, 2
        while name in taken:
            name = f"{base[:11]} {n}"
            n += 1
    return name


def team_sizes(room):
    s = [0, 0]
    for p in room["players"]:
        s[p["team"]] += 1
    return s


def pick_team(room, preferred=None):
    """Katılan oyuncu için takım seç: tercih > en boş takım."""
    sizes = team_sizes(room)
    if preferred in (0, 1) and sizes[preferred] < room["teamSizes"][preferred]:
        return preferred
    return 0 if sizes[0] <= sizes[1] else 1


def reflow_teams(room):
    """Slot değişiminde taşan oyuncuları boş slotlara kaydır."""
    sizes = team_sizes(room)
    moved = True
    while moved:
        moved = False
        for t in (0, 1):
            while sizes[t] > room["teamSizes"][t]:
                other = 1 - t
                if sizes[other] < room["teamSizes"][other]:
                    p = next(p for p in room["players"] if p["team"] == t)
                    p["team"] = other
                    sizes[t] -= 1
                    sizes[other] += 1
                    moved = True
                else:
                    room["teamSizes"][t] = sizes[t]
                    break


def room_state(room):
    return {
        "type": "room_state",
        "code": room["code"],
        "phase": room["phase"],
        "game": room["game"],
        "seed": room["seed"],
        "host": room["players"][0]["id"],
        "maxPlayers": ROOM_CAPACITY,
        "teamSizes": list(room["teamSizes"]),
        "players": [
            {"id": p["id"], "name": p["name"], "team": p["team"]}
            for p in room["players"]
        ],
    }


async def safe_send(ws, msg):
    try:
        await ws.send(msg)
    except Exception:
        pass  # kopmuş bağlantı; finally bloğu temizleyecek


async def send(ws, obj):
    await safe_send(ws, json.dumps(obj, separators=(",", ":")))


async def broadcast(room, obj, exclude=None, only=None):
    msg = json.dumps(obj, separators=(",", ":"))
    targets = [
        p["ws"]
        for p in room["players"]
        if p["ws"] is not exclude and (only is None or p["id"] == only)
    ]
    if targets:
        await asyncio.gather(*(safe_send(ws, msg) for ws in targets))


async def safe_handle(ws, data):
    action = data.get("action")
    if action == "heartbeat":
        info = clients.get(ws)
        room = rooms.get(info["room"]) if info else None
        if room:
            me = next((p for p in room["players"] if p["ws"] is ws), None)
            if me:
                if data.get("open") is True:
                    me["beat"] = time.monotonic()      # ekranı açık: sayım akabilir
                elif "beat" in me:
                    me["beat"] = 0.0                    # ekranı kapalı: sayımı durdur
        return
    await handle(ws, data)


async def remove_client(ws):
    info = clients.pop(ws, None)
    if not info:
        return
    room = rooms.get(info["room"])
    if not room:
        return
    room["players"] = [p for p in room["players"] if p["ws"] is not ws]
    if not room["players"]:
        rooms.pop(room["code"], None)
        return
    await broadcast(room, room_state(room))


# ------------------------------------------------------------ mesaj işleyici
async def handle(ws, data):
    action = data.get("action")
    info = clients.get(ws)
    room = rooms.get(info["room"]) if info else None
    me = None
    if room:
        me = next((p for p in room["players"] if p["ws"] is ws), None)
    is_host = bool(me and room["players"][0] is me)

    if action == "create":
        if room:
            await remove_client(ws)
        code = generate_room_code()
        player = {
            "id": secrets.token_hex(3),
            "ws": ws,
            "name": clean_name(data.get("name")),
            "team": 0,
        }
        new = {
            "code": code, "players": [player], "game": "fps",
            "phase": "lobby", "seed": 0, "teamSizes": [1, 1],
        }
        rooms[code] = new
        clients[ws] = {"room": code, "id": player["id"]}
        await send(ws, {"type": "joined", "id": player["id"]})
        await broadcast(new, room_state(new))

    elif action == "join":
        if room:
            await remove_client(ws)
        code = str(data.get("code", "")).strip().upper()
        target = rooms.get(code)
        if not target:
            await send(ws, {"type": "error", "message": "Böyle bir oda bulunamadı! Kodu kontrol et."})
        elif target["phase"] != "lobby":
            # F5 / sekme yenileme sonrası devam etmek için oyuncuyu geri al
            if data.get("rejoin") and sum(team_sizes(target)) < sum(target["teamSizes"]):
                player = {
                    "id": secrets.token_hex(3),
                    "ws": ws,
                    "name": clean_name(data.get("name"), target),
                    "team": pick_team(target),
                }
                target["players"].append(player)
                clients[ws] = {"room": code, "id": player["id"]}
                await send(ws, {"type": "joined", "id": player["id"]})
                await broadcast(target, room_state(target))
            else:
                await send(ws, {"type": "error", "message": "Bu odada oyun başlamış, bitmesini bekle."})
        else:
            sizes = team_sizes(target)
            preferred = data.get("team")
            if preferred not in (0, 1):
                preferred = None
            if preferred is not None and sizes[preferred] >= target["teamSizes"][preferred]:
                label = "Mavi" if preferred == 0 else "Kırmızı"
                await send(ws, {"type": "error", "message": f"{label} takım dolu!"})
                return
            if sum(sizes) >= sum(target["teamSizes"]):
                await send(ws, {"type": "error", "message": "Oda tamamen dolu!"})
                return
            player = {
                "id": secrets.token_hex(3),
                "ws": ws,
                "name": clean_name(data.get("name"), target),
                "team": pick_team(target, preferred),
            }
            target["players"].append(player)
            clients[ws] = {"room": code, "id": player["id"]}
            await send(ws, {"type": "joined", "id": player["id"]})
            await broadcast(target, room_state(target))

    elif action == "leave":
        await remove_client(ws)
        await send(ws, {"type": "left"})

    elif not room or not me:
        return

    elif action == "select_game":
        game = data.get("game")
        if is_host and room["phase"] == "lobby" and game in GAMES:
            room["game"] = game
            await broadcast(room, room_state(room))

    elif action == "set_team_size":
        if is_host and room["phase"] == "lobby" and room["game"] == "fps":
            t = data.get("team")
            d = data.get("delta")
            if t in (0, 1) and d in (-1, 1):
                sizes = team_sizes(room)
                nxt = room["teamSizes"][t] + d
                if 1 <= nxt <= TEAM_MAX:
                    other = 1 - t
                    if d == -1 and sizes[t] > nxt:
                        # küçültme: o takımdaki fazlalığı diğer takıma kaydır (yol varsa)
                        if sizes[other] < room["teamSizes"][other]:
                            p = next(p for p in room["players"] if p["team"] == t)
                            p["team"] = other
                        else:
                            return
                    if d == 1 and sum(room["teamSizes"]) >= ROOM_CAPACITY:
                        await send(ws, {"type": "error", "message": f"Toplam slot {ROOM_CAPACITY} kişiyi geçemez!"})
                        return
                    room["teamSizes"][t] = nxt
                    reflow_teams(room)
                    await broadcast(room, room_state(room))

    elif action == "switch_team":
        if room["phase"] == "lobby" and room["game"] == "fps":
            target = 1 - me["team"]
            sizes = team_sizes(room)
            if sizes[target] < room["teamSizes"][target]:
                me["team"] = target
                await broadcast(room, room_state(room))
            else:
                await send(ws, {"type": "error", "message": "Karşı takım dolu!"})

    elif action == "start_game":
        if not is_host or room["phase"] != "lobby":
            return
        if room["game"] not in GAMES:
            await send(ws, {"type": "error", "message": "Önce bir oyun seç!"})
            return
        sizes = team_sizes(room)
        if len(room["players"]) < 2:
            await send(ws, {"type": "error", "message": "Başlamak için en az 2 oyuncu gerekli!"})
            return
        if room["game"] == "fps" and (sizes[0] == 0 or sizes[1] == 0):
            await send(ws, {"type": "error", "message": "Her iki takımda da en az 1 oyuncu olmalı!"})
            return
        room["phase"] = "playing"
        room["seed"] = random.randint(1, 2**31 - 1)
        # sunucu taraflı oyun durumu: sadece hazırlık (freeze) süresini yönetir
        room["gameState"] = {"phase": "freeze", "round": 1, "timeLeft": FREEZE_TIME}
        for p in room["players"]:
            p["beat"] = 0.0   # ilk "open" heartbeat'i gelene kadar sayim bekle
        task = countdowns.pop(room["code"], None)
        if task:
            task.cancel()
        countdowns[room["code"]] = asyncio.create_task(countdown_task(room))
        await broadcast(room, room_state(room))

    elif action == "fps_new_round":
        # host istemci yeni bir freeze fazı başladığında sunucuya bildirir
        # böylece 2. raund ve sonrası için geri sayım yeniden başlar
        if is_host and room["phase"] == "playing":
            gs = room.get("gameState")
            if gs:
                gs["phase"] = "freeze"
                gs["timeLeft"] = FREEZE_TIME
                gs["round"] = data.get("round", gs.get("round", 1))
            else:
                room["gameState"] = {"phase": "freeze", "round": data.get("round", 1), "timeLeft": FREEZE_TIME}


    elif action == "return_lobby":
        if is_host and room["phase"] == "playing":
            room["phase"] = "lobby"
            room["gameState"] = None
            task = countdowns.pop(room["code"], None)
            if task:
                task.cancel()
            reflow_teams(room)
            await broadcast(room, room_state(room))

    elif action == "relay":
        if room["phase"] != "playing":
            return
        payload = data.get("data")
        if not isinstance(payload, dict):
            return
        out = {"type": "relay", "from": me["id"], "data": payload}
        to = data.get("to")
        if to:
            await broadcast(room, out, only=str(to))
        else:
            await broadcast(room, out, exclude=ws)

    elif action == "chat":
        if room:
            text = str(data.get("text", "")).strip()[:150]
            if text:
                await broadcast(room, {
                    "type": "chat",
                    "from": me["name"],
                    "text": text,
                    "team": me.get("team")
                })

    elif action == "signal":
        if room:
            to_id = data.get("to")
            signal_data = data.get("signal")
            if to_id and signal_data:
                await broadcast(room, {
                    "type": "signal",
                    "from": me["id"],
                    "signal": signal_data
                }, only=str(to_id))


async def lobby_handler(websocket):
    try:
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if isinstance(data, dict):
                await safe_handle(websocket, data)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await remove_client(websocket)


async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(
        lobby_handler,
        "0.0.0.0",
        port,
        ping_interval=20,
        ping_timeout=20,
    ):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
