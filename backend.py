import asyncio
import json
import os
import random
import secrets
import string

import websockets

ROOM_CAPACITY = 4
GAMES = {"prison", "fps"}

# Oyuncu sayısına göre FPS modu ve takım kapasiteleri: (Mavi, Kırmızı)
#   2 kişi -> 1v1,  3 kişi -> 1v2 (Mavi tek kişi),  4 kişi -> 2v2
FPS_CAPS = {2: (1, 1), 3: (1, 2), 4: (2, 2)}
FPS_MODE_NAMES = {2: "1v1", 3: "1v2", 4: "2v2"}

rooms = {}    # oda_kodu -> oda sözlüğü
clients = {}  # websocket -> {"room": kod, "id": oyuncu_id}


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


def room_state(room):
    n = len(room["players"])
    mode = room.get("mode") if room["phase"] == "playing" else FPS_MODE_NAMES.get(n)
    return {
        "type": "room_state",
        "code": room["code"],
        "phase": room["phase"],
        "game": room["game"],
        "mode": mode,
        "seed": room["seed"],
        "host": room["players"][0]["id"],
        "players": [
            {"id": p["id"], "name": p["name"], "team": p["team"]}
            for p in room["players"]
        ],
    }


def rebalance_teams(room):
    """Oyuncu sayısına göre takımları kapasiteye uydurur; mevcut seçimleri mümkün olduğunca korur."""
    players = room["players"]
    caps = FPS_CAPS.get(len(players))
    if not caps:
        for i, p in enumerate(players):
            p["team"] = i % 2
        return
    counts = [0, 0]
    leftovers = []
    for p in players:
        t = p.get("team")
        if t in (0, 1) and counts[t] < caps[t]:
            counts[t] += 1
        else:
            leftovers.append(p)
    for p in leftovers:
        t = 0 if counts[0] < caps[0] else 1
        p["team"] = t
        counts[t] += 1


def switch_team(room, player):
    caps = FPS_CAPS.get(len(room["players"]))
    if not caps:
        return
    target = 1 - player["team"]
    members = [p for p in room["players"] if p["team"] == target]
    if len(members) < caps[target]:
        player["team"] = target
    elif members:  # hedef takım dolu: ilk oyuncuyla yer değiştir
        other = members[0]
        other["team"], player["team"] = player["team"], target


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
    if room["phase"] == "lobby":
        rebalance_teams(room)
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
            "code": code, "players": [player], "game": None,
            "phase": "lobby", "seed": 0, "mode": None,
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
            await send(ws, {"type": "error", "message": "Bu odada oyun başlamış, bitmesini bekle."})
        elif len(target["players"]) >= ROOM_CAPACITY:
            await send(ws, {"type": "error", "message": f"Oda şu an tam kapasite ({ROOM_CAPACITY} kişi)!"})
        else:
            player = {
                "id": secrets.token_hex(3),
                "ws": ws,
                "name": clean_name(data.get("name"), target),
                "team": 0,
            }
            target["players"].append(player)
            rebalance_teams(target)
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
            rebalance_teams(room)
            await broadcast(room, room_state(room))

    elif action == "switch_team":
        if room["phase"] == "lobby" and room["game"] == "fps":
            switch_team(room, me)
            await broadcast(room, room_state(room))

    elif action == "start_game":
        if not is_host or room["phase"] != "lobby":
            return
        n = len(room["players"])
        if room["game"] not in GAMES:
            await send(ws, {"type": "error", "message": "Önce bir oyun seç!"})
        elif n < 2:
            await send(ws, {"type": "error", "message": "Başlamak için en az 2 oyuncu gerekli!"})
        else:
            rebalance_teams(room)
            room["phase"] = "playing"
            room["seed"] = random.randint(1, 2**31 - 1)
            room["mode"] = FPS_MODE_NAMES.get(n)
            await broadcast(room, room_state(room))

    elif action == "return_lobby":
        if is_host and room["phase"] == "playing":
            room["phase"] = "lobby"
            rebalance_teams(room)
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
                await handle(websocket, data)
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
