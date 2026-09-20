import asyncio
import websockets
import json
import random
import string
import os

rooms = {}  # room_code -> [{"ws":..., "name":...}, ...]
ROOM_CAPACITY = 3

def generate_room_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

async def broadcast(room_code, message, exclude_ws=None):
    """Odadaki herkese gönderir (exclude_ws hariç). Kopmuş bağlantıları düşürür."""
    if room_code not in rooms:
        return
    alive_players = []
    for player in rooms[room_code]:
        if player["ws"] is exclude_ws:
            alive_players.append(player)
            continue
        try:
            await player["ws"].send(json.dumps(message))
            alive_players.append(player)
        except websockets.exceptions.ConnectionClosed:
            pass  # bu oyuncu kopmuş, listeden düşecek
    if alive_players:
        rooms[room_code] = alive_players
    elif room_code in rooms:
        del rooms[room_code]

def remove_player(websocket):
    """Kopan websocket'i bulunduğu odadan siler, kalanlara haber verir."""
    for room_code in list(rooms.keys()):
        players = rooms[room_code]
        still_here = [p for p in players if p["ws"] != websocket]
        if len(still_here) != len(players):
            if still_here:
                rooms[room_code] = still_here
                asyncio.create_task(broadcast(room_code, {
                    "type": "player_joined",
                    "code": room_code,
                    "players": [p["name"] for p in still_here],
                    "host": still_here[0]["name"]
                }))
            else:
                del rooms[room_code]

# Bu action'lar, sunucuda özel bir mantık gerektirmeden odadaki
# diğer herkese (gönderen hariç) olduğu gibi iletilir. Oyun mesajları
# (hareket, canavar konumu, eşya alma, kaçış vb.) buradan geçer.
RELAY_ACTIONS = {"move", "monster_update", "item_pickup", "player_escaped", "player_caught"}

async def lobby_handler(websocket):
    try:
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")
            player_name = data.get("name", "Misafir").strip()
            if not player_name:
                player_name = "Misafir"

            if action == "create":
                room_code = generate_room_code()
                rooms[room_code] = [{"ws": websocket, "name": player_name}]
                await websocket.send(json.dumps({
                    "type": "room_created",
                    "code": room_code,
                    "players": [player_name],
                    "host": player_name
                }))

            elif action == "join":
                room_code = data.get("code")
                if room_code in rooms:
                    if len(rooms[room_code]) < ROOM_CAPACITY:
                        rooms[room_code].append({"ws": websocket, "name": player_name})
                        player_names = [p["name"] for p in rooms[room_code]]
                        await broadcast(room_code, {
                            "type": "player_joined",
                            "code": room_code,
                            "players": player_names,
                            "host": rooms[room_code][0]["name"]
                        })
                    else:
                        await websocket.send(json.dumps({"type": "error", "message": f"Oda şu an tam kapasite ({ROOM_CAPACITY} kişi)!"}))
                else:
                    await websocket.send(json.dumps({"type": "error", "message": "Böyle bir oda bulunamadı! Kodu kontrol et."}))

            elif action == "start_game":
                room_code = data.get("code")
                if room_code in rooms:
                    await broadcast(room_code, {
                        "type": "game_started",
                        "code": room_code,
                        "players": [p["name"] for p in rooms[room_code]],
                        "host": rooms[room_code][0]["name"]
                    })

            elif action in RELAY_ACTIONS:
                room_code = data.get("code")
                if room_code in rooms:
                    payload = dict(data)
                    payload["type"] = action
                    payload["name"] = player_name
                    payload.pop("action", None)
                    await broadcast(room_code, payload, exclude_ws=websocket)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        remove_player(websocket)

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(
        lobby_handler,
        "0.0.0.0",
        port,
        ping_interval=5,
        ping_timeout=5,
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
