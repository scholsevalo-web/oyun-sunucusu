import asyncio
import websockets
import json
import random
import string
import os

rooms = {}

def generate_room_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

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
                    "players": [player_name]
                }))

            elif action == "join":
                room_code = data.get("code")
                if room_code in rooms:
                    if len(rooms[room_code]) < 4: 
                        # Aynı isimde biri var mı kontrol et veya direkt ekle
                        rooms[room_code].append({"ws": websocket, "name": player_name})
                        
                        # Odadaki herkesin güncel isim listesini çıkar
                        player_names = [p["name"] for p in rooms[room_code]]
                        
                        # Odadaki HERKESE yeni listeyi gönder
                        for player in rooms[room_code]:
                            await player["ws"].send(json.dumps({
                                "type": "player_joined", 
                                "code": room_code, 
                                "players": player_names
                            }))
                    else:
                        await websocket.send(json.dumps({"type": "error", "message": "Oda şu an tam kapasite (4 kişi)!"}))
                else:
                    await websocket.send(json.dumps({"type": "error", "message": "Böyle bir oda bulunamadı! Kodu kontrol et."}))

            elif action == "start_game":
                room_code = data.get("code")
                if room_code in rooms:
                    for player in rooms[room_code]:
                        await player["ws"].send(json.dumps({"type": "game_started"}))

            elif action == "move":
                room_code = data.get("code")
                if room_code in rooms:
                    for player in rooms[room_code]:
                        if player["ws"] != websocket:
                            await player["ws"].send(json.dumps({
                                "type": "player_moved",
                                "name": player_name,
                                "x": data.get("x"),
                                "y": data.get("y")
                            }))

    except websockets.exceptions.ConnectionClosed:
        pass

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(lobby_handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
