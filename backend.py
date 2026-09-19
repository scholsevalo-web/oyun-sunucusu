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

            if action == "create":
                room_code = generate_room_code()
                rooms[room_code] = [websocket]
                await websocket.send(json.dumps({
                    "type": "room_created", 
                    "code": room_code,
                    "message": f"Oda kuruldu! Arkadaşlarına şu kodu gönder: {room_code}"
                }))

            elif action == "join":
                room_code = data.get("code")
                if room_code in rooms:
                    if len(rooms[room_code]) < 4: 
                        rooms[room_code].append(websocket)
                        for player in rooms[room_code]:
                            await player.send(json.dumps({
                                "type": "player_joined",
                                "players_count": len(rooms[room_code]),
                                "message": f"Bir oyuncu katıldı! Odada {len(rooms[room_code])} kişi var."
                            }))
                    else:
                        await websocket.send(json.dumps({"type": "error", "message": "Oda şu an tam kapasite!"}))
                else:
                    await websocket.send(json.dumps({"type": "error", "message": "Oda bulunamadı!"}))

    except websockets.exceptions.ConnectionClosed:
        pass

async def main():
    # Render.com'un atadığı portu otomatik almak için güncelledik
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(lobby_handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
