import asyncio
import websockets
from typing import Dict
from flask import Flask, request, jsonify
import threading
import json


app = Flask(__name__)
RECIEVER_APP_PORT = 5001
WSS_PORT = 8765

active_connections: Dict[str, websockets.WebSocketServerProtocol] = {}

async def register(websocket: websockets.WebSocketServerProtocol, prefix: str):
    active_connections[prefix] = websocket
    try:
        await websocket.wait_closed()
    finally:
        if prefix in active_connections and active_connections[prefix] == websocket:
            del active_connections[prefix]

async def send_to_clients(prefix: str, subpath: str):
    if prefix in active_connections:
        websocket = active_connections[prefix]
        try:
            await websocket.send(json.dumps({"subpath": subpath}))
            response = await websocket.recv()
            return response
        except websockets.exceptions.ConnectionClosed:
            if prefix in active_connections and active_connections[prefix] == websocket:
                del active_connections[prefix]

async def handler(websocket: websockets.WebSocketServerProtocol):
    try:
        # Wait for the initial prefix message
        message = await websocket.recv()
        data = json.loads(message)
        
        if "prefix" in data:
            prefix = data["prefix"]
            await register(websocket, prefix)
        else:
            print("Invalid initial message format")
            await websocket.close()
    except websockets.exceptions.ConnectionClosed:
        pass
    except json.JSONDecodeError:
        print("Invalid JSON message received")
        await websocket.close()
    except Exception as e:
        print(f"Error in handler: {e}")
        await websocket.close()

@app.route('/send', methods=['POST'])
def send_message():
    data = request.get_json()
    if 'prefix' not in data or 'subpath' not in data:
        return jsonify({"error": "Invalid request body"}), 400
        
    # Run the send in the event loop and get the response
    response = asyncio.run(send_to_clients(data['prefix'], data['subpath']))
    if response is None:
        return jsonify({"error": "No active connection for prefix"}), 404
    
    return jsonify(response)

async def main():
    async with websockets.serve(handler, "0.0.0.0", WSS_PORT):
        await asyncio.Future()

def start_server():
    ws_thread = threading.Thread(target=lambda: asyncio.run(main()))
    ws_thread.daemon = True
    ws_thread.start()
    
    # Start Flask server
    app.run(port=RECIEVER_APP_PORT)

if __name__ == '__main__':
    start_server()
