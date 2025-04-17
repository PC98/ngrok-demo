import re
import asyncio
import requests
from flask import Flask, jsonify
import sys
from dataclasses import dataclass
from server import create_app
import websockets
SERVICE_URL = "http://44.247.242.188:3000"
MAX_REDUNDANCY_FACTOR = 2

def validate_prefix(prefix):
    if not isinstance(prefix, str):
        raise ValueError("Prefix must be a string")
    if not re.match("^[a-z]{1,8}$", prefix):
        raise ValueError("Prefix must be 1-8 lowercase characters")
    return prefix


@dataclass
class SDK:
    port: int
    prefix: str
    worker_connections: list = None
        
    def __post_init__(self):
        if self.worker_connections is None:
            self.worker_connections = []
        
    def home(self):
        return jsonify({"status": "running", "prefix": self.prefix})
    
    async def connect_to_worker(self, worker_address):
        try:
            async with websockets.connect(f"ws://{worker_address}") as websocket:
                self.worker_connections.append(websocket)
                while True:
                    # Keep connection alive and handle messages
                    message = await websocket.recv()
                    print(f"Received from {worker_address}: {message}")
        except Exception as e:
            print(f"Error connecting to {worker_address}: {e}")
    
    def setup_worker_connections(self):
        try:
            response = requests.post(f"{SERVICE_URL}/proxy/create", json={"prefix": self.prefix})
            data = response.json()
            worker_addresses = data.get('workers', [])
            
            # Start WebSocket connections
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            tasks = [self.connect_to_worker(worker_address) for worker_address in worker_addresses[:MAX_REDUNDANCY_FACTOR]]
            loop.run_until_complete(asyncio.gather(*tasks))
            
        except Exception as e:
            print(f"Error setting up worker connections: {e}")
            return []
    
    def run(self):
        # # Start WebSocket connections in a separate thread
        # ws_thread = Thread(target=self.setup_worker_connections)
        # ws_thread.daemon = True
        # ws_thread.start()
        self.setup_worker_connections()
        app = create_app()
        app.run(port=self.port)

def create_sdk(port, prefix):
    sdk = SDK(port, prefix)
    return sdk

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python sdk.py <port> <prefix>")
        sys.exit(1)
    
    port, prefix = sys.argv[1:]
    sdk = create_sdk(port, prefix)
    sdk.run()
