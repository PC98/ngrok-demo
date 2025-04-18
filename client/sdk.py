import asyncio
import requests
import sys
from dataclasses import dataclass, field
from server import create_app
import websockets
import json
import threading

SERVICE_URL = "http://LoadBalancer-454729797.us-west-2.elb.amazonaws.com:80"
MAX_REDUNDANCY_FACTOR = 2

@dataclass
class SDK:
    port: int
    prefix: str
    
    async def handle_websocket(self, websocket):
        try:
            # Send prefix message
            await websocket.send(json.dumps({"prefix": self.prefix}))
            
            while True:
                # Listen for messages
                message = await websocket.recv()
                data = json.loads(message)
                
                if "subpath" in data:
                    # Make HTTP request to the Flask app
                    response = requests.get(f"http://localhost:{self.port}/{data['subpath']}")
                    if response.status_code != 200:
                        raise Exception(f"Failed to proxy request")
                    # Send response back through websocket
                    await websocket.send(json.dumps(response.json()))

        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket connection closed")
        except Exception as e:
            print(f"Error handling WebSocket: {e}")
    
    def setup_worker_connections(self):
        try:
            response = requests.post(f"{SERVICE_URL}/proxy/create", json={"prefix": self.prefix})
            if response.status_code != 200:
                raise Exception(f"Failed to create proxy")
            
            data = response.json()
            worker_addresses = data.get('workers', [])
            
            # Start WebSocket connections
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            tasks = []
            for worker_address in worker_addresses[:MAX_REDUNDANCY_FACTOR]:
                websocket = loop.run_until_complete(websockets.connect(f"ws://{worker_address}"))
                tasks.append(self.handle_websocket(websocket))
            
            # Run all WebSocket handlers concurrently
            loop.run_until_complete(asyncio.gather(*tasks))
            
        except Exception as e:
            print(f"Error setting up worker connections: {e}")
    
    def run(self):
        # Start WebSocket connections in a separate thread
        ws_thread = threading.Thread(target=self.setup_worker_connections, daemon=True)
        ws_thread.start()
        
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
