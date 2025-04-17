from flask import Flask, request, jsonify, g
import threading
import socket
import time
from cache import Cache
import requests
from wss import RECIEVER_APP_PORT, WSS_PORT

REGISTRATION_INTERVAL_SECONDS = 10
APP_PORT = 5000
MAX_REDUNDANCY_FACTOR = 2

app = Flask(__name__)

def get_server_address():
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    return ip_address


def register_server():
    cache = Cache()
    if not cache.connect():
        raise Exception("Failed to connect to cache for registration")
    
    server_address = get_server_address()
    while True:
        try:
            # Use a key format that allows for easy querying of all servers
            key = get_servers_cache_key(server_address)
            cache.set(key, server_address, ttl=REGISTRATION_INTERVAL_SECONDS)  # 10 second TTL
            print(f"Registered server at {server_address}")
        except Exception as e:
            print(f"Error registering server: {e}")
        time.sleep(REGISTRATION_INTERVAL_SECONDS)  # Wait 10 seconds before next registration


def get_cache():
    if 'cache' not in g:
        g.cache = Cache()
        if not g.cache.connect():
            raise Exception("Failed to connect to cache")
    return g.cache


def get_proxies_cache_key(query):
    return f"client_server_proxies:{query}"

def get_servers_cache_key(prefix):
    return f"servers:{prefix}"

@app.route('/query/<path:subpath>', methods=['GET'])
def proxy(subpath):
    cache = get_cache()
    
    # Get prefix from query params
    prefix = request.args.get('prefix')
    if not prefix:
        return jsonify({"error": "prefix parameter is required"}), 400
    
    try:
        # Get worker address keys from cache
        worker_address_keys = cache.get(get_proxies_cache_key(prefix))
        if not worker_address_keys:
            return jsonify({"error": f"No proxies found for prefix: {prefix}"}), 404
        
        worker_address = None
        for key in worker_address_keys:
            worker_address = cache.get(get_servers_cache_key(key))
                
        if not worker_address:
            return jsonify({"error": f"No active workers found for prefix: {prefix}"}), 404
        
        message = {
            "prefix": prefix,
            "subpath": subpath,
        }
        response = requests.post(f"http://{worker_address}:{RECIEVER_APP_PORT}/send", json=message)
        if response.status_code != 200:
            return jsonify({"error": "Failed to send proxy request to WebSocket"}), 500
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/proxy/create', methods=['POST'])
def create_proxy():
    cache = get_cache()    
    data = request.get_json()
    if not data or 'prefix' not in data or 'address' not in data:
        return jsonify({"error": "Invalid request body"}), 400
        
    prefix = data['prefix']
    
    workers = []
    worker_address_keys = []
    try:
        server_keys = cache.keys(get_servers_cache_key("*"))
        for key in server_keys[:MAX_REDUNDANCY_FACTOR]:
            worker_address = cache.get(key)
            if worker_address:
                workers.append(f"{worker_address}:{WSS_PORT}")
                worker_address_keys.append(key)

        if len(workers) < MAX_REDUNDANCY_FACTOR:
            raise Exception("Not enough servers registered")
            
        cache.set(get_proxies_cache_key(prefix), worker_address_keys)
    except Exception as e:
        print(f"Error getting workers: {e}")
        return jsonify({"error": str(e)}), 500
    
    return jsonify({"workers": workers})

@app.teardown_appcontext
def teardown_cache(_):
    cache = g.pop('cache', None)
    if cache is not None:
        cache.close()


def create_app():
    return app

if __name__ == '__main__':
    app = create_app()

    registration_thread = threading.Thread(target=register_server, daemon=True)
    registration_thread.start()
    
    app.run(debug=True, port=APP_PORT)
