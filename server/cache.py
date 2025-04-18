import asyncio
from typing import Optional, List

from glide import (
    ClosingError,
    ConnectionError,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    Logger,
    LogLevel,
    NodeAddress,
    RequestError,
    TimeoutError,
)
from dataclasses import dataclass

CLUSTER_CLIENT_ADDRESS = "storage-imybxy.serverless.usw2.cache.amazonaws.com"

@dataclass
class Cache:
    client: Optional[GlideClusterClient] = None
    loop: Optional[asyncio.AbstractEventLoop] = None

    def connect(self):
        # Set logger configuration
        Logger.set_logger_config(LogLevel.INFO)

        # Configure the Glide Cluster Client
        addresses = [
            NodeAddress(CLUSTER_CLIENT_ADDRESS, 6379)
        ]
        config = GlideClusterClientConfiguration(addresses=addresses, use_tls=True)

        try:
            print("Connecting to Valkey Glide...")
            # Create a new event loop for this thread
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Create the client
            self.client = self.loop.run_until_complete(GlideClusterClient.create(config))
            print("Connected successfully.")
            return True
        except (TimeoutError, RequestError, ConnectionError, ClosingError) as e:
            print(f"An error occurred: {e}")
            return False

    def set(self, key, value, ttl=None):
        try:
            if ttl:
                result = self.loop.run_until_complete(self.client.set(key, value, expiry=ttl))
            else:
                result = self.loop.run_until_complete(self.client.set(key, value))
            return result
        except Exception as e:
            print(f"Error setting key {key}: {e}")
            return False

    def get(self, key):
        try:
            value = self.loop.run_until_complete(self.client.get(key))
            return value
        except Exception as e:
            print(f"Error getting key {key}: {e}")
            return None

    def keys(self, pattern: str) -> List[str]:
        try:
            # Use SCAN to get all keys matching the pattern
            cursor = 0
            keys = []
            while True:
                cursor, new_keys = self.loop.run_until_complete(
                    self.client.scan(cursor, match=pattern, count=100)
                )
                keys.extend(new_keys)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            print(f"Error getting keys for pattern {pattern}: {e}")
            return []

    def close(self):
        if self.client:
            try:
                self.loop.run_until_complete(self.client.close())
                print("Client connection closed.")
            except ClosingError as e:
                print(f"Error closing client: {e}")

