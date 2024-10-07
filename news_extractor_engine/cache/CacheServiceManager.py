import zmq
import logging
import threading
from cachetools import TTLCache

class CacheServiceManager(threading.Thread):
    __event: threading.Event
    __context: zmq.Context
    __socket: zmq.Socket
    __ttl_cache: TTLCache


    def __init__(self, *, event: threading.Event):
        super().__init__()
        self.__event = event
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.REP)
        self.__socket.bind("tcp://*:5558")
        self.__ttl_cache = TTLCache(maxsize=2000, ttl=18600)

    def get_item(self, key):
        return self.__ttl_cache.get(key)

    def set_item(self, key, value) -> bool:
        try:
            self.__ttl_cache[key] = value
            return True
        except Exception as e:
            logging.error(f"Error setting item in cache: {e}")
            return False

    def run(self) -> None:
        while not self.__event.is_set():
            try:
                message = self.__socket.recv_json()
                if isinstance(message, dict):
                    if message['action'] == "get":
                        value = self.get_item(message['key'])
                        if value is None:
                            self.__socket.send_json({"status": "success", "value": "NA"})
                        else:
                            self.__socket.send_json({"status": "success", "value": value})
                    elif message['action'] == "set":
                        if self.set_item(message['key'], message['value']):
                            self.__socket.send_json({"status": "success"})
                        else:
                            self.__socket.send_json({"status": "failed", "error": "Received invalid data."})
            except Exception as e:
                logging.error(f"Cache Service Error: ({e.__class__.__name__}) {e.__str__()}")
                self.__socket.send_json({"status": "failed", "error": f"Cache Service Error: ({e.__class__.__name__}) {e.__str__()}"})

    def join(self, timeout=None):
        self.__socket.close()
        self.__context.term()
        super().join(timeout=timeout)
