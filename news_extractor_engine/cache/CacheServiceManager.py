import logging
import threading

import zmq
from cachetools import TTLCache


class CacheServiceManager(threading.Thread):
    __event: threading.Event
    __context: zmq.Context
    __socket: zmq.Socket
    __ttl_cache: TTLCache
    __SOCKET_HWM = 1000  # High water mark for socket buffer

    def __init__(self, *, event: threading.Event):
        super().__init__()
        self.__event = event
        self.__context = zmq.Context()
        self.__context.set(zmq.MAX_SOCKETS, 100)  # Set ZMQ socket limit

        # Create socket with better resource management settings
        self.__socket = self.__context.socket(zmq.REP)
        self.__socket.setsockopt(
            zmq.LINGER, 0
        )  # Don't keep messages in memory after close
        self.__socket.setsockopt(
            zmq.RCVHWM, self.__SOCKET_HWM
        )  # Receive high water mark
        self.__socket.setsockopt(zmq.SNDHWM, self.__SOCKET_HWM)  # Send high water mark
        self.__socket.setsockopt(zmq.RCVTIMEO, 1000)  # 1 second timeout on receives
        self.__socket.bind("tcp://*:5558")

        # Increase cache size to handle more sources
        self.__ttl_cache = TTLCache(maxsize=10000, ttl=18600)

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
        poller = zmq.Poller()
        poller.register(self.__socket, zmq.POLLIN)

        while not self.__event.is_set():
            try:
                # Poll with timeout so we can check the event flag periodically
                sockets = dict(poller.poll(1000))  # 1 second timeout

                if self.__socket in sockets and sockets[self.__socket] == zmq.POLLIN:
                    message = self.__socket.recv_json(zmq.NOBLOCK)
                    if isinstance(message, dict):
                        if message["action"] == "get":
                            value = self.get_item(message["key"])
                            if value is None:
                                self.__socket.send_json(
                                    {"status": "success", "value": "NA"}
                                )
                            else:
                                self.__socket.send_json(
                                    {"status": "success", "value": value}
                                )
                        elif message["action"] == "set":
                            if self.set_item(message["key"], message["value"]):
                                self.__socket.send_json({"status": "success"})
                            else:
                                self.__socket.send_json(
                                    {
                                        "status": "failed",
                                        "error": "Received invalid data.",
                                    }
                                )
            except zmq.Again:
                # No messages available, just continue
                continue
            except Exception as e:
                logging.error(
                    f"Cache Service Error: ({e.__class__.__name__}) {e.__str__()}"
                )
                try:
                    self.__socket.send_json(
                        {
                            "status": "failed",
                            "error": f"Cache Service Error: ({e.__class__.__name__}) {e.__str__()}",
                        }
                    )
                except Exception:
                    # If we can't send a response, just log the error and continue
                    logging.error("Failed to send error response")

    def join(self, timeout=None):
        """Properly join the thread, ensuring all resources are cleaned up."""
        logging.info("Cleaning up CacheServiceManager resources")
        if hasattr(self, "__socket") and self.__socket:
            self.__socket.close()
        if hasattr(self, "__context") and self.__context:
            self.__context.term()
        super().join(timeout=timeout)
