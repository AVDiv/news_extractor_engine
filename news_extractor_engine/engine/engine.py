import asyncio
import logging
import threading
from datetime import datetime

import markdownify
import zmq

from news_extractor_engine.engine.feed import FeedReader
from news_extractor_engine.engine.scraper import ArticleInfoXpaths, ArticleSpider
from news_extractor_engine.model.engine import ChannelItem, EngineStore
from news_extractor_engine.utils.devtools import is_dev_mode
from news_extractor_engine.utils.discord_tools import DiscordLogger


class ZMQSocketPool:
    """Pool for managing and reusing ZMQ sockets to avoid 'Too many open files' error."""

    def __init__(self, context: zmq.Context, socket_type: int, max_pool_size: int = 50):
        """Initialize the socket pool.

        Args:
            context: ZMQ context
            socket_type: Type of ZMQ socket (e.g., zmq.REQ, zmq.PUSH)
            max_pool_size: Maximum number of sockets to keep in the pool
        """
        self.context = context
        self.socket_type = socket_type
        self.max_pool_size = max_pool_size
        self.available_sockets = []
        self.lock = threading.RLock()
        self.socket_endpoints = {}

    def get_socket(self, endpoint: str):
        """Get a socket from the pool or create a new one if none available.

        Args:
            endpoint: The ZMQ endpoint to connect the socket to

        Returns:
            A ZMQ socket connected to the specified endpoint
        """
        with self.lock:
            # Check if we have an available socket already connected to this endpoint
            for i, (sock, sock_endpoint) in enumerate(
                [(s, self.socket_endpoints.get(id(s))) for s in self.available_sockets]
            ):
                if sock_endpoint == endpoint:
                    # Remove from available sockets
                    self.available_sockets.pop(i)
                    return sock

            # If pool is not empty, reuse a socket (reconnect to new endpoint)
            if self.available_sockets:
                sock = self.available_sockets.pop(0)
                sock.disconnect(self.socket_endpoints.get(id(sock), ""))
                sock.connect(endpoint)
                self.socket_endpoints[id(sock)] = endpoint
                return sock

            # Create a new socket if we haven't reached max pool size
            sock = self.context.socket(self.socket_type)
            sock.connect(endpoint)
            self.socket_endpoints[id(sock)] = endpoint
            return sock

    def return_socket(self, socket):
        """Return a socket to the pool.

        Args:
            socket: The ZMQ socket to return to the pool
        """
        with self.lock:
            # Limit pool size, close oldest sockets if exceeding max size
            if len(self.available_sockets) >= self.max_pool_size:
                old_socket = self.available_sockets.pop(0)
                old_endpoint = self.socket_endpoints.pop(id(old_socket), None)
                if old_endpoint:
                    try:
                        old_socket.disconnect(old_endpoint)
                    except Exception:
                        pass
                old_socket.close()

            # Add the returned socket to the pool
            self.available_sockets.append(socket)

    def close_all(self):
        """Close all sockets in the pool."""
        with self.lock:
            for socket in self.available_sockets:
                endpoint = self.socket_endpoints.pop(id(socket), None)
                if endpoint:
                    try:
                        socket.disconnect(endpoint)
                    except Exception:
                        pass
                socket.close()
            self.available_sockets = []


class Engine:
    engine_store: EngineStore = EngineStore()
    __context: zmq.Context
    __spider_socket_pool = None
    __cache_socket_pool = None
    __MAX_SOCKET_POOL_SIZE = 50  # Adjust based on system limits

    def __init__(self) -> None:
        self.__context = zmq.Context()
        self.__spider_socket_pool = ZMQSocketPool(
            self.__context, zmq.PUSH, self.__MAX_SOCKET_POOL_SIZE
        )
        self.__cache_socket_pool = ZMQSocketPool(
            self.__context, zmq.REQ, self.__MAX_SOCKET_POOL_SIZE
        )

    async def __run_feed_sync_cycle(
        self, feed: FeedReader, article_xpaths: ArticleInfoXpaths
    ):
        refresh_time = self.engine_store.feed_min_refresh_interval
        refresh_buffer = self.engine_store.feed_refresh_buffer

        # Get sockets from pool instead of creating new ones each time
        spider_sock = self.__spider_socket_pool.get_socket("tcp://localhost:5555")
        cache_sock = self.__cache_socket_pool.get_socket("tcp://localhost:5558")

        try:
            feed.set_cache_service_socket(cache_sock)
            while True:
                if is_dev_mode():
                    logging.debug(f"Checking for new articles from {feed.source.name}")
                try:
                    feed_data = await feed.get_feed()  # Should be debugged
                    if feed_data.feed.feed.get("ttl") is not None:
                        refresh_time = float(feed_data.feed.feed.ttl) * 60
                        if feed_data.feed_last_updated_on is not None:
                            refresh_time -= (
                                datetime.now().timestamp()
                                - feed_data.feed_last_updated_on.timestamp()
                            ) % refresh_time
                        refresh_time += refresh_buffer
                    if feed_data.has_updated_since_last_request:
                        if is_dev_mode():
                            logging.debug(f"New article found from {feed.source.name}")
                            DiscordLogger.send_embed(
                                title=f"**{feed.source.name}**",
                                description=f"**{feed_data.feed['entries'][0]['title']}**\n{markdownify.markdownify(feed_data.feed['entries'][0]['summary'])}",
                                url=str(feed_data.feed["entries"][0]["link"]),
                                color=0xDDCFEE,
                            )
                        message = {
                            "source_id": feed.source.id.__str__(),
                            "name": feed.source.name,
                            "url": feed_data.feed["entries"][0]["link"],
                        }
                        # print(message)
                        spider_sock.send_json(message)

                except Exception as e:
                    logging.error(
                        f"Exception occured: ({e.__class__.__name__}) {e.__str__()}",
                        exc_info=True,
                    )
                    return
                logging.info(f"{feed.source.name} next check in {refresh_time} seconds")
                await asyncio.sleep(refresh_time)
        finally:
            # Return sockets to pool when done
            self.__spider_socket_pool.return_socket(spider_sock)
            self.__cache_socket_pool.return_socket(cache_sock)

    async def generate_feed_sync_tasks(
        self, feed: FeedReader, article_xpaths: ArticleInfoXpaths
    ):
        task = asyncio.create_task(
            self.__run_feed_sync_cycle(feed, article_xpaths), name=feed.source.name
        )
        rss_item: ChannelItem = ChannelItem(
            id=feed.source.id, name=feed.source.name, feed=feed, worker=task
        )
        self.engine_store.rss_item_list[feed.source.id] = rss_item

    def close_socket_pools(self):
        """Close all socket pools when shutting down."""
        if self.__spider_socket_pool:
            self.__spider_socket_pool.close_all()
        if self.__cache_socket_pool:
            self.__cache_socket_pool.close_all()

    @staticmethod
    def set_feed_task_default_refresh_time(time: float):
        """Sets the minimum refresh time of feed re-fetch tasks.

        Args:
            time (float): Time in seconds (Default is 10).
        """
        Engine.engine_store.feed_min_refresh_interval = time

    @staticmethod
    def set_feed_task_refresh_buffer(time: float):
        """Sets the refresh buffer time of feed re-fetch tasks.

        Args:
            time (float): Time in seconds (Default is 5).
        """
        Engine.engine_store.feed_refresh_buffer = time
