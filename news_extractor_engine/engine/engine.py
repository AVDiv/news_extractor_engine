import asyncio
import logging
import threading
import time
from datetime import datetime
from queue import Empty, Queue

import markdownify
import zmq

from news_extractor_engine.engine.feed import FeedReader
from news_extractor_engine.engine.scraper import ArticleInfoXpaths, ArticleSpider
from news_extractor_engine.model.engine import ChannelItem, EngineStore
from news_extractor_engine.utils.devtools import is_dev_mode
from news_extractor_engine.utils.discord_tools import DiscordLogger


class ZMQSocketPool:
    """Enhanced pool for managing and reusing ZMQ sockets to avoid 'Too many open files' error."""

    def __init__(
        self,
        context: zmq.Context,
        socket_type: int,
        max_pool_size: int = 25,
        max_concurrent_users: int = 10,
        connection_timeout: int = 10,
    ):
        """Initialize the socket pool with strict limits.

        Args:
            context: ZMQ context
            socket_type: Type of ZMQ socket (e.g., zmq.REQ, zmq.PUSH)
            max_pool_size: Maximum number of sockets to keep in the pool
            max_concurrent_users: Maximum number of concurrent socket users
            connection_timeout: Timeout for waiting for a socket in seconds
        """
        self.context = context
        self.socket_type = socket_type
        self.max_pool_size = max_pool_size
        self.max_concurrent_users = max_concurrent_users
        self.connection_timeout = connection_timeout
        self.available_sockets = []
        self.in_use_count = 0
        self.lock = threading.RLock()
        self.socket_endpoints = {}
        self.semaphore = threading.Semaphore(max_concurrent_users)
        self.socket_queue = Queue()

    async def get_socket_async(self, endpoint: str):
        """Asynchronously get a socket from the pool with a timeout.

        Args:
            endpoint: The ZMQ endpoint to connect the socket to

        Returns:
            A ZMQ socket connected to the specified endpoint

        Raises:
            TimeoutError: If no socket is available within the timeout period
        """
        # Try to acquire semaphore to limit concurrent users
        acquired = False
        for _ in range(self.connection_timeout):
            acquired = self.semaphore.acquire(blocking=False)
            if acquired:
                break
            await asyncio.sleep(1)

        if not acquired:
            raise TimeoutError(
                f"Could not acquire socket within {self.connection_timeout} seconds"
            )

        try:
            with self.lock:
                # Check if we have an available socket for this endpoint
                for i, (sock, sock_endpoint) in enumerate(
                    [
                        (s, self.socket_endpoints.get(id(s)))
                        for s in self.available_sockets
                    ]
                ):
                    if sock_endpoint == endpoint:
                        sock = self.available_sockets.pop(i)
                        self.in_use_count += 1
                        return sock

                # If pool is not empty, reuse a socket
                if self.available_sockets:
                    sock = self.available_sockets.pop(0)
                    sock.disconnect(self.socket_endpoints.get(id(sock), ""))
                    sock.connect(endpoint)
                    self.socket_endpoints[id(sock)] = endpoint
                    self.in_use_count += 1
                    return sock

                # Create a new socket if under limit
                if self.in_use_count < self.max_pool_size:
                    sock = self.context.socket(self.socket_type)
                    sock.setsockopt(
                        zmq.LINGER, 0
                    )  # Don't keep messages in memory after close
                    sock.setsockopt(
                        zmq.RCVTIMEO, 10000
                    )  # 10 second timeout on receives
                    sock.setsockopt(zmq.SNDTIMEO, 10000)  # 10 second timeout on sends
                    sock.connect(endpoint)
                    self.socket_endpoints[id(sock)] = endpoint
                    self.in_use_count += 1
                    return sock

                # If we get here, we need to wait for a socket
                raise TimeoutError(
                    "No sockets available in the pool and at maximum limit"
                )
        except Exception as e:
            self.semaphore.release()
            raise e

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
            self.in_use_count -= 1
            self.semaphore.release()

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
            self.in_use_count = 0


class Engine:
    engine_store: EngineStore = EngineStore()
    __context: zmq.Context
    __spider_socket_pool = None
    __cache_socket_pool = None
    __MAX_SOCKET_POOL_SIZE = 25  # Reduced from 50 to 25
    __MAX_CONCURRENT_USERS = 10  # Maximum concurrent socket users

    def __init__(self) -> None:
        self.__context = zmq.Context()
        self.__context.set(
            zmq.MAX_SOCKETS, 1000
        )  # Increase ZMQ's internal socket limit
        self.__spider_socket_pool = ZMQSocketPool(
            self.__context,
            zmq.PUSH,
            max_pool_size=self.__MAX_SOCKET_POOL_SIZE,
            max_concurrent_users=self.__MAX_CONCURRENT_USERS,
        )
        self.__cache_socket_pool = ZMQSocketPool(
            self.__context,
            zmq.REQ,
            max_pool_size=self.__MAX_SOCKET_POOL_SIZE,
            max_concurrent_users=self.__MAX_CONCURRENT_USERS,
        )

    async def __run_feed_sync_cycle(
        self, feed: FeedReader, article_xpaths: ArticleInfoXpaths
    ):
        refresh_time = self.engine_store.feed_min_refresh_interval
        refresh_buffer = self.engine_store.feed_refresh_buffer

        while True:
            # Get sockets from pool with proper error handling
            spider_sock = None
            cache_sock = None

            try:
                try:
                    # Get sockets from pool
                    spider_sock = await self.__spider_socket_pool.get_socket_async(
                        "tcp://localhost:5555"
                    )
                    cache_sock = await self.__cache_socket_pool.get_socket_async(
                        "tcp://localhost:5558"
                    )

                    # Set cache socket for this cycle
                    feed.set_cache_service_socket(cache_sock)

                    if is_dev_mode():
                        logging.debug(
                            f"Checking for new articles from {feed.source.name}"
                        )

                    feed_data = await feed.get_feed()

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
                        spider_sock.send_json(message)

                except TimeoutError as e:
                    logging.warning(
                        f"Socket pool timeout for {feed.source.name}: {str(e)}"
                    )
                    # Implement backoff/retry mechanism
                    refresh_time = min(
                        refresh_time * 1.5, 300
                    )  # Exponential backoff up to 5 minutes

                except Exception as e:
                    logging.error(
                        f"Exception occurred in {feed.source.name}: ({e.__class__.__name__}) {e.__str__()}",
                        exc_info=True,
                    )
                    # Use a longer refresh time on error to avoid hammering the system
                    refresh_time = min(
                        refresh_time * 2, 600
                    )  # Exponential backoff up to 10 minutes

            finally:
                # Always return sockets to pool
                if spider_sock:
                    self.__spider_socket_pool.return_socket(spider_sock)
                if cache_sock:
                    self.__cache_socket_pool.return_socket(cache_sock)

            # Add jitter to prevent thundering herd
            jitter = (refresh_time * 0.1) * (threading.get_ident() % 10) / 10
            actual_refresh = max(10, refresh_time + jitter)  # Ensure minimum 10s

            logging.info(
                f"{feed.source.name} next check in {actual_refresh:.1f} seconds"
            )
            await asyncio.sleep(actual_refresh)

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
        if self.__context:
            self.__context.term()

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
