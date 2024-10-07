import zmq
import logging
import asyncio
import markdownify
from datetime import datetime

from news_extractor_engine.engine.scraper import ArticleSpider, ArticleInfoXpaths
from news_extractor_engine.model.engine import ChannelItem, EngineStore
from news_extractor_engine.utils.discord_tools import DiscordLogger
from news_extractor_engine.engine.feed import FeedReader

from news_extractor_engine.utils.devtools import is_dev_mode


class Engine:
    engine_store: EngineStore = EngineStore()
    __context: zmq.Context

    def __init__(self) -> None:
        self.__context = zmq.Context()

    async def __run_feed_sync_cycle(self, feed: FeedReader, article_xpaths: ArticleInfoXpaths):
        refresh_time = self.engine_store.feed_min_refresh_interval
        refresh_buffer = self.engine_store.feed_refresh_buffer
        spider_sock = self.__context.socket(zmq.PUSH)
        spider_sock.connect("tcp://localhost:5555")
        cache_sock = self.__context.socket(zmq.REQ)
        feed.set_cache_service_socket(cache_sock)
        while True:
            if is_dev_mode():
                logging.debug(f"Checking for new articles from {feed.source.name}")
            try:
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
                    # print(message)
                    spider_sock.send_json(message)

            except Exception as e:
                logging.error(f"Exception occured: ({e.__class__.__name__}) {e.__str__()}", exc_info=True)
                return
            logging.info(f"{feed.source.name} next check in {refresh_time} seconds")
            await asyncio.sleep(refresh_time)

    async def generate_feed_sync_tasks(self, feed: FeedReader, article_xpaths: ArticleInfoXpaths):
        task = asyncio.create_task(
            self.__run_feed_sync_cycle(feed, article_xpaths),
            name=feed.source.name
        )
        rss_item: ChannelItem = ChannelItem(
            id=feed.source.id, name=feed.source.name, feed=feed, worker=task
        )
        self.engine_store.rss_item_list[feed.source.id] = rss_item

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
