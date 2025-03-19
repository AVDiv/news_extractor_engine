import asyncio
import builtins
import logging
import os
import threading
from datetime import datetime

import uvicorn
from pymongo import MongoClient

from news_extractor_engine.api import ApiServerManager
from news_extractor_engine.cache.CacheServiceManager import CacheServiceManager
from news_extractor_engine.engine.engine import Engine
from news_extractor_engine.engine.feed import FeedReader
from news_extractor_engine.engine.scraper import ArticleInfoXpaths
from news_extractor_engine.engine.SpiderTaskQue import SpiderTaskQue
from news_extractor_engine.model.error.Environment import (
    EnvironmentVariableNotFoundException,
    InvalidEnvironmentVariableFormatException,
)
from news_extractor_engine.model.feed import ArticleSource
from news_extractor_engine.utils.discord_tools import DiscordLogger


class App:
    __engine: Engine
    __api_server_manager: ApiServerManager
    __scraper_task_que: SpiderTaskQue
    __scraper_task_que_event: threading.Event
    __cache_service: CacheServiceManager
    __cache_service_event: threading.Event
    __env: dict[str, str | int | None]

    def __setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(name)s; %(levelname)s :: %(message)s",
            handlers=[
                logging.FileHandler(
                    f'logs/engine-{datetime.now().strftime("%d-%m-%Y_%H:%M:%S")}.log'
                ),
                logging.StreamHandler(),
            ],
        )

    def __load_env_vars(self):
        self.__env = {}
        variables = {
            "ENVIRONMENT": (True, str),
            "DISCORD_WEBHOOK_URL": (True, str),
            "MONGODB_CONFIG_DB_CONNECTION_STRING": (True, str),
            "API_HOST": (False, str),
            "API_PORT": (False, int),
        }
        for name, var_details in variables.items():
            env_var = os.getenv(name)
            if (env_var is None) and var_details[0]:
                raise EnvironmentVariableNotFoundException(
                    f"{name} environment variable is not set."
                )
            if (env_var is not None) and (not isinstance(env_var, var_details[1])):
                raise InvalidEnvironmentVariableFormatException(
                    f"{name} environment variable should be in {var_details}, but it is {type(env_var)}."
                )
            self.__env[name] = env_var
        setattr(builtins, "ENVIRONMENT", self.__env["ENVIRONMENT"])

    def __setup_discord_logger(self):
        DiscordLogger.set_webhook_url(self.__env["DISCORD_WEBHOOK_URL"])

    def __set_engine_parameters(self):
        Engine.set_feed_task_default_refresh_time(10.0)
        Engine.set_feed_task_refresh_buffer(5.0)

    def __init_engine(self):
        self.__engine = Engine()

    async def __init_feed_tasks(self):
        logging.debug("Initializing RSS feed tasks...")
        source_db = MongoClient(
            str(self.__env["MONGODB_CONFIG_DB_CONNECTION_STRING"])
        ).get_default_database()
        source_collection = source_db.get_collection("worker_source")
        for source_doc in source_collection.find():
            source = ArticleSource(
                id=source_doc["_id"],
                name=source_doc["title"],
                domain=source_doc["domain"],
                rss_url=source_doc["rss"],
                categories=source_doc["channels"],
            )
            rss_feed = FeedReader(source)
            source_doc: dict
            if "xpaths" in source_doc.keys():
                article_xpaths = ArticleInfoXpaths(
                    title=source_doc["xpaths"]["title"],
                    author=source_doc["xpaths"]["author"],
                    publication_date=source_doc["xpaths"]["publication_date"],
                    summary=source_doc["xpaths"]["summary"],
                    content=source_doc["xpaths"]["content"],
                    tags=source_doc["xpaths"]["tags"],
                    categories=source_doc["xpaths"]["categories"],
                )
            else:
                article_xpaths = ArticleInfoXpaths(
                    None, None, None, None, None, None, None
                )
            await self.__engine.generate_feed_sync_tasks(rss_feed, article_xpaths)
            setattr(
                builtins, "RSS_SOURCE_LIST", self.__engine.engine_store.rss_item_list
            )

    async def __destruct_all_tasks(self):
        for rss_item in self.__engine.engine_store.rss_item_list.values():
            rss_item.worker.cancel()

    async def __start_api_server(self):
        self.__api_server_manager = ApiServerManager(
            host=str(self.__env["API_HOST"]) or "0.0.0.0",
            port=(
                int(self.__env["API_PORT"])
                if self.__env["API_PORT"] is not None
                else (8080 if ENVIRONMENT == "development" else 80)
            ),
        )
        self.__api_server_manager.start()

    async def __stop_api_server(self):
        self.__api_server_manager.join()

    def __start_scraper_task_que(self):
        logging.info("Starting scraper task que...")
        self.__scraper_task_que_event = threading.Event()
        self.__scraper_task_que = SpiderTaskQue(event=self.__scraper_task_que_event)
        self.__scraper_task_que.start()
        logging.info("Started scraper task que!")

    def __stop_scraper_task_que(self):
        logging.info("Stopping scraper task que...")
        self.__scraper_task_que_event.set()
        self.__scraper_task_que.join()
        logging.info("Stopped scraper task que!")

    def __start_cache_service_manager(self):
        logging.info("Starting cache service manager...")
        self.__cache_service_event = threading.Event()
        self.__cache_service = CacheServiceManager(event=self.__cache_service_event)
        self.__cache_service.start()
        logging.info("Started cache service manager!")

    def __stop_cache_service_manager(self):
        logging.info("Stopping cache service manager...")
        self.__cache_service_event.set()
        self.__cache_service.join()
        logging.info("Stopped cache service manager!")

    async def __console_app_menu(self):
        while True:
            print("1. List all sources status")
            print("2. Exit")
            choice = input("Enter your choice: ")
            if choice == "1":
                pass
            elif choice == "2":
                pass
            else:
                print("Invalid choice. Please try again.")

    async def start(self):
        try:
            self.__setup_logging()  # Setup logging
            self.__load_env_vars()  # Load envirnment
            self.__setup_discord_logger()  # Setup discord logger
            self.__set_engine_parameters()  # Set engine parameters
            self.__start_cache_service_manager()  # Start cache service manager
            self.__init_engine()  # Create engine instance
            self.__start_scraper_task_que()
            await self.__start_api_server()  # Start the API server
            await self.__init_feed_tasks()  # Initiate the feed tasks

            worker_list = []
            for rss_item in self.__engine.engine_store.rss_item_list.values():
                worker_list.append(rss_item.worker)
            await asyncio.gather(*worker_list)  # Start all feed tasks
        except Exception as e:
            logging.error(
                f"Exception occured: ({e.__class__.__name__}) {e.__str__()}",
                exc_info=True,
            )
            exit(1)

    async def stop(self):
        try:
            self.__stop_scraper_task_que()
            self.__stop_cache_service_manager()  # Stop cache service manager
            await self.__destruct_all_tasks()  # Stop all feed tasks
            await self.__stop_api_server()  # Stop the API server
        except Exception as e:
            logging.error(
                f"Exception occured: ({e.__class__.__name__}) {e.__str__()}",
                exc_info=True,
            )
            exit(1)
