import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import zmq
from bson.objectid import ObjectId

# from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from news_extractor_engine.engine.kafka_manager import KafkaProducerManager
from news_extractor_engine.engine.scraper import ArticleSpider
from news_extractor_engine.model.feed import Article, ArticleSource


# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings
class SpiderTaskQue(threading.Thread):
    __event: threading.Event
    __context: zmq.Context
    __socket: zmq.Socket
    __datalake_table: str
    __kafka_manager: KafkaProducerManager
    __thread_pool: ThreadPoolExecutor = None
    __MAX_CONCURRENT_WORKERS = 3
    __SOCKET_HWM = 100  # High water mark for socket buffer

    def __init__(
        self,
        *,
        event: threading.Event,
        datalake_path: str = "data/articles",
    ):
        super().__init__()
        self.__event = event
        self.__context = zmq.Context()
        self.__context.set(zmq.MAX_SOCKETS, 100)  # Set ZMQ socket limit

        # Create socket with resource limits
        self.__socket = self.__context.socket(zmq.PULL)
        self.__socket.setsockopt(
            zmq.RCVHWM, self.__SOCKET_HWM
        )  # Set receive buffer size limit
        self.__socket.setsockopt(
            zmq.LINGER, 0
        )  # Don't keep messages in memory after close
        self.__socket.bind("tcp://*:5555")

        self.__datalake_table = datalake_path

        # Initialize the Kafka producer manager with a fixed number of producers
        # This creates a pool with 3 dedicated worker threads for Kafka publishing
        self.__kafka_manager = KafkaProducerManager(num_producers=3)

        # We'll initialize the thread pool when we start running, not during init

    def run(self):
        # Create the thread pool at runtime
        self.__thread_pool = ThreadPoolExecutor(
            max_workers=self.__MAX_CONCURRENT_WORKERS
        )

        # Set a polling timeout so we can check the event periodically
        poller = zmq.Poller()
        poller.register(self.__socket, zmq.POLLIN)

        futures = []
        try:
            while not self.__event.is_set():
                try:
                    # Poll with timeout so we can check if we should exit
                    socks = dict(poller.poll(1000))  # 1 second timeout

                    if self.__socket in socks and socks[self.__socket] == zmq.POLLIN:
                        message = self.__socket.recv_json(zmq.NOBLOCK)
                        if isinstance(message, dict):
                            logging.debug(
                                f"Received request for crawler: {message['name']}"
                            )
                            future = self.__thread_pool.submit(
                                self.__mine_article,
                                message["url"],
                                RSS_SOURCE_LIST[
                                    ObjectId(message["source_id"])
                                ].feed.source,
                            )
                            future.add_done_callback(
                                lambda f: logging.debug(
                                    f"Completed crawler task: {message['url']}"
                                )
                            )
                            futures.append(future)

                            # Clean up completed futures
                            futures = [f for f in futures if not f.done()]

                except zmq.Again:
                    # No messages available, just continue
                    continue
                except Exception as e:
                    logging.error(
                        f"Task Queue Error: ({e.__class__.__name__}) {e.__str__()}"
                    )
        finally:
            # Make sure we clean up resources properly
            self.__cleanup()

    def __cleanup(self):
        """Properly clean up resources when shutting down."""
        logging.info("Cleaning up SpiderTaskQue resources")

        # Shutdown thread pool properly
        if self.__thread_pool:
            self.__thread_pool.shutdown(wait=True, cancel_futures=False)

        # Flush and close all Kafka producers
        if hasattr(self, "__kafka_manager") and self.__kafka_manager:
            self.__kafka_manager.flush_all()
            self.__kafka_manager.close_all()

        # Close ZMQ resources
        if hasattr(self, "__socket") and self.__socket:
            self.__socket.close()
        if hasattr(self, "__context") and self.__context:
            self.__context.term()

    def __mine_article(self, url: str, source: ArticleSource):
        try:
            logging.info(f"Mining article: {url}")
            article: Article = ArticleSpider.extract_data(url, source)
            article_dict = article.__dict__
            article_dict["id"] = article_dict["id"].__str__()
            article_dict["source"] = article_dict["source"].name
            article_dict["publication_date"] = (
                article_dict["publication_date"].strftime("%Y-%m-%dT%H:%M:%S.%f%z")
                if isinstance(article_dict["publication_date"], datetime)
                else article_dict["publication_date"]
            )

            # Process article data for Kafka
            kafka_article = {}
            for key, value in article_dict.items():
                if isinstance(value, list) or isinstance(value, set):
                    if len(value) == 0:
                        kafka_article[key] = "NULL"
                    else:
                        kafka_article[key] = " ,".join(article_dict[key])
                elif value is None:
                    kafka_article[key] = "NULL"
                else:
                    kafka_article[key] = value

            # Try to publish to Kafka using the queue-based producer
            kafka_success = self.__kafka_manager.publish_message(
                key=str(article_dict["id"]),
                value=kafka_article,
            )

            # If Kafka publishing fails, fall back to DeltaLake
            if not kafka_success:
                logging.warning(
                    f"Failed to publish article {article_dict['id']} to Kafka, falling back to DeltaLake"
                )

                # Format data for DeltaLake (as lists for DataFrame)
                delta_article = {}
                for key, value in article_dict.items():
                    if isinstance(value, list) or isinstance(value, set):
                        if len(value) == 0:
                            delta_article[key] = "NULL"
                        else:
                            delta_article[key] = " ,".join(value)
                    elif value is None:
                        delta_article[key] = "NULL"
                    else:
                        delta_article[key] = [value]

                article_df = pd.DataFrame(delta_article)
                write_deltalake(self.__datalake_table, article_df, mode="append")
                logging.info(f"Article {article_dict['id']} written to DeltaLake")
            else:
                topic = self.__kafka_manager._KafkaProducerManager__KAFKA_PRODUCER_TOPIC
                logging.info(
                    f"Article {article_dict['id']} published to Kafka topic {topic}"
                )

        except Exception as e:
            logging.error(
                f"Data miner Error: ({e.__class__.__name__}) {e.__str__()}",
                exc_info=True,
            )

    def join(self, timeout=None):
        """Properly join the thread, ensuring all resources are cleaned up."""
        self.__cleanup()
        super().join(timeout=timeout)
