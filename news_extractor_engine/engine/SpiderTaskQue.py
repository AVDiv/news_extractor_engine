import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd
import zmq
from bson.objectid import ObjectId
from confluent_kafka import Producer

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

    def __init__(
        self,
        *,
        event: threading.Event,
        datalake_path: str = "data/articles",
    ):
        super().__init__()
        self.__event = event
        self.__context = zmq.Context()
        self.__socket = self.__context.socket(zmq.PULL)
        self.__socket.bind("tcp://*:5555")
        self.__datalake_table = datalake_path

        # Initialize the Kafka producer manager
        self.__kafka_manager = KafkaProducerManager(num_producers=3)

    def run(self):
        with ThreadPoolExecutor(max_workers=3) as executor:
            while not self.__event.is_set():
                try:
                    message = self.__socket.recv_json()
                    if isinstance(message, dict):
                        logging.debug(
                            f"Received request for crawler: {message['name']}"
                        )
                        future = executor.submit(
                            self.__mine_article,
                            message["url"],
                            RSS_SOURCE_LIST[ObjectId(message["source_id"])].feed.source,
                        )
                        future.add_done_callback(
                            lambda f: logging.debug(
                                f"Completed crawler task: {message['url']}"
                            )
                        )

                except Exception as e:
                    logging.error(
                        f"Task Que Error: ({e.__class__.__name__}) {e.__str__()}"
                    )

        # Flush and close all Kafka producers before exiting
        self.__kafka_manager.flush_all()
        self.__kafka_manager.close_all()

    def __mine_article(self, url: str, source: ArticleSource):
        try:
            print(f"Mining article: {url}")
            article: Article = ArticleSpider.extract_data(url, source)
            article_dict = article.__dict__
            article_dict["id"] = article_dict["id"].__str__()
            article_dict["source"] = article_dict["source"].__str__()
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

            # Try to publish to Kafka first
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
                for key, value in article_dict.items():
                    if isinstance(value, list) or isinstance(value, set):
                        if len(value) == 0:
                            article_dict[key] = "NULL"
                        else:
                            article_dict[key] = " ,".join(article_dict[key])
                    elif value is None:
                        article_dict[key] = "NULL"
                    else:
                        article_dict[key] = [value]

                article_df = pd.DataFrame(article.__dict__)
                write_deltalake(self.__datalake_table, article_df, mode="append")
                logging.info(f"Article {article_dict['id']} written to DeltaLake")
            else:
                logging.info(
                    f"Article {article_dict['id']} published to Kafka topic {self.__kafka_topic}"
                )

        except Exception as e:
            logging.error(
                f"Data miner Error: ({e.__class__.__name__}) {e.__str__()}",
                exc_info=True,
            )

    def join(self):
        self.__socket.close()
        self.__context.term()
        super().join()
