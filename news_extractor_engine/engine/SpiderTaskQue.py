import zmq
import time
import logging
import threading

from concurrent.futures import ThreadPoolExecutor
from news_extractor_engine.model.feed import Article, ArticleSource

# from scrapy.crawler import CrawlerProcess
# from scrapy.utils.project import get_project_settings

from news_extractor_engine.main import app
from news_extractor_engine.engine.scraper import ArticleSpider

class SpiderTaskQue(threading.Thread):
  __event: threading.Event
  __context: zmq.Context
  __socket: zmq.SyncSocket

  def __init__(self, event: threading.Event):
    super().__init__()
    self.__event = event
    self.__context = zmq.Context()
    self.__socket = self.__context.socket(zmq.PULL)
    self.__socket.bind("tcp://*:5555")

  def run(self):
    # process = CrawlerProcess(get_project_settings())
    # while not self.__event.is_set:
    #   message = self.__socket.recv_json()
    #   if isinstance(message, dict):
    #     logging.debug(f"Received request for crawler: {message['name']}")
    #     process.crawl(ArticleSpider, start_urls=[message['url']], article_xpaths=message['xpaths'])
    #     process.start()
    with ThreadPoolExecutor(max_workers=3) as executor:
      while not self.__event.is_set:
        messsage = self.__socket.recv_json()
        if isinstance(message, dict):
          logging.debug(f"Received request for crawler: {message['name']}")
          executor.submit(self.__mine_article, message['url'], app.__engine.engine_store.rss_item_list[message['id']].feed.source)

  def __mine_article(self, url: str, source: ArticleSource):
    article: Article = ArticleSpider.extract_data(url, source)
    
  
  def join(self):
    self.__socket.close()
    self.__context.term()
    super().join()