import asyncio
from dataclasses import dataclass
import aiohttp
import feedparser
from datetime import datetime, timedelta

from news_extractor_engine.model import ArticleSource
@dataclass
class FeedReaderData:
  source: ArticleSource
  feed_last_updated_on: datetime | None
  feed_last_refresh_on: datetime | None
  feed: feedparser.FeedParserDict
  has_updated_since_last_request: bool

  def __post_init__(self):
    if not isinstance(self.source, ArticleSource):
      raise TypeError("source must be an instance of ArticleSource")
    if not isinstance(self.feed_last_updated_on, datetime) and self.feed_last_updated_on is not None:
      raise TypeError("feed_last_updated_on must be an instance of datetime")
    if not isinstance(self.feed_last_refresh_on, datetime) and self.feed_last_refresh_on is not None:
      raise TypeError("feed_last_refresh_on must be an instance of datetime")
    if not isinstance(self.feed, feedparser.FeedParserDict):
      raise TypeError("feed must be an instance of feedparser.FeedParserDict")
    if not isinstance(self.has_updated_since_last_request, bool):
      raise TypeError("has_updated_since_last_request must be an instance of bool")

class FeedReader:
  source: ArticleSource
  __feed_last_updated_on: datetime | None = None
  __feed_last_refresh_on: datetime | None = None
  __feed: feedparser.FeedParserDict
  refresh_time: float = 15.0
  __has_updated_since_last_request: bool = False
  __last_feed_item_hash: int = 0

  def __init__(self, source: ArticleSource, refresh_time: float = 15.0) -> None:
    """The FeedReader class is used to fetch and parse RSS feeds from a given source.

    Args:
        source (ArticleSource): The source object from which the feed will be fetched.
        refresh_time (float, optional): The refresh time of the feed. Defaults to 15 seconds.

    Raises:
        TypeError: _description_
    """
    self.refresh_time = refresh_time
    if isinstance(source, ArticleSource):
      self.source = source
    else:
      raise TypeError("source must be an instance of ArticleSource") 

  def __update_feed_update_time(self, feed: feedparser.FeedParserDict):
    new_update_time = None
    date_formats = ["%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"]
    for date_format in date_formats:
      try:
        if "published" in feed.keys():
          new_update_time = datetime.strptime(str(feed['published']), date_format)
        elif "updated" in feed.keys():
          new_update_time = datetime.strptime(str(feed['updated']), date_format)
        elif "updated" in feed['feed'].keys():
          new_update_time = datetime.strptime(str(feed['feed']['updated']), date_format)
        elif "published" in feed['entries'][0].keys():
          new_update_time = datetime.strptime(str(feed['entries'][0]['published']), date_format)
        elif "updated" in feed['entries'][0].keys():
          new_update_time = datetime.strptime(str(feed['entries'][0]['updated']), date_format)
        break
      except ValueError:
        continue

    if isinstance(new_update_time, datetime) and self.__feed_last_updated_on != new_update_time:
      self.__feed_last_updated_on = new_update_time

  async def fetch_feed(self) -> feedparser.FeedParserDict:
    async with aiohttp.ClientSession() as session:
      async with session.get(self.source.rss_url) as response:
        feed_xml = await response.text()
        feed = feedparser.parse(feed_xml)
        if feed.bozo:
          raise ValueError(f"Invalid feed XML: ({self.source.id}, {self.source.name})")
        latest_feed_hash = hash(str(feed['entries'][0]))
        if self.__last_feed_item_hash != latest_feed_hash:
          self.__has_updated_since_last_request = True
          self.__last_feed_item_hash = latest_feed_hash
        self.__feed = feed
        self.__feed_last_refresh_on = datetime.now()
        self.__update_feed_update_time(feed)
        return feed
  
  async def get_feed(self) -> FeedReaderData:
    boundry_of_no_refresh = datetime.now() - timedelta(seconds=self.refresh_time)
    if self.__feed_last_refresh_on is None or self.__feed_last_refresh_on < boundry_of_no_refresh:
      await self.fetch_feed()
    data = FeedReaderData(
      source=self.source,
      feed_last_updated_on=self.__feed_last_updated_on,
      feed_last_refresh_on=self.__feed_last_refresh_on,
      feed=self.__feed,
      has_updated_since_last_request=self.__has_updated_since_last_request
    )
    self.__has_updated_since_last_request = False
    return data
  
  def get_last_updated_on(self) -> datetime | None:
      return self.__feed_last_updated_on
