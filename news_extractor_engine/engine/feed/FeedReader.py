import aiohttp
import feedparser

from news_extractor_engine.model import ArticleSource

class FeedReader:
  source: ArticleSource

  def __init__(self, source: ArticleSource) -> None:
    if isinstance(source, ArticleSource):
      self.source = source
    else:
      raise TypeError("source must be an instance of ArticleSource") 

  async def fetch_feed(self) -> feedparser.FeedParserDict:
    async with aiohttp.ClientSession() as session:
      async with session.get(self.source.rss_url) as response:
        feed_xml = await response.text()
        feed = feedparser.parse(feed_xml)
        if feed.bozo:
          raise ValueError(f"Invalid feed XML: ({self.source.id}, {self.source.name})")
        return feed