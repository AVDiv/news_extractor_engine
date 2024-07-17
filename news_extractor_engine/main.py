import builtins
import os
import logging
import asyncio
from time import sleep
import markdownify
from typing import Coroutine

import polars as pl
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

from news_extractor_engine.engine.feed import FeedReader, FeedReaderData
from news_extractor_engine.model import ArticleSource
from news_extractor_engine.utils.devtools import dev_mode
from news_extractor_engine.utils.discord_tools import DiscordLogger

load_dotenv()

ENVIRONMENT = os.getenv("ENVIRONMENT")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MONGO_CONFIG_DB_CONN_STRING = os.getenv("MONGODB_CONFIG_DB_CONNECTION_STRING")

builtins.ENVIRONMENT = ENVIRONMENT

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


feed_list: list[FeedReader] = []
worker_list: list[asyncio.Task] = []
discord = DiscordLogger(DISCORD_WEBHOOK_URL)
sources_db = MongoClient(MONGO_CONFIG_DB_CONN_STRING).get_default_database()
sources_collection = sources_db.get_collection("source")
sources_pipeline = [
      {
        '$match': {
          'is_enabled': True
        }
      },
      {
        '$project':
          {
            'title': 1,
            'rss': 1,
            'domain': 1,
            'channels': 1
          }
      },
      {
        '$limit': 5
      }
    ]

async def create_feed_reader_task(feed: FeedReader):
  while True:
    print(f"Checking for new articles from {feed.source.name}")
    try:
      feed_data = await feed.get_feed()
      if feed_data.has_updated_since_last_request:
        discord.send_embed(
          title=f"## **{feed.source.name}**",
          description=f"**{feed_data.feed['entries'][0]['title']}**\n{markdownify.markdownify(feed_data.feed['entries'][0]['summary'])}",
          url=str(feed_data.feed['entries'][0]['link']),
          color=0xddcfee
        )
    except Exception as e:
      logging.error(e)
      return
    await asyncio.sleep(10)

def is_source_valid(data: FeedReaderData):
  if data.feed['bozo'] != False:
    return False
  return True


async def main():
  current_directory = os.path.dirname(__file__)
  # news_sources = pl.read_csv(f"{current_directory}/config/news_sources.csv")
  news_sources = sources_db.source.aggregate(sources_pipeline)
  with dev_mode():
    feed_check_list = []
    print(news_sources)
  for doc in news_sources:
    source = ArticleSource(
      id=doc['_id'],
      name=doc['title'],
      domain=doc['domain'],
      rss_url=doc['rss'],
      categories=doc['channels']
    )
    rss_feed = FeedReader(source)
    feed_list.append(rss_feed)
    with dev_mode():
      try:
        data = await rss_feed.get_feed()
        feed_check_list.append(f"{source.name}: {'✅' if is_source_valid(data) else '❌'}")
      except Exception as e:
        feed_check_list.append(f"{source.name}: {e.__class__.__name__}")
        logging.error(e)

  for feed in feed_list:
    task = asyncio.create_task(create_feed_reader_task(feed), name=feed.source.name)
    worker_list.append(task)
  discord.send_embed(title="Articles status:", description="\n".join(feed_check_list))
  await asyncio.wait(worker_list)

async def destruct():
  for worker in worker_list:
    worker.cancel()
  loop.stop()

if __name__ == "__main__":
  try:
    loop.run_until_complete(main())
  except KeyboardInterrupt as e:
    webhook = discord.send_embed(title="Terminating...", description=f"Shutting down the news extractor engine...")
    sleep(3)
    logging.error(e)
  except Exception as e:
    webhook = discord.send_embed(title="Error", description=f"An error occurred while running the news extractor engine:\n```{e}```")
    logging.error(e)
  finally:
    asyncio.run(destruct())
    discord.send_embed(title="Terminated", description=f"The news extractor engine was shutdown <t:{int(datetime.now().timestamp())}:R>.", webhook=webhook)