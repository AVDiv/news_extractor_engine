from dataclasses import dataclass
from asyncio import Task

from news_extractor_engine.engine.feed import FeedReader


@dataclass
class ChannelItem:
    """Channel Item dataclass.

    Attributes:
      - id (int): ID of the channel.
      - name (str): Name of the channel.
      - worker (Task): Task object which sync with the channel.
      - feed (FeedReader): Feed object for the channel.
    """

    id: int
    name: str
    worker: Task
    feed: FeedReader
