from dataclasses import dataclass, field

from .ChannelItem import ChannelItem


@dataclass
class EngineStore:
    """Engine Store dataclass.

    Attributes:
      - min_refresh_interval (float): Minimum refresh interval in seconds for the feed (Default is 10).
      - refresh_buffer (float): Refresh buffer in seconds for the feed (Default is 5).
      - rss_feed_list (dict[str, RSSItem]): List of RSS channel items.
    """
    feed_min_refresh_interval: float = 10.0
    feed_refresh_buffer: float = 5.0
    rss_item_list: dict[int, ChannelItem] = field(default_factory=dict)
