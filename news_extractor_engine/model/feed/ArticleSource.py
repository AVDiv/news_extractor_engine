from dataclasses import dataclass


@dataclass
class ArticleSource:
    """
    Represents a source of news articles.

    Attributes:
      id (str): The ID of the source.
      name (str): The name of the source.
      domain (str): The domain of the source.
      rss_url (str): The URL of the RSS feed of the source.
      categories (list[str]): A list of categories the source belongs to.
    """

    id: str
    name: str
    domain: str
    rss_url: str
    categories: list[str]
