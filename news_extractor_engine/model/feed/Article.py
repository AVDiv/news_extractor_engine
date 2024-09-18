from dataclasses import dataclass

from datetime import datetime

from news_extractor_engine.model.feed.ArticleSource import ArticleSource


@dataclass
class Article:
    """
    Represents an article extracted from a news source.

    Attributes:
      title (str): The title of the article.
      author (list): The list of authors of the article.
      publication_date (datetime): The publication date of the article.
      source (ArticleSource): The source of the article.
      url (str): The URL of the article.
      summary (str): A summary of the article.
      content (str): The full content of the article.
      tags (list[str]): A list of tags associated with the article.
      categories (list[str]): A list of categories the article belongs to.
      images (list[str]): A list of URLs of images associated with the article.
    """

    title: str
    author: list
    publication_date: datetime
    source: ArticleSource
    url: str
    summary: str
    content: str
    tags: list[str]
    categories: list[str]
    images: list[str]
