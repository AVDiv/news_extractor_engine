from dataclasses import dataclass
import logging
# import threading
from typing import Optional

import aiohttp
# import scrapy
from bson.objectid import ObjectId
import newspaper as news
from bs4 import BeautifulSoup
from lxml import etree
from urllib.parse import urlparse

from news_extractor_engine.model.feed import Article, ArticleSource
from news_extractor_engine.model.error.Scraper import InvalidDomainInArticleUrlException


@dataclass
class ArticleInfoXpaths:
    title: Optional[str]
    author: Optional[str]
    publication_date: Optional[str]
    summary: Optional[str]
    content: Optional[str]
    tags: Optional[str]
    categories: Optional[str]


class ArticleScraper:
    name: str
    domain: str
    xpaths: ArticleInfoXpaths

    def __init__(self, name: str, domain: str, xpaths: ArticleInfoXpaths):
        self.name = name
        self.domain = domain
        self.xpaths = xpaths

    async def fetch_article(self, url) -> dict:
        url_domain = urlparse(url).netloc
        if url_domain != self.domain:
            raise InvalidDomainInArticleUrlException(f"{self.name}: URL domain {url_domain} does not match scraper domain {self.domain}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                return self.__parse_article(html)

    def __parse_article(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        tree = etree.parse(str(soup), etree.HTMLParser())
        article = {}
        for key, value in self.xpaths.__dict__.items():
            if value:
                article[key] = tree.xpath(value)
        return article

# class ArticleSpider(scrapy.Spider):
#     name: str = "ArticleSpider"
#     custom_settings: dict[str, str | int | dict | bool | None] = {
#         'BOT_NAME': 'pulsebee-daemon',
#     }
#     article_xpaths: dict

#     def parse(self, response):
#         data = {
#             'title': self.article_xpaths['title'],
#             'author': self.article_xpaths['author'],
#             'publication_date': self.article_xpaths['publication_date'],
#             'summary': self.article_xpaths['summary'],
#             'content': self.article_xpaths['content'],
#             'tags': self.article_xpaths['tags'],
#             'categories': self.article_xpaths['categories'],
#         }
#         logging.debug(f"Extracted data for: {data['title']}")

class ArticleSpider:
    def __init__(self):
        pass

    @classmethod
    def extract_data(cls, url: str, source: ArticleSource) -> Article:
        article = cls.__download_data(url)
        return cls.__scrape_data(article, source)

    @classmethod
    def __download_data(cls, url: str):
        article = news.Article(url)
        article.download()
        article.parse()
        return article

    @classmethod
    def __scrape_data(cls, article: news.Article, source: ArticleSource) -> Article:
        article_data = Article(
            id=ObjectId(),
            title=article.title,
            author=article.authors,
            publication_date=article.publish_date,
            source=source,
            url=article.url,
            summary=article.summary,
            content=article.text,
            tags=article.tags,
            categories=[],
            images=article.images
        )
        return article_data
