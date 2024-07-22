from dataclasses import dataclass
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup
from lxml import etree


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
    url: str
    xpaths: ArticleInfoXpaths

    def __init__(self, name: str, url: str, xpaths: ArticleInfoXpaths):
        self.name = name
        self.url = url
        self.xpaths = xpaths

    async def fetch_article(self) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                html = await response.text()
                return self.parse_article(html)

    def parse_article(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        tree = etree.parse(str(soup), etree.HTMLParser())
        article = {}
        for key, value in self.xpaths.__dict__.items():
            if value:
                article[key] = tree.xpath(value)
        return article
