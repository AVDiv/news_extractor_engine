class ScraperException(Exception):
    """Base class for exceptions in this module."""
    pass

class InvalidDomainInArticleUrlException(ScraperException):
    """Exception raised for receiving an Article URL with a non-matching domain of the scraper."""
    pass
