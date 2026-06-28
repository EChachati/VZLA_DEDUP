from scrapers.adapters.api_adapter import ApiAdapter
from scrapers.adapters.base import AdapterProtocol, RawContent
from scrapers.adapters.html_adapter import HtmlAdapter
from scrapers.adapters.rss_adapter import RssAdapter

__all__ = [
    "AdapterProtocol",
    "RawContent",
    "ApiAdapter",
    "HtmlAdapter",
    "RssAdapter",
]
