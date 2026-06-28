"""
scrapers/adapters/rss_adapter.py
================================
Adapter para fuentes de tipo ``rss``: feeds RSS/Atom.

Descarga un feed y extrae sus ``<item>`` con el parser RSS existente
(``scrapers/parsers/rss_extractor.py``), igual que ``HtmlAdapter`` envuelve
``html_extractor.py``.  No interpreta el significado del contenido ni persiste
nada — solo produce ``RawContent`` para el resto del pipeline.

Contrato de salida
------------------
``fetch_all`` produce un ``RawContent`` por cada ``<item>`` del feed (cada
item es un registro independiente; el pipeline los procesa como "páginas").
``fetch`` devuelve el feed completo como un único ``RawContent`` con los textos
de todos los items unidos.  En ambos casos ``raw_content`` es texto (``str``).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urlparse

from defusedxml.ElementTree import ParseError

from scrapers.adapters._shared import now_utc, sha256_hex
from scrapers.adapters.base import RawContent
from scrapers.adapters.http_client import fetch_url
from scrapers.models.source import SourceConfig
from scrapers.parsers.rss_extractor import extract_rss_items

RssFetcher = Callable[[str, int], tuple[str, str]]

_DEFAULT_TIMEOUT = 25


class RssAdapter:
    """Adapter para feeds RSS/Atom estáticos.

    Parameters
    ----------
    source_key:
        Identificador de la fuente (de ``SourceConfig.id``).
    fetcher:
        Función de fetch inyectable ``(url, timeout) -> (raw, content_type)``.
        Default: ``fetch_url`` de ``http_client``.  Permite tests sin red.
    timeout:
        Timeout en segundos para el fetch (default: 25s).
    """

    def __init__(
        self,
        source_key: str | None = None,
        *,
        fetcher: RssFetcher = fetch_url,
        timeout: int = _DEFAULT_TIMEOUT,
    ) -> None:
        self.source_key = source_key
        self.fetcher = fetcher
        self.timeout = timeout

    @classmethod
    def from_source_config(
        cls,
        source_config: SourceConfig,
        *,
        fetcher: RssFetcher = fetch_url,
        timeout: int = _DEFAULT_TIMEOUT,
    ) -> "RssAdapter":
        _validate_rss(source_config)
        return cls(source_key=source_config.id, fetcher=fetcher, timeout=timeout)

    def fetch(
        self,
        url: str,
        *,
        source_config: SourceConfig | None = None,
        timeout: int | None = None,
        **_: Any,
    ) -> RawContent:
        """Devuelve el feed completo como un único RawContent.

        ``raw_content`` es el texto de todos los items unidos por saltos de
        línea.  Para procesar cada item por separado, usar ``fetch_all``.
        """
        source_key, url, content_type, items = self._load(url, source_config, timeout)
        text = "\n".join(item_text for _title, item_text in items)
        return _raw_content(
            source_key=source_key,
            source_url=url,
            content_type=content_type,
            text=text,
            page=None,
            total_pages=None,
            rss_title=None,
            rss_item_count=len(items),
        )

    def fetch_source(self, source_config: SourceConfig, **kwargs: Any) -> RawContent:
        return self.fetch(source_config.url, source_config=source_config, **kwargs)

    def fetch_all(
        self,
        url: str,
        *,
        source_config: SourceConfig | None = None,
        timeout: int | None = None,
        **_: Any,
    ) -> Iterator[RawContent]:
        """Produce un RawContent por cada ``<item>`` del feed."""
        source_key, url, content_type, items = self._load(url, source_config, timeout)
        total = len(items)
        for idx, (title, item_text) in enumerate(items, start=1):
            yield _raw_content(
                source_key=source_key,
                source_url=url,
                content_type=content_type,
                text=item_text,
                page=idx,
                total_pages=total,
                rss_title=title,
                rss_item_count=total,
            )

    def _load(
        self,
        url: str,
        source_config: SourceConfig | None,
        timeout: int | None,
    ) -> tuple[str | None, str, str, list[tuple[str | None, str]]]:
        if source_config is not None:
            _validate_rss(source_config)
            source_key: str | None = source_config.id
            url = source_config.url
        else:
            source_key = self.source_key or _source_key_from_url(url)

        raw, content_type = self.fetcher(url, timeout or self.timeout)
        items = _safe_extract_items(raw)
        return source_key, url, content_type, items


def _raw_content(
    *,
    source_key: str | None,
    source_url: str,
    content_type: str,
    text: str,
    page: int | None,
    total_pages: int | None,
    rss_title: str | None,
    rss_item_count: int,
) -> RawContent:
    return RawContent(
        source_key=source_key,
        source_url=source_url,
        fetched_at=now_utc(),
        # fetch_url() hace raise_for_status(): si llegamos aquí, la respuesta fue
        # 2xx, así que 200 es seguro.  http_client no expone el código real; si
        # cambia su contrato (ver #60), surfacing el status real aquí en vez de
        # hardcodearlo.
        http_status=200,
        content_type=content_type,
        content_hash=sha256_hex(text.encode("utf-8")),
        raw_content=text,
        page=page,
        total_pages=total_pages,
        rss_title=rss_title,
        rss_item_count=rss_item_count,
    )


def _safe_extract_items(raw: str) -> list[tuple[str | None, str]]:
    """``extract_rss_items`` con degradación.

    Si el XML está mal formado no se descarta el feed (en una crisis cada
    registro cuenta): se devuelve su texto crudo, normalizado, como un único
    item sin título.
    """
    try:
        return extract_rss_items(raw)
    except ParseError:
        return [(None, " ".join(raw.split()))]


def _validate_rss(source_config: SourceConfig) -> None:
    if source_config.type != "rss":
        raise ValueError(
            f"RssAdapter only supports source type 'rss'; got {source_config.type!r}"
        )


def _source_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc or url
