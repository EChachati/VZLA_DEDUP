from __future__ import annotations

import re

import pytest

from scrapers.adapters import AdapterProtocol
from scrapers.adapters.rss_adapter import RssAdapter
from scrapers.models.source import SourceConfig

RSS_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Feed Demo</title>
    <item>
      <title>Reporte 1</title>
      <description>Persona vista en Maracay</description>
      <link>https://example.test/1</link>
    </item>
    <item>
      <title>Reporte 2</title>
      <description>Centro de acopio en Valencia</description>
      <link>https://example.test/2</link>
    </item>
  </channel>
</rss>
"""

ITEM_1 = "Reporte 1 Persona vista en Maracay https://example.test/1"
ITEM_2 = "Reporte 2 Centro de acopio en Valencia https://example.test/2"


def _source_config(
    source_type: str = "rss",
    url: str = "https://example.test/feed.xml",
) -> SourceConfig:
    return SourceConfig(
        id="rss_demo",
        name="RSS Demo",
        type=source_type,
        enabled=True,
        trust_tier="C",
        url=url,
        refresh_minutes=30,
        parser_asignado="rss",
    )


def _fake_fetcher(
    raw: str = RSS_SAMPLE, content_type: str = "application/rss+xml"
):
    calls: list[tuple[str, int]] = []

    def fetcher(url: str, timeout: int) -> tuple[str, str]:
        calls.append((url, timeout))
        return raw, content_type

    return fetcher, calls


def test_adapter_satisfies_protocol() -> None:
    adapter = RssAdapter(source_key="demo", fetcher=_fake_fetcher()[0])

    assert isinstance(adapter, AdapterProtocol)


def test_fetch_all_yields_one_raw_content_per_item() -> None:
    fetcher, _calls = _fake_fetcher()
    adapter = RssAdapter(source_key="rss_demo", fetcher=fetcher)

    results = list(adapter.fetch_all("https://example.test/feed.xml"))

    assert len(results) == 2
    assert [r["raw_content"] for r in results] == [ITEM_1, ITEM_2]
    assert [r["rss_title"] for r in results] == ["Reporte 1", "Reporte 2"]
    assert [r["page"] for r in results] == [1, 2]
    assert all(r["total_pages"] == 2 for r in results)
    assert all(r["rss_item_count"] == 2 for r in results)


def test_fetch_all_raw_content_has_standard_fields() -> None:
    fetcher, calls = _fake_fetcher()
    adapter = RssAdapter(source_key="rss_demo", fetcher=fetcher, timeout=9)

    first = next(adapter.fetch_all("https://example.test/feed.xml"))

    assert calls[0] == ("https://example.test/feed.xml", 9)
    assert first["source_key"] == "rss_demo"
    assert first["source_url"] == "https://example.test/feed.xml"
    assert first["http_status"] == 200
    assert first["content_type"] == "application/rss+xml"
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", first["content_hash"])
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", first["fetched_at"])


def test_fetch_returns_whole_feed_as_single_raw_content() -> None:
    fetcher, _calls = _fake_fetcher()
    adapter = RssAdapter(source_key="rss_demo", fetcher=fetcher)

    result = adapter.fetch("https://example.test/feed.xml")

    assert result["raw_content"] == f"{ITEM_1}\n{ITEM_2}"
    assert result["page"] is None
    assert result["total_pages"] is None
    assert result["rss_title"] is None
    assert result["rss_item_count"] == 2


def test_fetch_infers_source_key_from_url_when_not_configured() -> None:
    fetcher, _calls = _fake_fetcher()
    adapter = RssAdapter(fetcher=fetcher)

    result = adapter.fetch("https://feeds.example.test/rss")

    assert result["source_key"] == "feeds.example.test"


def test_from_source_config_uses_id_as_source_key() -> None:
    fetcher, calls = _fake_fetcher()
    config = _source_config(url="https://example.test/config-feed.xml")
    adapter = RssAdapter.from_source_config(config, fetcher=fetcher)

    result = adapter.fetch_source(config)

    assert calls == [("https://example.test/config-feed.xml", 25)]
    assert result["source_key"] == "rss_demo"
    assert result["source_url"] == "https://example.test/config-feed.xml"


def test_rejects_non_rss_source_config() -> None:
    fetcher, _calls = _fake_fetcher()
    config = _source_config(source_type="html_static")

    with pytest.raises(ValueError, match="only supports source type 'rss'"):
        RssAdapter.from_source_config(config, fetcher=fetcher)


def test_feed_without_items_falls_back_to_full_text() -> None:
    empty_feed = '<rss version="2.0"><channel><title>Sin items</title></channel></rss>'
    fetcher, _calls = _fake_fetcher(raw=empty_feed)
    adapter = RssAdapter(source_key="rss_demo", fetcher=fetcher)

    results = list(adapter.fetch_all("https://example.test/feed.xml"))

    assert len(results) == 1
    assert results[0]["rss_title"] is None
    assert "Sin items" in results[0]["raw_content"]


def test_malformed_xml_degrades_to_raw_text_without_raising() -> None:
    fetcher, _calls = _fake_fetcher(raw="esto no es   xml <<< roto")
    adapter = RssAdapter(source_key="rss_demo", fetcher=fetcher)

    results = list(adapter.fetch_all("https://example.test/feed.xml"))

    assert len(results) == 1
    # No se descarta el feed: el texto crudo (normalizado) queda como item único.
    assert results[0]["raw_content"] == "esto no es xml <<< roto"


def test_fetch_all_propagates_fetcher_errors() -> None:
    def boom(url: str, timeout: int) -> tuple[str, str]:
        raise RuntimeError("network down")

    adapter = RssAdapter(source_key="rss_demo", fetcher=boom)

    with pytest.raises(RuntimeError, match="network down"):
        list(adapter.fetch_all("https://example.test/feed.xml"))


def test_pipeline_registry_returns_rss_adapter() -> None:
    from scrapers.pipelines.run_pipeline import _get_adapter

    adapter = _get_adapter(_source_config())

    assert isinstance(adapter, RssAdapter)
    assert adapter.source_key == "rss_demo"
