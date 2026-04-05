"""Tests for the base connector infrastructure."""

from __future__ import annotations

import pytest
import respx
import httpx

from atlas_core.connectors.base import BaseConnector, _TokenBucket
from atlas_core.exceptions import ConnectorAuthError, ConnectorHTTPError, ConnectorRateLimitError


class _ConcreteConnector(BaseConnector):
    """Minimal concrete connector for testing."""
    source_name = "test"

    async def health_check(self) -> bool:
        return True


@pytest.mark.asyncio
async def test_token_bucket_throttles():
    """Token bucket should delay when empty."""
    import time
    bucket = _TokenBucket(rate=10.0, capacity=1.0)
    t0 = time.monotonic()
    await bucket.acquire(1.0)
    await bucket.acquire(1.0)  # should require ~0.1s wait
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.05  # at least 50ms


@pytest.mark.asyncio
async def test_connector_cache_round_trip(tmp_path):
    """Connector should cache responses and return cached value."""
    connector = _ConcreteConnector(cache_dir=tmp_path / "cache", cache_ttl=3600)
    test_data = {"foo": "bar", "n": 42}

    key = connector._cache_key("http://example.com", {"k": "v"})
    connector._write_cache(key, test_data)
    result = connector._read_cache(key)
    assert result == test_data


@pytest.mark.asyncio
async def test_connector_cache_expired(tmp_path):
    """Stale cache entries should be discarded."""
    connector = _ConcreteConnector(cache_dir=tmp_path / "cache", cache_ttl=0)
    key = connector._cache_key("http://example.com/x", {})
    connector._write_cache(key, {"stale": True})
    import time; time.sleep(0.01)
    result = connector._read_cache(key)
    assert result is None


@pytest.mark.asyncio
async def test_connector_raises_auth_error():
    """401 responses should raise ConnectorAuthError."""
    connector = _ConcreteConnector()
    with respx.mock:
        respx.get("http://example.com/auth").mock(
            return_value=httpx.Response(401)
        )
        async with connector:
            with pytest.raises(ConnectorAuthError):
                await connector._get("http://example.com/auth", use_cache=False)


@pytest.mark.asyncio
async def test_connector_raises_rate_limit():
    """429 responses should raise ConnectorRateLimitError."""
    connector = _ConcreteConnector()
    with respx.mock:
        respx.get("http://example.com/rate").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "5"})
        )
        async with connector:
            with pytest.raises(ConnectorRateLimitError) as exc_info:
                await connector._get("http://example.com/rate", use_cache=False)
            assert exc_info.value.retry_after == 5.0


@pytest.mark.asyncio
async def test_connector_raises_http_error():
    """Non-2xx non-auth responses should raise ConnectorHTTPError."""
    connector = _ConcreteConnector()
    with respx.mock:
        respx.get("http://example.com/err").mock(
            return_value=httpx.Response(503, text="Service unavailable")
        )
        async with connector:
            with pytest.raises(ConnectorHTTPError) as exc_info:
                await connector._get("http://example.com/err", use_cache=False)
            assert exc_info.value.status_code == 503
