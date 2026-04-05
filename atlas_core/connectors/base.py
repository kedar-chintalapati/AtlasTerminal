"""
Atlas Terminal — abstract base connector.

Every data-source connector inherits from ``BaseConnector``.  The base class
handles:

* Async HTTP via ``httpx.AsyncClient`` with connection pooling.
* Automatic retries with exponential back-off via ``tenacity``.
* Per-source rate limiting (token-bucket in memory).
* Structured logging via ``loguru``.
* Transparent response-level caching to disk (JSON / text) with configurable TTL.

Subclasses must implement
-------------------------
``source_name`` class attribute  — used in log messages and error context.
``_fetch(...)``                   — raw data-retrieval coroutine; no retry/cache
                                    logic needed there.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import httpx
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from atlas_core.config import settings
from atlas_core.exceptions import (
    ConnectorAuthError,
    ConnectorHTTPError,
    ConnectorRateLimitError,
    ConnectorTimeoutError,
)


class _TokenBucket:
    """Simple token-bucket rate limiter (thread/coroutine-safe via asyncio.Lock)."""

    def __init__(self, rate: float, capacity: float) -> None:
        self._rate = rate        # tokens per second
        self._capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            if self._tokens < tokens:
                wait = (tokens - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= tokens


class BaseConnector(ABC):
    """Abstract base for all Atlas data-source connectors."""

    # Subclasses must set this
    source_name: str = "base"

    # Per-source rate-limit defaults (tokens/sec, bucket capacity)
    _rate_limit_rps: float = 2.0
    _rate_limit_burst: float = 5.0

    def __init__(
        self,
        *,
        cache_dir: Optional[Path] = None,
        cache_ttl: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self._cache_dir = cache_dir or settings.cache_dir / self.source_name
        self._cache_ttl = cache_ttl if cache_ttl is not None else settings.cache_ttl_seconds
        self._timeout = timeout or settings.http_timeout_seconds
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._bucket = _TokenBucket(self._rate_limit_rps, self._rate_limit_burst)
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------ #
    # Context-manager support                                              #
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> "BaseConnector":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._timeout),
            follow_redirects=True,
            headers={"User-Agent": "AtlasTerminal/0.1 (research; contact atlas@terminal.dev)"},
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------ #
    # HTTP helpers                                                         #
    # ------------------------------------------------------------------ #

    def _cache_key(self, url: str, params: dict[str, Any]) -> str:
        raw = json.dumps({"url": url, "params": params}, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()

    def _cache_path(self, key: str) -> Path:
        return self._cache_dir / f"{key}.json"

    def _read_cache(self, key: str) -> Optional[Any]:
        path = self._cache_path(key)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > self._cache_ttl:
            path.unlink(missing_ok=True)
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            path.unlink(missing_ok=True)
            return None

    def _write_cache(self, key: str, data: Any) -> None:
        try:
            self._cache_path(key).write_text(
                json.dumps(data, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning(f"[{self.source_name}] cache write failed: {exc}")

    async def _get(
        self,
        url: str,
        params: Optional[dict[str, Any]] = None,
        *,
        use_cache: bool = True,
        headers: Optional[dict[str, str]] = None,
    ) -> Any:
        """GET with rate-limiting, retry, and disk cache."""
        params = params or {}
        cache_key = self._cache_key(url, params)

        if use_cache:
            cached = self._read_cache(cache_key)
            if cached is not None:
                logger.debug(f"[{self.source_name}] cache hit {url}")
                return cached

        await self._bucket.acquire()

        retrying = AsyncRetrying(
            stop=stop_after_attempt(settings.http_max_retries + 1),
            wait=wait_exponential(
                min=settings.http_retry_wait_min,
                max=settings.http_retry_wait_max,
            ),
            retry=retry_if_exception_type((ConnectorHTTPError, ConnectorTimeoutError)),
            reraise=True,
        )

        async for attempt in retrying:
            with attempt:
                data = await self._do_get(url, params, headers)

        if use_cache:
            self._write_cache(cache_key, data)

        return data

    async def _do_get(
        self,
        url: str,
        params: dict[str, Any],
        extra_headers: Optional[dict[str, str]],
    ) -> Any:
        if self._client is None:
            raise RuntimeError("Connector not started — use async with connector:")

        req_headers: dict[str, str] = dict(extra_headers or {})

        try:
            logger.debug(f"[{self.source_name}] GET {url} params={params}")
            resp = await self._client.get(url, params=params, headers=req_headers)
        except httpx.TimeoutException as exc:
            raise ConnectorTimeoutError(
                f"Timeout fetching {url}", source=self.source_name, url=url
            ) from exc
        except httpx.RequestError as exc:
            raise ConnectorHTTPError(
                f"Network error: {exc}", source=self.source_name, url=url, status_code=0
            ) from exc

        self._handle_status(resp, url)

        content_type = resp.headers.get("content-type", "")
        if "json" in content_type:
            return resp.json()
        # Return text for CSV / XML sources
        return resp.text

    def _handle_status(self, resp: httpx.Response, url: str) -> None:
        if resp.is_success:
            return
        if resp.status_code in (401, 403):
            raise ConnectorAuthError(
                f"Auth failure {resp.status_code} for {url}",
                source=self.source_name,
                status_code=resp.status_code,
            )
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 60))
            raise ConnectorRateLimitError(
                f"Rate-limited by {self.source_name}",
                source=self.source_name,
                retry_after=retry_after,
            )
        raise ConnectorHTTPError(
            f"HTTP {resp.status_code} from {url}: {resp.text[:200]}",
            source=self.source_name,
            status_code=resp.status_code,
            url=url,
        )

    # ------------------------------------------------------------------ #
    # Public interface every connector must implement                      #
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def health_check(self) -> bool:
        """Return True if the data source is reachable and authenticated."""

    # ------------------------------------------------------------------ #
    # Convenience: run an async method in a sync context                  #
    # ------------------------------------------------------------------ #

    def run(self, coro: Any) -> Any:  # type: ignore[override]
        """
        Execute ``coro`` in a new event loop.  Useful for quick scripting and
        tests; prefer ``async with`` in production code.
        """
        return asyncio.run(coro)
