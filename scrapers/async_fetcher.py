import asyncio
import logging
import warnings

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_fixed,
)

logger = logging.getLogger(__name__)

# Suppress httpx/httpcore SSL warnings for BCRA's problematic certificates
warnings.filterwarnings("ignore", message=".*verify.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx")

MAX_CONCURRENT_REQUESTS: int = 6

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


def _is_retryable(exc: BaseException) -> bool:
    """Return True for network errors and 5xx; False for 4xx (no point retrying)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.TimeoutException, httpx.ConnectError, httpx.NetworkError))


class AsyncFetcher:
    """
    Async HTTP client wrapping httpx.AsyncClient with:
    - bounded concurrency (asyncio.Semaphore)
    - automatic retries via tenacity (network errors + 5xx only)
    - SSL verification disabled for BCRA
    """

    def __init__(
        self,
        max_concurrent: int = MAX_CONCURRENT_REQUESTS,
        timeout: float = 20.0,
    ) -> None:
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "AsyncFetcher":
        self._client = httpx.AsyncClient(
            verify=False,
            http2=True,
            headers=BASE_HEADERS,
            timeout=self._timeout,
            limits=httpx.Limits(
                max_connections=MAX_CONCURRENT_REQUESTS + 4,
                max_keepalive_connections=MAX_CONCURRENT_REQUESTS,
            ),
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get_text(self, url: str) -> str | None:
        """Fetch URL and return response body as text. Returns None on unrecoverable error."""
        try:
            return await self._fetch_text(url)
        except Exception as exc:
            logger.warning("[!] get_text failed for %s: %s", url, exc)
            return None

    async def get_json(self, url: str) -> dict | None:
        """Fetch URL and return parsed JSON. Returns None on unrecoverable error."""
        try:
            return await self._fetch_json(url)
        except Exception as exc:
            logger.warning("[!] get_json failed for %s: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # Internal retry-decorated methods
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_fixed(3),
        reraise=True,
    )
    async def _fetch_text(self, url: str) -> str:
        assert self._client is not None, "AsyncFetcher must be used as a context manager"
        async with self._semaphore:
            r = await self._client.get(url)
            r.raise_for_status()
            return r.text

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_fixed(3),
        reraise=True,
    )
    async def _fetch_json(self, url: str) -> dict:
        assert self._client is not None, "AsyncFetcher must be used as a context manager"
        async with self._semaphore:
            r = await self._client.get(url)
            r.raise_for_status()
            content_type = r.headers.get("content-type", "")
            if "html" in content_type.lower():
                raise ValueError(f"Expected JSON but got HTML from {url}")
            return r.json()
