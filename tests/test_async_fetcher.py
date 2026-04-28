"""Unit tests for AsyncFetcher — mocked with respx."""
import asyncio

import httpx
import pytest
import respx

from scrapers.async_fetcher import AsyncFetcher, MAX_CONCURRENT_REQUESTS


@pytest.fixture
def fetcher():
    return AsyncFetcher(max_concurrent=3, timeout=5.0)


# ---------------------------------------------------------------------------
# Successful responses
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_get_text_success(fetcher):
    respx.get("https://example.com/page").mock(
        return_value=httpx.Response(200, text="<html>hello</html>")
    )
    async with fetcher:
        result = await fetcher.get_text("https://example.com/page")
    assert result == "<html>hello</html>"


@respx.mock
@pytest.mark.asyncio
async def test_get_json_success(fetcher):
    respx.get("https://example.com/api").mock(
        return_value=httpx.Response(
            200,
            json={"key": "value"},
            headers={"content-type": "application/json"},
        )
    )
    async with fetcher:
        result = await fetcher.get_json("https://example.com/api")
    assert result == {"key": "value"}


# ---------------------------------------------------------------------------
# 4xx — should NOT be retried, returns None
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_get_text_404_returns_none(fetcher):
    respx.get("https://example.com/missing").mock(
        return_value=httpx.Response(404, text="Not Found")
    )
    async with fetcher:
        result = await fetcher.get_text("https://example.com/missing")
    assert result is None
    # Only one attempt (no retries for 4xx)
    assert respx.calls.call_count == 1


@respx.mock
@pytest.mark.asyncio
async def test_get_json_html_content_type_returns_none(fetcher):
    """API returns HTML instead of JSON — should fail gracefully."""
    respx.get("https://example.com/api").mock(
        return_value=httpx.Response(
            200,
            text="<html>error page</html>",
            headers={"content-type": "text/html"},
        )
    )
    async with fetcher:
        result = await fetcher.get_json("https://example.com/api")
    assert result is None


# ---------------------------------------------------------------------------
# 5xx — should retry 3 times then return None
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_get_text_500_retries_then_returns_none(fetcher):
    respx.get("https://example.com/broken").mock(
        return_value=httpx.Response(500, text="Server Error")
    )
    async with fetcher:
        result = await fetcher.get_text("https://example.com/broken")
    assert result is None
    assert respx.calls.call_count == 3  # 3 attempts total


# ---------------------------------------------------------------------------
# Semaphore cap
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_semaphore_limits_concurrency():
    """Verify that no more than max_concurrent requests run simultaneously."""
    max_concurrent = 3
    in_flight = 0
    peak = 0

    async def slow_handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await asyncio.sleep(0.05)
        in_flight -= 1
        return httpx.Response(200, text="ok")

    with respx.mock:
        respx.get(url__regex=r"https://example\.com/\d+").mock(side_effect=slow_handler)

        f = AsyncFetcher(max_concurrent=max_concurrent, timeout=5.0)
        async with f:
            tasks = [f.get_text(f"https://example.com/{i}") for i in range(9)]
            await asyncio.gather(*tasks)

    assert peak <= max_concurrent


# ---------------------------------------------------------------------------
# Context manager enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_text_without_context_manager_raises():
    f = AsyncFetcher()
    with pytest.raises(AssertionError):
        await f._fetch_text("https://example.com")
