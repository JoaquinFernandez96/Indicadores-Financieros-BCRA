"""Unit tests for async extract_indicators."""
import httpx
import pytest
import respx

from scrapers.async_fetcher import AsyncFetcher
from scrapers.api_client import extract_indicators, _parse_json_response, _parse_html_response

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_JSON = {
    "logo_url": "https://example.com/logo.png",
    "columnas": {"col1": "Dic-2024", "col2": "Dic-2023"},
    "secciones": {
        "capital": [
            {"in_titulo": "RPC / APR", "in_c1": "21.5", "in_c2": "19.8"},
        ],
        "activos": [
            {"in_titulo": "Incobrabilidad", "in_c1": "3.2", "in_c2": "2.9"},
        ],
        "eficiencia": [],
        "rentabilidad": [],
        "liquidez": [],
    },
}

SAMPLE_HTML = """
<html><body>
<table>
  <tr><td>Banco Test</td><td>Ene-2024</td><td>Feb-2024</td><td>Mar-2024</td></tr>
  <tr><td>Rentabilidad</td><td>1.234</td><td>2.345</td><td>3.456</td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Pure parser tests (no network)
# ---------------------------------------------------------------------------

def test_parse_json_response_extracts_records():
    records, logo_url = _parse_json_response(SAMPLE_JSON, bco_int=16)
    assert logo_url == "https://example.com/logo.png"
    assert len(records) == 4  # 2 indicators × 2 periods
    indicadores = {r["indicador"] for r in records}
    assert "RPC / APR" in indicadores
    assert "Incobrabilidad" in indicadores
    periodos = {r["periodo"] for r in records}
    assert "Dic-2024" in periodos
    assert "Dic-2023" in periodos
    assert all(r["codigo_entidad"] == 16 for r in records)


def test_parse_json_response_logo_slash_unescaped():
    data = {**SAMPLE_JSON, "logo_url": "https:\\/\\/example.com\\/logo.png"}
    _, logo_url = _parse_json_response(data, bco_int=1)
    assert logo_url == "https://example.com/logo.png"


def test_parse_json_response_invalid_valor_skipped():
    data = {
        "logo_url": None,
        "columnas": {"col1": "Dic-2024"},
        "secciones": {
            "capital": [{"in_titulo": "Indicator", "in_c1": "not_a_number"}],
            "activos": [], "eficiencia": [], "rentabilidad": [], "liquidez": [],
        },
    }
    records, _ = _parse_json_response(data, bco_int=1)
    assert records == []


def test_parse_html_response_extracts_records():
    records, logo_url = _parse_html_response(SAMPLE_HTML, bco_int=99)
    assert logo_url is None
    assert len(records) == 3  # 3 period columns
    assert all(r["indicador"] == "Rentabilidad" for r in records)
    assert all(r["codigo_entidad"] == 99 for r in records)


# ---------------------------------------------------------------------------
# Integration: extract_indicators uses fetcher
# ---------------------------------------------------------------------------

@respx.mock
@pytest.mark.asyncio
async def test_extract_indicators_uses_json_api():
    respx.get(url__regex=r".*action=indicadores.*bco=00016").mock(
        return_value=httpx.Response(
            200,
            json=SAMPLE_JSON,
            headers={"content-type": "application/json"},
        )
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo_url = await extract_indicators(fetcher, "00016", "Banco Test")

    assert len(records) == 4
    assert logo_url == "https://example.com/logo.png"


@respx.mock
@pytest.mark.asyncio
async def test_extract_indicators_falls_back_to_html():
    """When JSON API returns HTML (error page), fallback HTML page is used."""
    respx.get(url__regex=r".*action=indicadores.*bco=00016").mock(
        return_value=httpx.Response(
            200,
            text="<html>error</html>",
            headers={"content-type": "text/html"},
        )
    )
    respx.get(url__regex=r".*entidades-financieras-indicadores.*bco=00016").mock(
        return_value=httpx.Response(200, text=SAMPLE_HTML)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, _ = await extract_indicators(fetcher, "00016", "Banco Test")

    assert len(records) == 3


@respx.mock
@pytest.mark.asyncio
async def test_extract_indicators_returns_empty_on_total_failure():
    """Both JSON and HTML fail → ([], None)."""
    respx.get(url__regex=r".*action=indicadores.*bco=00016").mock(
        return_value=httpx.Response(500)
    )
    respx.get(url__regex=r".*entidades-financieras-indicadores.*bco=00016").mock(
        return_value=httpx.Response(500)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo_url = await extract_indicators(fetcher, "00016", "Banco Test")

    assert records == []
    assert logo_url is None
