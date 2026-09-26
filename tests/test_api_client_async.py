"""Unit tests for async extract_indicators."""
import httpx
import pytest
import respx

from scrapers.async_fetcher import AsyncFetcher
from scrapers.api_client import (
    extract_indicators,
    extract_eecc,
    extract_debtors,
    _parse_json_response,
    _parse_html_response,
    _parse_eecc_json,
    _parse_debtors_json,
)

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


# ---------------------------------------------------------------------------
# EECC (Balances) Tests
# ---------------------------------------------------------------------------

SAMPLE_EECC_JSON = {
    "logo_banco_url": "https://example.com/logo_eecc.png",
    "fechas": ["Dic-2024", "Dic-2025"],
    "filas": [
        {"nivel": 0, "titulo": "A C T I V O", "valores": [1000, 2000]},
        {"nivel": 0, "titulo": "P A S I V O", "valores": [600, 1200]},
        {"nivel": 0, "titulo": "P A T R I M O N I O   N E T O", "valores": [400, 800]},
    ],
}


def test_parse_eecc_json_extracts_records():
    records, logo_url = _parse_eecc_json(SAMPLE_EECC_JSON, bco_int=340, nombre="BACS")
    assert logo_url == "https://example.com/logo_eecc.png"
    assert len(records) == 6  # 3 indicators × 2 periods
    assert all(r["codigo_entidad"] == 340 for r in records)
    assert all(r["seccion"] == "Balances" for r in records)
    assert all(r["nombre"] == "BACS" for r in records)

    activos = [r for r in records if r["indicador"] == "A C T I V O"]
    assert len(activos) == 2
    assert {r["valor"] for r in activos} == {1000.0, 2000.0}


@respx.mock
@pytest.mark.asyncio
async def test_extract_eecc_uses_json_api():
    respx.get(url__regex=r".*entidades-financieras-estados-contables\.php.*category=00340").mock(
        return_value=httpx.Response(200, json=SAMPLE_EECC_JSON)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo = await extract_eecc(fetcher, "00340", "BACS")

    assert len(records) == 6
    assert logo == "https://example.com/logo_eecc.png"


@respx.mock
@pytest.mark.asyncio
async def test_extract_eecc_handles_failure():
    respx.get(url__regex=r".*entidades-financieras-estados-contables\.php.*category=00340").mock(
        return_value=httpx.Response(500)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo = await extract_eecc(fetcher, "00340", "BACS")

    assert records == []
    assert logo is None


# ---------------------------------------------------------------------------
# Deudores Tests
# ---------------------------------------------------------------------------

SAMPLE_DEUDORES_JSON = {
    "logo_banco_url": "https://example.com/logo_deud.png",
    "columnas": ["Dic-2024", "Dic-2025"],
    "filas": [
        {"titulo": "TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)", "valores": [100.0, 200.0]},
        {"titulo": "TF.Sit.1: En situación normal (%)", "valores": [98.5, 99.0]},
        {"titulo": "CARTERA COMERCIAL ($)", "valores": [50.0, 100.0]},
        {"titulo": "C.COM.Sit.1: En situación normal (%)", "valores": [97.0, 98.0]},
    ],
}


def test_parse_debtors_json_extracts_records():
    records, logo_url = _parse_debtors_json(SAMPLE_DEUDORES_JSON, bco_int=340, nombre="BACS")
    assert logo_url == "https://example.com/logo_deud.png"
    assert len(records) == 8  # 4 rows × 2 periods
    assert all(r["codigo_entidad"] == 340 for r in records)
    assert all(r["nombre"] == "BACS" for r in records)

    # Check portfolio tracking
    tf_records = [r for r in records if "TF.Sit." in r["indicador"]]
    assert len(tf_records) == 2
    assert all(r["seccion"] == "TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)" for r in tf_records)

    com_records = [r for r in records if "C.COM.Sit." in r["indicador"]]
    assert len(com_records) == 2
    assert all(r["seccion"] == "CARTERA COMERCIAL ($)" for r in com_records)


@respx.mock
@pytest.mark.asyncio
async def test_extract_debtors_uses_json_api():
    respx.get(url__regex=r".*entidades-financieras-situacion-deudores\.php.*category=00340").mock(
        return_value=httpx.Response(200, json=SAMPLE_DEUDORES_JSON)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo = await extract_debtors(fetcher, "00340", "BACS")

    assert len(records) == 8
    assert logo == "https://example.com/logo_deud.png"


@respx.mock
@pytest.mark.asyncio
async def test_extract_debtors_handles_failure():
    respx.get(url__regex=r".*entidades-financieras-situacion-deudores\.php.*category=00340").mock(
        return_value=httpx.Response(500)
    )
    async with AsyncFetcher(max_concurrent=2) as fetcher:
        records, logo = await extract_debtors(fetcher, "00340", "BACS")

    assert records == []
    assert logo is None

