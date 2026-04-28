"""
Parity tests: verify lxml and html.parser produce identical output.
This is the key safety check before merging — if these pass, the
parser backend switch is safe.
"""
from bs4 import BeautifulSoup
from scrapers.html_parser import scrape_debtors_table, scrape_balances_table, parse_bcra_number

# ---------------------------------------------------------------------------
# Sample BCRA-style HTML fixtures
# ---------------------------------------------------------------------------

DEBTORS_HTML = """
<html><body>
<table>
  <tr>
    <th>Entidad</th><th>Ene-2024</th><th>Feb-2024</th><th>Mar-2024</th>
  </tr>
  <tr>
    <td>TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)</td>
    <td>1.000,50</td><td>2.000,75</td><td>3.000,25</td>
  </tr>
  <tr>
    <td>Monto</td>
    <td>100,00</td><td>200,00</td><td>300,00</td>
  </tr>
</table>
</body></html>
"""

BALANCES_HTML = """
<html><body>
<table>
  <tr>
    <th>Item</th><th>Dic-2023</th><th>Dic-2024</th><th>Dic-2025</th>
  </tr>
  <tr>
    <td>Activo Total</td>
    <td>10.000,00</td><td>12.000,00</td><td>14.000,00</td>
  </tr>
  <tr>
    <td>Pasivo Total</td>
    <td>8.000,00</td><td>9.500,00</td><td>11.000,00</td>
  </tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# parse_bcra_number
# ---------------------------------------------------------------------------

def test_parse_bcra_number_standard():
    assert parse_bcra_number("1.234,56") == pytest.approx(1234.56)


def test_parse_bcra_number_zero():
    assert parse_bcra_number("-") == 0.0


def test_parse_bcra_number_empty():
    assert parse_bcra_number("") == 0.0


def test_parse_bcra_number_whole():
    assert parse_bcra_number("1.000") == pytest.approx(1000.0)


import pytest


# ---------------------------------------------------------------------------
# Parity: lxml vs html.parser on debtors
# ---------------------------------------------------------------------------

def _debtors_with_parser(html: str, parser: str) -> list[dict]:
    """Temporarily patch BeautifulSoup to use a specific parser, run the scraper."""
    import scrapers.html_parser as hp
    original = hp.BeautifulSoup

    def patched(markup, *args, **kwargs):
        return original(markup, parser)

    hp.BeautifulSoup = patched
    try:
        return scrape_debtors_table(html, "00016", "Banco Test", "Deudores")
    finally:
        hp.BeautifulSoup = original


def _balances_with_parser(html: str, parser: str) -> list[dict]:
    import scrapers.html_parser as hp
    original = hp.BeautifulSoup

    def patched(markup, *args, **kwargs):
        return original(markup, parser)

    hp.BeautifulSoup = patched
    try:
        return scrape_balances_table(html, "00016", "Banco Test", "Balances")
    finally:
        hp.BeautifulSoup = original


def test_debtors_lxml_matches_html_parser():
    lxml_result = scrape_debtors_table(DEBTORS_HTML, "00016", "Banco Test", "Deudores")
    html_result = _debtors_with_parser(DEBTORS_HTML, "html.parser")
    assert len(lxml_result) == len(html_result), (
        f"Record count differs: lxml={len(lxml_result)}, html.parser={len(html_result)}"
    )
    for l, h in zip(lxml_result, html_result):
        assert l["indicador"] == h["indicador"]
        assert l["periodo"] == h["periodo"]
        assert l["valor"] == pytest.approx(h["valor"])


def test_balances_lxml_matches_html_parser():
    lxml_result = scrape_balances_table(BALANCES_HTML, "00016", "Banco Test", "Balances")
    html_result = _balances_with_parser(BALANCES_HTML, "html.parser")
    assert len(lxml_result) == len(html_result), (
        f"Record count differs: lxml={len(lxml_result)}, html.parser={len(html_result)}"
    )
    for l, h in zip(lxml_result, html_result):
        assert l["indicador"] == h["indicador"]
        assert l["periodo"] == h["periodo"]
        assert l["valor"] == pytest.approx(h["valor"])


# ---------------------------------------------------------------------------
# Debtors: functional correctness
# ---------------------------------------------------------------------------

def test_debtors_correct_record_count():
    records = scrape_debtors_table(DEBTORS_HTML, "00016", "Banco Test", "Deudores")
    # 2 rows × 3 periods = 6 records
    assert len(records) == 6


def test_debtors_section_detected():
    records = scrape_debtors_table(DEBTORS_HTML, "00016", "Banco Test", "Deudores")
    total_records = [r for r in records if "TOTAL" in r["seccion"].upper()]
    assert len(total_records) == 6  # seccion propagates to all rows under TOTAL portfolio (2 rows × 3 periods)


def test_debtors_values_parsed():
    records = scrape_debtors_table(DEBTORS_HTML, "00016", "Banco Test", "Deudores")
    monto_records = [r for r in records if r["indicador"] == "Monto"]
    values = {r["periodo"]: r["valor"] for r in monto_records}
    assert values["Ene-2024"] == pytest.approx(100.0)
    assert values["Feb-2024"] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Balances: functional correctness
# ---------------------------------------------------------------------------

def test_balances_correct_record_count():
    records = scrape_balances_table(BALANCES_HTML, "00016", "Banco Test", "Balances")
    # 2 rows × 3 periods = 6 records
    assert len(records) == 6


def test_balances_values_parsed():
    records = scrape_balances_table(BALANCES_HTML, "00016", "Banco Test", "Balances")
    activo = [r for r in records if r["indicador"] == "Activo Total"]
    assert len(activo) == 3
    by_period = {r["periodo"]: r["valor"] for r in activo}
    assert by_period["Dic-2024"] == pytest.approx(12000.0)
