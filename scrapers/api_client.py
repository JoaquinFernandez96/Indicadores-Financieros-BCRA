import logging
import os
import sqlite3

from bs4 import BeautifulSoup

from scrapers.async_fetcher import AsyncFetcher

logger = logging.getLogger(__name__)

DB_PATH = "bcra_dashboard.db"

# Mapeo de secciones JSON → nombre interno de sección
_SECTION_MAP = {
    "capital":      "Indicadores",
    "activos":      "Indicadores",
    "eficiencia":   "Indicadores",
    "rentabilidad": "Indicadores",
    "liquidez":     "Indicadores",
}


# ---------------------------------------------------------------------------
# Entity loading (sync — runs once at startup)
# ---------------------------------------------------------------------------

def _get_entities_from_db() -> list[dict]:
    """Read entity list from local SQLite. Returns [] if DB missing or empty."""
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT codigo_entidad, nombre FROM entities ORDER BY codigo_entidad"
        )
        rows = cursor.fetchall()
        conn.close()
        if rows:
            return [
                {"codigo": str(r[0]).zfill(5), "nombre": r[1]}
                for r in rows
                if r[1]
            ]
    except Exception as exc:
        logger.warning("[!] Error leyendo entidades desde DB: %s", exc)
    return []


def _get_entities_from_bcra() -> list[dict]:
    """
    Sync fallback: scrape the entity list from BCRA HTML pages.
    Only called when the local DB is empty (first run).
    """
    import requests  # kept for this one-shot sync call only
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    urls = [
        "https://www.bcra.gob.ar/entidades-financieras-situacion-deudores/",
        "https://www.bcra.gob.ar/entidades-financieras-estados-contables/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=15, verify=False)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            select = (
                soup.find("select", {"id": "bco"})
                or soup.find("select", {"name": "bco"})
                or soup.find("select")
            )
            if not select:
                continue
            entities = []
            for opt in select.find_all("option"):
                val = opt.get("value", "").strip()
                nombre = opt.get_text(strip=True)
                if val and val.lstrip("0").isdigit() and nombre:
                    entities.append({"codigo": val.zfill(5), "nombre": nombre})
            if entities:
                logger.info("[OK] %d entidades cargadas desde BCRA (%s)", len(entities), url)
                return entities
        except Exception as exc:
            logger.warning("[!] Fallo BCRA %s: %s", url, exc)
    return []


def get_entities() -> list[dict]:
    """
    Two-level entity resolution:
      1. Local DB entities table (authoritative)
      2. BCRA web scraping (first-run fallback)
    """
    entities = _get_entities_from_db()
    if entities:
        logger.info("[OK] %d entidades cargadas desde base de datos local.", len(entities))
        return entities

    logger.warning("[!] DB local vacía. Intentando scraping del BCRA...")
    entities = _get_entities_from_bcra()
    if entities:
        return entities

    logger.error(
        "[!] No se pudo obtener el listado de entidades. "
        "Poblar la tabla entities antes de ejecutar el scraper."
    )
    return []


# ---------------------------------------------------------------------------
# Indicator extraction (async — called concurrently per entity)
# ---------------------------------------------------------------------------

async def extract_indicators(
    fetcher: AsyncFetcher,
    bco: str,
    nombre: str,
) -> tuple[list[dict], str | None]:
    """
    Extract financial indicators for one entity.
    Tries JSON API first; falls back to HTML page if the API fails.

    Returns: (records, logo_url)
    Records match the schema expected by DatabaseManager.save_observations().
    """
    url_api = (
        f"https://www.bcra.gob.ar/api-indicadores-economicos.php"
        f"?action=indicadores&bco={bco}"
    )
    try:
        bco_int = int(bco)
    except (ValueError, TypeError):
        bco_int = 0

    # ------------------------------------------------------------------
    # Attempt 1: JSON API
    # ------------------------------------------------------------------
    data = await fetcher.get_json(url_api)
    if data is not None:
        try:
            records, logo_url = _parse_json_response(data, bco_int)
            return records, logo_url
        except Exception as exc:
            logger.warning("[!] Error parseando JSON para %s (%s): %s", bco, nombre, exc)

    logger.info("[!] API JSON falló para %s (%s). Intentando fallback HTML...", bco, nombre)

    # ------------------------------------------------------------------
    # Attempt 2: HTML fallback
    # ------------------------------------------------------------------
    url_html = f"https://www.bcra.gob.ar/entidades-financieras-indicadores/?bco={bco}"
    html = await fetcher.get_text(url_html)
    if html is None:
        logger.warning("[!] Fallback HTML también falló para %s (%s).", bco, nombre)
        return [], None

    try:
        return _parse_html_response(html, bco_int)
    except Exception as exc:
        logger.warning("[!] Error parseando HTML para %s (%s): %s", bco, nombre, exc)
        return [], None


# ---------------------------------------------------------------------------
# Private parsing helpers (pure, synchronous)
# ---------------------------------------------------------------------------

def _parse_json_response(data: dict, bco_int: int) -> tuple[list[dict], str | None]:
    logo_url = data.get("logo_url")
    if logo_url and isinstance(logo_url, str):
        logo_url = logo_url.replace("\\/", "/")
    else:
        logo_url = None

    columnas: dict = data.get("columnas", {})
    secciones: dict = data.get("secciones", {})

    records: list[dict] = []
    for section_key, seccion_nombre in _SECTION_MAP.items():
        items = secciones.get(section_key)
        if not items or not isinstance(items, list):
            continue

        for item in items:
            indicador = item.get("in_titulo")
            if not indicador:
                continue

            periodo_keys = {
                k: v
                for k, v in item.items()
                if k.startswith("in_c") and k != "in_titulo"
            }

            if periodo_keys:
                for ck, valor in periodo_keys.items():
                    col_num = ck.replace("in_c", "")
                    periodo_label = columnas.get(f"col{col_num}") or "Actual"
                    if valor is None:
                        continue
                    try:
                        valor_float = float(str(valor).replace(",", "."))
                    except (ValueError, TypeError):
                        continue
                    records.append({
                        "codigo_entidad": bco_int,
                        "seccion":        seccion_nombre,
                        "periodo":        str(periodo_label).strip(),
                        "indicador":      str(indicador).strip(),
                        "valor":          valor_float,
                    })
            else:
                valor = item.get("in_c1")
                if valor is None:
                    continue
                try:
                    valor_float = float(str(valor).replace(",", "."))
                except (ValueError, TypeError):
                    continue
                records.append({
                    "codigo_entidad": bco_int,
                    "seccion":        seccion_nombre,
                    "periodo":        columnas.get("col1") or "Actual",
                    "indicador":      str(indicador).strip(),
                    "valor":          valor_float,
                })

    return records, logo_url


def _parse_html_response(html: str, bco_int: int) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(html, "lxml")

    logo_url: str | None = None
    img_logo = soup.find("img", {"class": lambda c: c and "logo" in c.lower()})
    if img_logo and img_logo.get("src"):
        src = img_logo["src"]
        logo_url = src if src.startswith("http") else f"https://www.bcra.gob.ar{src}"

    records: list[dict] = []
    current_periods: list[str] = []

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all(["td", "th"])
            texts = [td.get_text(strip=True) for td in tds]
            if not texts:
                continue

            periods_found = [
                t for t in texts
                if "-" in t and any(
                    m in t for m in
                    ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                     "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
                )
            ]
            if len(periods_found) >= 2:
                current_periods = periods_found
                continue

            if not current_periods:
                continue

            indicador = texts[0]
            if not indicador or len(indicador) < 3:
                continue

            vals = texts[-len(current_periods):]
            for periodo, val_str in zip(current_periods, vals):
                if not val_str or val_str == "-":
                    continue
                try:
                    valor_float = float(val_str.replace(".", "").replace(",", "."))
                except ValueError:
                    continue
                records.append({
                    "codigo_entidad": bco_int,
                    "seccion":        "Indicadores",
                    "periodo":        periodo,
                    "indicador":      indicador,
                    "valor":          valor_float,
                })

    return records, logo_url
