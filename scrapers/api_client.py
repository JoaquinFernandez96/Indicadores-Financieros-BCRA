import requests
import urllib3
from bs4 import BeautifulSoup
import sqlite3
import os
import time

# Suprimir warnings de SSL (el BCRA tiene certificados problemáticos)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

MAX_RETRIES = 3
RETRY_DELAY = 3  # segundos entre reintentos

# Ruta de la DB (relativa al directorio de ejecución, igual que database_manager.py)
DB_PATH = "bcra_dashboard.db"


def _get_with_retries(url, timeout=15, verify=False):
    """
    Wrapper de requests.get con reintentos automáticos.
    Reintenta ante timeout, error de conexión o status 5xx.
    Lanza la excepción final si todos los intentos fallan.
    """
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=BASE_HEADERS, timeout=timeout, verify=verify)
            r.raise_for_status()
            return r
        except requests.exceptions.Timeout as e:
            last_exc = e
            print(f"      [!] Timeout en intento {attempt}/{MAX_RETRIES}: {url}")
        except requests.exceptions.ConnectionError as e:
            last_exc = e
            print(f"      [!] Error de conexión en intento {attempt}/{MAX_RETRIES}: {url}")
        except requests.exceptions.HTTPError as e:
            # Para 4xx no tiene sentido reintentar
            if r.status_code < 500:
                raise
            last_exc = e
            print(f"      [!] HTTP {r.status_code} en intento {attempt}/{MAX_RETRIES}: {url}")
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    raise last_exc


def _get_entities_from_db():
    """
    Lee el listado de entidades desde la base de datos local.
    Retorna lista de dicts {codigo, nombre} o [] si la DB no existe o está vacía.
    """
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute("SELECT codigo_entidad, nombre FROM entities ORDER BY codigo_entidad")
        rows = cursor.fetchall()
        conn.close()
        if rows:
            # El código se guarda como int; lo formateamos como string de 5 dígitos
            # igual que el formato original del BCRA (ej: "00016")
            return [{"codigo": str(r[0]).zfill(5), "nombre": r[1]} for r in rows if r[1]]
    except Exception as e:
        print(f"      [!] Error leyendo entidades desde DB: {e}")
    return []


def _get_entities_from_bcra():
    """
    Intenta obtener entidades desde el BCRA via HTML.
    El BCRA cambió su estructura: ya no hay un <select> en la página base,
    por lo que buscamos el listado en cualquier select disponible.
    """
    urls_a_probar = [
        "https://www.bcra.gob.ar/entidades-financieras-situacion-deudores/",
        "https://www.bcra.gob.ar/entidades-financieras-estados-contables/",
    ]
    for url in urls_a_probar:
        try:
            r = _get_with_retries(url, timeout=15)
            soup = BeautifulSoup(r.text, 'html.parser')

            select = (soup.find('select', {'id': 'bco'}) or
                      soup.find('select', {'name': 'bco'}) or
                      soup.find('select'))  # cualquier select como último recurso

            if not select:
                continue

            entities = []
            for opt in select.find_all('option'):
                val = opt.get('value', '').strip()
                nombre = opt.get_text(strip=True)
                if val and val.lstrip('0').isdigit() and nombre:
                    entities.append({"codigo": val.zfill(5), "nombre": nombre})

            if entities:
                print(f"      [OK] {len(entities)} entidades cargadas desde BCRA ({url})")
                return entities

        except Exception as e:
            print(f"      [!] Fallo BCRA {url}: {e}")
            continue

    return []


def get_entities():
    """
    Obtiene el listado de entidades financieras con estrategia de dos niveles:
      1. Tabla entities de la base de datos local — fuente autoritativa
      2. Scraping del sitio del BCRA              — si la DB no existe aún

    El BCRA cambió su estructura web y ya no expone un <select> en la página base;
    las URLs ahora usan parámetros ?bco=XXXXX&nom=NOMBRE directamente.
    """
    # --- Nivel 1: DB local ---
    entities = _get_entities_from_db()
    if entities:
        print(f"      [OK] {len(entities)} entidades cargadas desde base de datos local.")
        return entities

    print("      [!] DB local vacía o inexistente. Intentando scraping del BCRA...")

    # --- Nivel 2: BCRA web ---
    entities = _get_entities_from_bcra()
    if entities:
        return entities

    print("      [!] No se pudo obtener el listado de entidades. Poblar la tabla entities antes de ejecutar el scraper.")
    return []


def extract_indicators(bco, nombre):
    """
    Extrae indicadores financieros para una entidad.
    Intenta primero la API JSON; si falla, hace fallback al HTML de la página.
    
    Retorna: (lista de records, logo_url)
    Los records tienen las mismas claves que espera save_observations():
      codigo_entidad, seccion, periodo, indicador, valor
    """
    url_api = f"https://www.bcra.gob.ar/api-indicadores-economicos.php?action=indicadores&bco={bco}"

    try:
        bco_int = int(bco)
    except (ValueError, TypeError):
        bco_int = 0  # Códigos especiales como 'AAA00'

    # ----------------------------------------------------------------
    # Intento 1: API JSON
    # ----------------------------------------------------------------
    try:
        r = _get_with_retries(url_api, timeout=15, verify=False)

        # Validar que la respuesta sea JSON válido (a veces el BCRA devuelve HTML de error)
        content_type = r.headers.get("Content-Type", "")
        if "html" in content_type.lower():
            raise ValueError(f"La API devolvió HTML en lugar de JSON (posible página de error)")

        data = r.json()

        logo_url = data.get('logo_url')
        if logo_url and isinstance(logo_url, str):
            logo_url = logo_url.replace('\\/', '/')
        else:
            logo_url = None

        # Mapeo de secciones JSON → nombre interno de sección
        SECTION_MAP = {
            'capital':      'Indicadores',
            'activos':      'Indicadores',
            'eficiencia':   'Indicadores',
            'rentabilidad': 'Indicadores',
            'liquidez':     'Indicadores',
        }

        # Periodos están en data['columnas']: {'col1': 'Dic-2023', 'col2': 'Dic-2024', ...}
        columnas = data.get('columnas', {})
        secciones = data.get('secciones', {})

        records = []
        for section_key, seccion_nombre in SECTION_MAP.items():
            items = secciones.get(section_key)
            if not items or not isinstance(items, list):
                continue

            for item in items:
                indicador = item.get('in_titulo')
                if not indicador:
                    continue

                # La API devuelve múltiples periodos: in_c1 (más reciente), in_c2, in_c3...
                # Los labels están en data['columnas']['col1'], ['col2'], etc.
                periodo_keys = {k: v for k, v in item.items() if k.startswith('in_c') and k != 'in_titulo'}

                if periodo_keys:
                    for ck, valor in periodo_keys.items():
                        col_num = ck.replace('in_c', '')
                        periodo_label = columnas.get(f'col{col_num}') or 'Actual'

                        if valor is None:
                            continue
                        try:
                            valor_float = float(str(valor).replace(',', '.'))
                        except (ValueError, TypeError):
                            continue

                        records.append({
                            'codigo_entidad': bco_int,
                            'seccion':        seccion_nombre,
                            'periodo':        str(periodo_label).strip(),
                            'indicador':      str(indicador).strip(),
                            'valor':          valor_float,
                        })
                else:
                    # Fallback: solo in_c1
                    valor = item.get('in_c1')
                    if valor is None:
                        continue
                    try:
                        valor_float = float(str(valor).replace(',', '.'))
                    except (ValueError, TypeError):
                        continue
                    records.append({
                        'codigo_entidad': bco_int,
                        'seccion':        seccion_nombre,
                        'periodo':        columnas.get('col1') or 'Actual',
                        'indicador':      str(indicador).strip(),
                        'valor':          valor_float,
                    })

        return records, logo_url

    except Exception as e:
        print(f"         [!] API JSON falló para {bco} ({nombre}): {e}. Intentando fallback HTML...")

    # ----------------------------------------------------------------
    # Intento 2: Fallback HTML (página de indicadores del BCRA)
    # ----------------------------------------------------------------
    url_html = f"https://www.bcra.gob.ar/entidades-financieras-indicadores/?bco={bco}"
    try:
        r = _get_with_retries(url_html, timeout=20)
        soup = BeautifulSoup(r.text, 'html.parser')

        # Intentar extraer logo desde el HTML
        logo_url = None
        img_logo = soup.find('img', {'class': lambda c: c and 'logo' in c.lower()})
        if img_logo and img_logo.get('src'):
            src = img_logo['src']
            logo_url = src if src.startswith('http') else f"https://www.bcra.gob.ar{src}"

        # Buscar periodos en la cabecera de la tabla
        records = []
        tables = soup.find_all('table')
        current_periods = []

        for table in tables:
            for tr in table.find_all('tr'):
                tds = tr.find_all(['td', 'th'])
                texts = [td.get_text(strip=True) for td in tds]
                if not texts:
                    continue

                # Detectar fila de periodos
                periods_found = [
                    t for t in texts
                    if '-' in t and any(m in t for m in
                       ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'])
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
                    if not val_str or val_str == '-':
                        continue
                    try:
                        valor_float = float(val_str.replace('.', '').replace(',', '.'))
                    except ValueError:
                        continue
                    records.append({
                        'codigo_entidad': bco_int,
                        'seccion':        'Indicadores',
                        'periodo':        periodo,
                        'indicador':      indicador,
                        'valor':          valor_float,
                    })

        return records, logo_url

    except Exception as e:
        print(f"         [!] Fallback HTML también falló para {bco} ({nombre}): {e}")
        return [], None
