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
    Obtiene el listado de entidades financieras con estrategia de tres niveles:
      1. Base de datos local (bcra_dashboard.db) — más rápido y confiable
      2. Scraping del sitio del BCRA                — si la DB no existe aún
      3. Lista mínima hardcodeada                   — último recurso para no abortar

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

    print("      [!] Scraping del BCRA no disponible. Usando lista oficial de entidades (Com. A 8367).")

    # --- Nivel 3: Lista oficial extraída de Com. "A" 8367 del BCRA (12/12/2025) ---
    # Fuente: Anexo listado de entidades financieras Grupo A y B.
    # Nota: los códigos de compañías financieras tienen 5 dígitos sin padding (ej: 44077).
    # Para el resto se usa padding de 5 dígitos con ceros (ej: 00007).
    ENTIDADES_CONOCIDAS = [
        # --- Grupo A ---
        {"codigo": "00007", "nombre": "BANCO DE GALICIA Y BUENOS AIRES S.A."},
        {"codigo": "00011", "nombre": "BANCO DE LA NACION ARGENTINA"},
        {"codigo": "00014", "nombre": "BANCO DE LA PROVINCIA DE BUENOS AIRES"},
        {"codigo": "00015", "nombre": "INDUSTRIAL AND COMMERCIAL BANK OF CHINA (ARGENTINA) S.A.U."},
        {"codigo": "00016", "nombre": "CITIBANK N.A."},
        {"codigo": "00017", "nombre": "BANCO BBVA ARGENTINA S.A."},
        {"codigo": "00020", "nombre": "BANCO DE LA PROVINCIA DE CORDOBA S.A."},
        {"codigo": "00027", "nombre": "BANCO SUPERVIELLE S.A."},
        {"codigo": "00029", "nombre": "BANCO DE LA CIUDAD DE BUENOS AIRES"},
        {"codigo": "00034", "nombre": "BANCO PATAGONIA S.A."},
        {"codigo": "00044", "nombre": "BANCO HIPOTECARIO S.A."},
        {"codigo": "00072", "nombre": "BANCO SANTANDER ARGENTINA S.A."},
        {"codigo": "00191", "nombre": "BANCO CREDICOOP COOPERATIVO LIMITADO"},
        {"codigo": "00285", "nombre": "BANCO MACRO S.A."},
        {"codigo": "00299", "nombre": "BANCO COMAFI S.A."},
        {"codigo": "00322", "nombre": "BANCO INDUSTRIAL S.A."},
        {"codigo": "00330", "nombre": "NUEVO BANCO DE SANTA FE S.A."},
        # --- Grupo B ---
        {"codigo": "00045", "nombre": "BANCO DE SAN JUAN S.A."},
        {"codigo": "00065", "nombre": "BANCO MUNICIPAL DE ROSARIO"},
        {"codigo": "00083", "nombre": "BANCO DEL CHUBUT S.A."},
        {"codigo": "00086", "nombre": "BANCO DE SANTA CRUZ S.A."},
        {"codigo": "00093", "nombre": "BANCO DE LA PAMPA S.A."},
        {"codigo": "00094", "nombre": "BANCO DE CORRIENTES S.A."},
        {"codigo": "00097", "nombre": "BANCO PROVINCIA DEL NEUQUÉN S.A."},
        {"codigo": "00131", "nombre": "BANK OF CHINA LIMITED, SUCURSAL BUENOS AIRES"},
        {"codigo": "00143", "nombre": "BRUBANK S.A.U."},
        {"codigo": "00147", "nombre": "BIBANK S.A."},
        {"codigo": "00165", "nombre": "JPMORGAN CHASE BANK, NATIONAL ASSOCIATION (SUCURSAL BUENOS AIRES)"},
        {"codigo": "00198", "nombre": "BANCO DE VALORES S.A."},
        {"codigo": "00247", "nombre": "BANCO ROELA S.A."},
        {"codigo": "00254", "nombre": "BANCO MARIVA S.A."},
        {"codigo": "00266", "nombre": "BNP PARIBAS"},
        {"codigo": "00268", "nombre": "BANCO PROVINCIA DE TIERRA DEL FUEGO"},
        {"codigo": "00269", "nombre": "BANCO DE LA REPUBLICA ORIENTAL DEL URUGUAY"},
        {"codigo": "00277", "nombre": "BANCO SAENZ S.A."},
        {"codigo": "00281", "nombre": "BANCO MERIDIAN S.A."},
        {"codigo": "00300", "nombre": "BANCO DE INVERSION Y COMERCIO EXTERIOR S.A."},
        {"codigo": "00301", "nombre": "BANCO PIANO S.A."},
        {"codigo": "00305", "nombre": "BANCO JULIO S.A."},
        {"codigo": "00309", "nombre": "BANCO RIOJA SOCIEDAD ANONIMA UNIPERSONAL"},
        {"codigo": "00310", "nombre": "BANCO DEL SOL S.A."},
        {"codigo": "00311", "nombre": "NUEVO BANCO DEL CHACO S.A."},
        {"codigo": "00312", "nombre": "BANCO VOII S.A."},
        {"codigo": "00315", "nombre": "BANCO DE FORMOSA S.A."},
        {"codigo": "00319", "nombre": "BANCO CMF S.A."},
        {"codigo": "00321", "nombre": "BANCO DE SANTIAGO DEL ESTERO S.A."},
        {"codigo": "00331", "nombre": "BANCO CETELEM ARGENTINA S.A."},
        {"codigo": "00332", "nombre": "BANCO DE SERVICIOS FINANCIEROS S.A."},
        {"codigo": "00338", "nombre": "BANCO DE SERVICIOS Y TRANSACCIONES S.A.U."},
        {"codigo": "00339", "nombre": "RCI BANQUE S.A."},
        {"codigo": "00340", "nombre": "BACS BANCO DE CREDITO Y SECURITIZACION S.A."},
        {"codigo": "00341", "nombre": "BANCO MASVENTAS S.A."},
        {"codigo": "00384", "nombre": "UALA BANK S.A.U."},
        {"codigo": "00386", "nombre": "NUEVO BANCO DE ENTRE RIOS S.A."},
        {"codigo": "00389", "nombre": "BANCO COLUMBIA S.A."},
        {"codigo": "00426", "nombre": "BANCO BICA S.A."},
        {"codigo": "00431", "nombre": "BANCO COINAG S.A."},
        {"codigo": "00432", "nombre": "BANCO DE COMERCIO S.A."},
        {"codigo": "00435", "nombre": "BANCO SUCREDITO REGIONAL S.A.U."},
        {"codigo": "00448", "nombre": "BANCO DINO S.A."},
        # --- Compañías Financieras (códigos de 5 dígitos sin padding) ---
        {"codigo": "44077", "nombre": "COMPAÑIA FINANCIERA ARGENTINA S.A."},
        {"codigo": "44088", "nombre": "VOLKSWAGEN FINANCIAL SERVICES COMPAÑIA FINANCIERA S.A."},
        {"codigo": "44092", "nombre": "FCA COMPAÑIA FINANCIERA S.A."},
        {"codigo": "44093", "nombre": "GPAT COMPAÑIA FINANCIERA S.A.U."},
        {"codigo": "44094", "nombre": "MERCEDES-BENZ COMPAÑIA FINANCIERA ARGENTINA S.A."},
        {"codigo": "44095", "nombre": "ROMBO COMPAÑIA FINANCIERA S.A."},
        {"codigo": "44096", "nombre": "JOHN DEERE CREDIT COMPAÑIA FINANCIERA S.A."},
        {"codigo": "44098", "nombre": "PSA FINANCE ARGENTINA COMPAÑIA FINANCIERA S.A."},
        {"codigo": "44099", "nombre": "TOYOTA COMPAÑIA FINANCIERA DE ARGENTINA S.A."},
        {"codigo": "45030", "nombre": "NARANJA DIGITAL COMPAÑIA FINANCIERA S.A.U."},
        {"codigo": "45056", "nombre": "MONTEMAR COMPAÑIA FINANCIERA S.A."},
        {"codigo": "45072", "nombre": "REBA COMPAÑIA FINANCIERA S.A."},
        {"codigo": "65203", "nombre": "CREDITO REGIONAL COMPAÑIA FINANCIERA S.A.U."},
    ]
    print(f"      [OK] Usando {len(ENTIDADES_CONOCIDAS)} entidades oficiales (Com. A 8367) como fallback.")
    return ENTIDADES_CONOCIDAS


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
