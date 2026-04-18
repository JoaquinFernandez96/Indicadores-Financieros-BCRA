import requests
import pandas as pd
import time
import os
from database_manager import DatabaseManager
from scrapers.api_client import get_entities, extract_indicators
from scrapers.html_parser import scrape_debtors_table, scrape_balances_table

# Configuración y URLs
EECC_URL = "https://www.bcra.gob.ar/entidades-financieras-estados-contables/?bco={bco}"
DEUDORES_URL = "https://www.bcra.gob.ar/entidades-financieras-situacion-deudores/?bco={bco}"

# Límite para pruebas (0 para todas las entidades)
TEST_MODE_LIMIT = 0 

def fetch_html_content(url):
    """Auxiliar para descargas HTML con reintentos mínimos."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"      [!] Error de red en {url}: {e}")
        return None

def main():
    db = DatabaseManager()
    print("\n" + "="*60)
    print("  SCRAPER BCRA MODULAR — INDICADORES + EECC + DEUDORES")
    print("="*60)
    
    entities = get_entities()
    if not entities: 
        print("  [!] No se pudieron cargar las entidades. Abortando.")
        return
        
    print(f"  [1/3] Entidades encontradas: {len(entities)}")

    # Scrappear indicadores del Sistema Total (bco=AAA00) — se guarda con codigo_entidad=0
    print("\n  [1.5/3] Scrapeando indicadores del Sistema Total (AAA00)...")
    recs_sistema, _ = extract_indicators('AAA00', 'Sistema Total')
    if recs_sistema:
        df_sistema = pd.DataFrame(recs_sistema)
        df_sistema['fuente'] = 'indicadores_sistema'
        db.save_observations(df_sistema)
        print(f"         ( {len(recs_sistema)} indicadores del sistema guardados )")
    else:
        print("         [!] No se pudieron obtener indicadores del Sistema Total.")

    limit_str = f"LIMITADO A {TEST_MODE_LIMIT}" if TEST_MODE_LIMIT else "TODAS"
    print(f"\n  [2/3] Extrayendo datos ({limit_str})...")
    
    LOGO_DIR = "logos"
    if not os.path.exists(LOGO_DIR): os.makedirs(LOGO_DIR)
    
    target_entities = entities[:TEST_MODE_LIMIT] if TEST_MODE_LIMIT else entities
    
    for i, entity in enumerate(target_entities, 1):
        bco, nombre = entity["codigo"], entity["nombre"]
        print(f"  [{i:02d}/{len(target_entities):02d}] {nombre}")
        
        # 1. Indicadores Financieros (Vía API/Table Parser)
        recs_ind, logo = extract_indicators(bco, nombre)
        if recs_ind:
            df_ind = pd.DataFrame(recs_ind)
            df_ind['fuente'] = 'indicadores'
            db.save_observations(df_ind)
        
        # 2. Estados Contables (Vía HTML Scraper con Balance Logic)
        html_eecc = fetch_html_content(EECC_URL.format(bco=bco))
        if html_eecc:
            recs_eecc = scrape_balances_table(html_eecc, bco, nombre, "Balances")
            if recs_eecc:
                df_eecc = pd.DataFrame(recs_eecc)
                df_eecc['fuente'] = 'eecc'
                db.save_observations(df_eecc)
            
        # 3. Situación de Deudores (Vía HTML Scraper con Portfolio Logic)
        html_deud = fetch_html_content(DEUDORES_URL.format(bco=bco))
        if html_deud:
            recs_deud = scrape_debtors_table(html_deud, bco, nombre, "Deudores")
            if recs_deud:
                df_deud = pd.DataFrame(recs_deud)
                df_deud['fuente'] = 'deudores'
                db.save_observations(df_deud)
        
        print(f"         ( {len(recs_ind)} ind | {len(recs_eecc) if html_eecc else 0} eecc | {len(recs_deud) if html_deud else 0} deud )")
        
        # Guardar metadatos de la entidad (con logo)
        db.save_entities(pd.DataFrame([{
            'codigo_entidad': int(bco), 
            'nombre': nombre, 
            'logo_url': logo if logo else None,
            'es_cliente': 0 # Defaults
        }]))
        
        time.sleep(0.5) # Respeto al servidor
    
    print("\n  [3/3] Proceso completado en base de datos.")
    print("\n" + "=" * 60)
    print("  PROCESO DE SCRAPPING FINALIZADO")
    print("=" * 60)

if __name__ == "__main__":
    main()
