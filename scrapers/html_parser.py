import pandas as pd
from bs4 import BeautifulSoup
import re

def parse_bcra_number(text):
    """Limpia y convierte formato BCRA '9.999,99' a float."""
    if not text or text == "-": return 0.0
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except:
        return 0.0

def scrape_debtors_table(html, bco, nombre, seccion_defecto):
    """
    Parseador robusto para la Situación de Deudores del BCRA.
    Detecta las 4 carteras principales y mapea periodos y valores correctamente.
    """
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    
    # Buscamos todas las tablas relevantes.
    # A veces hay una tabla gigante, a veces varias.
    tables = soup.find_all('table')
    
    current_section = seccion_defecto
    current_periods = []
    
    # Carteras oficiales
    OFFICIAL_PORTFOLIOS = [
        "TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)",
        "CARTERA COMERCIAL ($)",
        "CARTERA DE CONSUMO O VIVIENDA ($)",
        "CARTERA COMERCIAL ASIMILABLE A CONSUMO ($)"
    ]
    
    for table in tables:
        rows = table.find_all('tr')
        for tr in rows:
            tds = tr.find_all(['td', 'th'])
            if not tds: continue

            texts = [td.get_text(strip=True) for td in tds]

            # 1. Detectar cabecera de periodos
            # Solo es cabecera si casi todas las celdas son fechas (estructura: [banco, f1, f2, ...])
            # Esto evita que filas de la estructura "staircase" del BCRA sean detectadas como cabeceras.
            periods_found = [t for t in texts if ("-" in t and any(m in t for m in ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]))]
            if len(periods_found) >= 3 and len(texts) <= len(periods_found) + 1:
                current_periods = periods_found
                continue

            if not current_periods: continue
            num_periods = len(current_periods)

            # 2. Detectar Indicador y potencial Cambio de Sección
            indicador = texts[0]

            # Limpieza básica de ruido (comienza con [, es el nombre del banco, o es la fecha)
            if not indicador or indicador == nombre or indicador in current_periods or indicador.startswith("["):
                continue

            # Si el indicador es una de las 4 carteras oficiales, reseteamos la sección
            # (Usamos in para mayor flexibilidad si hay espacios o variaciones)
            for portfolio in OFFICIAL_PORTFOLIOS:
                if portfolio in indicador.upper():
                    current_section = portfolio
                    break

            # 3. Extraer valores
            # La tabla usa una estructura "staircase" responsive: cada fila contiene el indicador
            # en texts[0] y sus N valores en texts[1:N+1]. No usar texts[-N:] ya que las filas
            # largas acumulan datos de filas posteriores al final.
            if len(texts) < (num_periods + 1): continue

            vals = texts[1:num_periods + 1]
            
            for label, val_str in zip(current_periods, vals):
                valor = parse_bcra_number(val_str)
                records.append({
                    "codigo_entidad": int(bco),
                    "nombre": nombre, # Metadata útil, aunque se unirá en la DB
                    "seccion": current_section,
                    "periodo": label,
                    "indicador": indicador,
                    "valor": valor
                })
                
    return records

def scrape_balances_table(html, bco, nombre, seccion_defecto):
    """Parseador simple para Estados Contables (Balances)."""
    # Por ahora similar a deudores pero sin la lógica de las 4 carteras
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    tables = soup.find_all('table')
    
    current_section = seccion_defecto
    current_periods = []
    
    for table in tables:
        rows = table.find_all('tr')
        for tr in rows:
            tds = tr.find_all(['td', 'th'])
            if not tds: continue
            texts = [td.get_text(strip=True) for td in tds]

            periods_found = [t for t in texts if ("-" in t and any(m in t for m in ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]))]
            if len(periods_found) >= 3:
                current_periods = periods_found
                continue
            
            if not current_periods: continue
            num_periods = len(current_periods)
            
            indicador = texts[0]
            if not indicador or indicador == nombre or indicador in current_periods: continue
            
            if len(texts) < (num_periods + 1): continue
            
            # Para balances, si el indicador está en MAYÚSCULAS lo tratamos como sección
            if indicador.isupper() and len(indicador) > 3:
                current_section = indicador
                
            vals = texts[-num_periods:]
            for label, val_str in zip(current_periods, vals):
                valor = parse_bcra_number(val_str)
                records.append({
                    "codigo_entidad": int(bco),
                    "nombre": nombre,
                    "seccion": current_section,
                    "periodo": label,
                    "indicador": indicador,
                    "valor": valor
                })
    return records
