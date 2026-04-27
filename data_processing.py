import pandas as pd
from PyPDF2 import PdfReader
import re
import os
from database_manager import DatabaseManager


def main():
    db = DatabaseManager()
    print("Iniciando procesamiento de datos (SQLite)...")

    # 1. Cargar grupos desde PDF
    try:
        reader = PdfReader("A-8367 (Listado SF completo).pdf")
        full_text = "\n".join([p.extract_text() for p in reader.pages]).upper()
        
        # Estrategia más robusta: buscar "GRUPO A" y "GRUPO B" como encabezados de sección.
        # Usamos regex para encontrar la posición de los grupos ignorando otros textos.
        match_idx_a = re.search(r'\n\s*GRUPO\s+A\s*\n', full_text)
        match_idx_b = re.search(r'\n\s*GRUPO\s+B\s*\n', full_text)
        
        if match_idx_a and match_idx_b:
            idx_a = match_idx_a.end()
            idx_b = match_idx_b.start()
            text_a = full_text[idx_a:idx_b]
            text_b = full_text[match_idx_b.end():]
        else:
            # Fallback a búsqueda rfind si fallan las regex de encabezado exacto
            idx_a = full_text.rfind("\nGRUPO A")
            idx_b = full_text.rfind("\nGRUPO B")
            if idx_a != -1 and idx_b != -1 and idx_b > idx_a:
                text_a = full_text[idx_a:idx_b]
                text_b = full_text[idx_b:]
            else:
                text_a, text_b = "", ""
                
        # Extraer mapeos de grupos y guardarlos
        if text_a or text_b:
            print("Extrayendo mapeos de grupos desde el PDF...")
            groups_mapping = []
            
            # Buscamos patrones como "123 BANCO..." al inicio de línea
            # text_a -> Grupo A
            for match in re.finditer(r'(?m)^\s*(\d+)\s+', text_a):
                groups_mapping.append({'codigo_entidad': int(match.group(1)), 'grupo': 'Grupo A'})
            
            # text_b -> Grupo B
            for match in re.finditer(r'(?m)^\s*(\d+)\s+', text_b):
                groups_mapping.append({'codigo_entidad': int(match.group(1)), 'grupo': 'Grupo B'})
                
            if groups_mapping:
                df_groups = pd.DataFrame(groups_mapping)
                db.save_entity_groups(df_groups)
                print(f"Se actualizaron {len(df_groups)} mapeos de grupos.")

    except Exception as e:
        print(f"Error parseando PDF: {e}")

    # 2. Enriquecer Entidades con grupos
    print("Enriqueciendo metadatos de entidades...")
    df_entities = pd.read_sql("SELECT * FROM entities", db.conn)
    df_mapped_groups = pd.read_sql("SELECT * FROM entity_groups", db.conn)

    # Unir con grupos mapeados
    df_entities = pd.merge(df_entities.drop(columns=['grupo_sistema']), 
                           df_mapped_groups, 
                           on='codigo_entidad', 
                           how='left')
    df_entities['grupo_sistema'] = df_entities['grupo'].fillna('Otros')
    df_entities = df_entities.drop(columns=['grupo'])
    
    db.save_entities(df_entities)

    # 3. Calcular Benchmarks
    print("Calculando benchmarks...")
    df_obs = db.get_long_data(seccion='Indicadores')
    benchmarks_list = []

    if not df_obs.empty:
        df_obs = pd.merge(df_obs, df_entities[['codigo_entidad', 'grupo_sistema']], on='codigo_entidad')

        agrupaciones = {
            'Sistema Total': df_obs,
            'Grupo A': df_obs[df_obs['grupo_sistema'] == 'Grupo A'],
            'Grupo B': df_obs[df_obs['grupo_sistema'] == 'Grupo B']
        }

        for nombre_agrup, df_agrup in agrupaciones.items():
            if df_agrup.empty: continue
            # Promedio y Mediana de Sistema Total vienen del BCRA oficial (AAA00), no se calculan.
            metricas = [('P25', lambda x: x.quantile(0.25)), ('P75', lambda x: x.quantile(0.75))]
            if nombre_agrup != 'Sistema Total':
                metricas = [('Promedio', 'mean'), ('Mediana', 'median')] + metricas

            for periodo in df_agrup['periodo'].unique():
                df_pivot = df_agrup[df_agrup['periodo'] == periodo].pivot_table(
                    index='codigo_entidad', columns='indicador', values='valor', aggfunc='last'
                )
                for m_name, m_func in metricas:
                    m_values = df_pivot.agg(m_func).to_dict() if isinstance(m_func, str) else m_func(df_pivot).to_dict()
                    for ind, val in m_values.items():
                        benchmarks_list.append({
                            'agrupacion': nombre_agrup,
                            'metrica': m_name,
                            'periodo': periodo,
                            'indicador': ind,
                            'valor': val
                        })

    # Promedio de Sistema Total: leer desde DB (scrapeado con bco=AAA00, codigo_entidad=0)
    print("  Leyendo benchmarks oficiales del BCRA desde la base de datos (codigo_entidad=0)...")
    df_sistema = pd.read_sql(
        "SELECT periodo, indicador, valor FROM observations WHERE codigo_entidad = 0 AND fuente = 'indicadores_sistema'",
        db.conn
    )
    if not df_sistema.empty:
        # El valor oficial del BCRA se usa tanto para Promedio como para Mediana,
        # ya que es el dato autoritativo del sistema — no un cálculo sobre la muestra.
        for metrica_oficial in ('Promedio', 'Mediana'):
            for _, row in df_sistema.iterrows():
                benchmarks_list.append({
                    'agrupacion': 'Sistema Total',
                    'metrica': metrica_oficial,
                    'periodo': row['periodo'],
                    'indicador': row['indicador'],
                    'valor': row['valor']
                })
        print(f"  {len(df_sistema)} valores oficiales leídos (Promedio + Mediana).")
    else:
        print("  [!] Sin datos de Sistema Total en DB. Ejecutar scraper primero. Usando cálculo local como fallback.")
        if not df_obs.empty:
            for periodo in df_obs['periodo'].unique():
                df_pivot = df_obs[df_obs['periodo'] == periodo].pivot_table(
                    index='codigo_entidad', columns='indicador', values='valor', aggfunc='last'
                )
                for ind, val in df_pivot.mean().to_dict().items():
                    benchmarks_list.append({
                        'agrupacion': 'Sistema Total',
                        'metrica': 'Promedio',
                        'periodo': periodo,
                        'indicador': ind,
                        'valor': val
                    })

    if benchmarks_list:
        db.save_benchmarks(pd.DataFrame(benchmarks_list))

    print("\nProcesamiento finalizado. Base de datos actualizada.")

if __name__ == "__main__":
    main()
