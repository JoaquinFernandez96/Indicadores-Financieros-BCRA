import streamlit as st
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import main  # Importamos el pipeline principal
from report_engine import generate_pdf_report
from database_manager import DatabaseManager
import tempfile
import io

# Configuración de página
st.set_page_config(page_title="Dashboard Financiero BCRA", layout="wide")

def render_svg(icon_name, size=24):
    icon_path = os.path.join("static", "icons", f"{icon_name}.svg")
    try:
        with open(icon_path, "r", encoding="utf-8") as f:
            svg_content = f.read()
        # Eliminar dimensiones fijas para controlarlas por CSS
        svg_content = svg_content.replace('width="24"', f'width="{size}"').replace('height="24"', f'height="{size}"')
        return f'<span class="svg-icon">{svg_content}</span>'
    except Exception:
        return ""

st.markdown(f"<h1>{render_svg('bank', 32)} Dashboard de Benchmarking Financiero BCRA</h1>", unsafe_allow_html=True)
st.markdown("---")

@st.cache_data(ttl=300)
def load_data_v3(db_path="bcra_dashboard.db", read_only=False):
    db = DatabaseManager(db_path=db_path, read_only=read_only)
    
    # 1. Cargar metadatos de entidades
    df_entities = pd.read_sql("SELECT * FROM entities", db.conn)
    df_entities = df_entities.rename(columns={
        'codigo_entidad': 'Codigo_Entidad',
        'nombre': 'Nombre de Entidad'
    })
    
    # 2. Cargar Indicadores (pivoted)
    df_enriched = db.get_wide_data('Indicadores')
    df_enriched = df_enriched.rename(columns={'codigo_entidad': 'Codigo_Entidad', 'nombre': 'Nombre de Entidad', 'periodo': 'Periodo'})
    df_enriched = pd.merge(df_enriched, df_entities[['Codigo_Entidad', 'grupo_sistema']], on='Codigo_Entidad', how='left')
    
    # 3. Cargar EECC y Deudores
    df_eecc = db.get_wide_data('Balances')
    if not df_eecc.empty:
        df_eecc = df_eecc.rename(columns={'codigo_entidad': 'Codigo_Entidad', 'nombre': 'Nombre de Entidad', 'periodo': 'Periodo'})
    
    # Para Deudores, ahora tenemos múltiples secciones específicas. Las agrupamos todas.
    query_deud = """
        SELECT o.*, e.nombre 
        FROM observations o
        LEFT JOIN entities e ON o.codigo_entidad = e.codigo_entidad
        WHERE o.fuente = 'deudores'
    """
    df_deud_long = pd.read_sql(query_deud, db.conn)
    if not df_deud_long.empty:
        df_deudores = df_deud_long.pivot_table(
            index=['codigo_entidad', 'nombre', 'periodo'],
            columns='indicador',
            values='valor',
            aggfunc='last' # Por si acaso hay solapamiento, aunque los indicadores son únicos
        ).reset_index()
        df_deudores = df_deudores.rename(columns={'codigo_entidad': 'Codigo_Entidad', 'nombre': 'Nombre de Entidad', 'periodo': 'Periodo'})
    else:
        df_deudores = pd.DataFrame()
        
    # 4. Cargar Benchmarks
    df_benchmarks = pd.read_sql("SELECT * FROM benchmarks", db.conn)
    df_benchmarks = df_benchmarks.rename(columns={
        'agrupacion': 'Agrupacion',
        'metrica': 'Metrica',
        'periodo': 'Periodo',
        'indicador': 'Indicador',
        'valor': 'Valor'
    })
    # Pivot benchmarks if the app expects wide format
    if not df_benchmarks.empty:
        df_benchmarks = df_benchmarks.pivot_table(
            index=['Agrupacion', 'Metrica', 'Periodo'],
            columns='Indicador',
            values='Valor'
        ).reset_index()

    # --- FINAL DEDUPLICATION AND CLEAN-UP ---
    # Strip whitespace from keys and FORCE NUMERIC for all data columns
    for df in [df_enriched, df_eecc, df_deudores]:
        if df is not None and not df.empty:
            # 1. Clean identity columns
            for col in ['Nombre de Entidad', 'Periodo', 'Agrupacion', 'Metrica']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip()
            
            # 2. Force numeric for all other columns (indicators)
            id_cols = ['Codigo_Entidad', 'Nombre de Entidad', 'Periodo', 'grupo_sistema', 'Agrupacion', 'Metrica', 'logo_url']
            for col in df.columns:
                if col not in id_cols:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # 3. Fill NaNs
            df.fillna(0, inplace=True)
    
    # Strictly drop duplicates by identity columns
    if not df_enriched.empty:
        df_enriched = df_enriched.drop_duplicates(subset=['Codigo_Entidad', 'Periodo'])
    if not df_deudores.empty:
        df_deudores = df_deudores.drop_duplicates(subset=['Codigo_Entidad', 'Periodo'])
    if not df_eecc.empty:
        df_eecc = df_eecc.drop_duplicates(subset=['Codigo_Entidad', 'Periodo'])
    # 4. Generate Logo Map
    logo_map = df_entities.set_index('Codigo_Entidad')['logo_url'].to_dict()
    
    return df_enriched, df_benchmarks, df_eecc, df_deudores, logo_map

# --- SIDEBAR: DB SELECTION (widget se renderiza al final del sidebar) ---
if "db_mode" not in st.session_state:
    st.session_state.db_mode = False

db_mode = st.session_state.db_mode
selected_db = "bcra_dashboard_demo.db" if db_mode else "bcra_dashboard.db"

# Clear cache if DB switches to ensure data integrity
if "last_db" not in st.session_state:
    st.session_state.last_db = selected_db

if st.session_state.last_db != selected_db:
    st.cache_data.clear()
    st.session_state.last_db = selected_db
    st.rerun()

# --- LOAD DATA ---
df_enriched, df_benchmarks, df_eecc, df_deudores, logo_map = load_data_v3(selected_db, read_only=db_mode)

if df_enriched.empty:
    st.stop()

# Mapeo de indicadores por sección (basado en prefijo del nombre del indicador)
SECCIONES = {
    "Capital": lambda c: c.startswith("C"),
    "Activos": lambda c: c.startswith("A"),
    "Eficiencia": lambda c: c.startswith("E"),
    "Rentabilidad": lambda c: c.startswith("R"),
    "Liquidez": lambda c: c.startswith("L"),
}

# Todos los ratios disponibles (excluyendo columnas de meta-datos)
META_COLS = {'Codigo_Entidad', 'Nombre de Entidad', 'Periodo', 'grupo_sistema', 'Agrupacion', 'Metrica', 'Logo_URL'}
ratios_disponibles = [col for col in df_benchmarks.columns if col not in META_COLS]

# -----------------
# 1. SIDEBAR
# -----------------
# Cargar datos de la entidad seleccionada tempranamente para el logo
nombres_display = sorted(df_enriched['Nombre de Entidad'].astype(str).unique().tolist())

# Intentar recuperar la selección previa si existe en session_state para mantener el logo sincronizado
if 'selected_display' not in st.session_state and nombres_display:
    st.session_state.selected_display = nombres_display[0]

# Marcador de posición para el Logo (se llenará después de la selección para evitar lag)
logo_placeholder = st.sidebar.empty()

# 0. CONFIGURACIÓN PREMIUM Y ESTILOS (CSS INYECTADO)
# -----------------
# Paleta de colores Bloomberg-Style (Alto Contraste)
PEER_COLORS = {
    "Seleccionado": "#2563EB",  # Bloomberg Blue
    "Sistema": "#64748B",       # Slate 500
    "Grupo": "#94A3B8",         # Slate 400
    "Positive": "#10B981",      # Bloomberg Green
    "Negative": "#EF4444",      # Bloomberg Red
    "Peers": ["#F59E0B", "#10B981", "#6366F1", "#EC4899", "#8B5CF6"] # Colores ajustados
}

METRIC_SUCCESS_DIRECTION = {
    "C1 - Apalancamiento (en veces)": False,
    "A9 - Total Cartera Irregular / Total Financiaciones (%)": False,
    "A11 - Cartera Irregular s/ Financiaciones al Sector Privado (%)": False,
    "A1 - Activos del Sector Público / Activos Totales (%)": False,
    "E1 - Absorción de Gastos de Ad. con Volúmen de Negocio (%)": False,
    "RG1 - Retorno sobre Activos ( ROA) (%)": True,
    "L1-Liquidez Tit c/cot, POS. CALL, LELIQ, LEFIs (netas) (%)": True,
    "R1 - Margen Financiero / Activos (%)": True,
    "E7 - Gastos de Administración /Activos (%)": False,
    "Liquidez": True,
    "Activos": False,
    "Eficiencia": True,
    "Rentabilidad": True,
    "Capital": False
}

ALIAS_MAP = {}

def get_label(full_name, max_len=60):
    return full_name

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
    }

    /* Tabs Sticky UI - Final Correction (Aggressive Override) */
    div[data-testid="stTabs"] {
        overflow: visible !important;
    }
    div[data-testid="stTabs"] [role="tablist"] {
        position: sticky !important;
        top: 0px !important;
        z-index: 100000 !important;
        background-color: var(--background-color) !important;
        margin-top: -10px !important;
        padding-top: 10px !important;
        border-bottom: 2px solid rgba(128,128,128,0.1) !important;
    }
    
    /* Forzar visibilidad en Light Mode */
    [data-testid="stTabs"] [role="tab"] {
        background-color: transparent !important;
    }
    
    /* Estilo de los Labels de los Tabs para Visibilidad */
    [data-testid="stTabs"] [role="tab"] p {
        font-size: 1.05rem !important;
        font-weight: 700 !important;
        opacity: 0.6;
        transition: opacity 0.2s ease;
    }
    
    [data-testid="stTabs"] [role="tab"][aria-selected="true"] p {
        opacity: 1 !important;
        color: #2563EB !important; /* Resaltar activo con Azul Bloomberg */
    }

    /* Sidebar - Ancho Reducido y Adaptativo */
    [data-testid="stSidebar"] {
        min-width: 320px !important;
        max-width: 320px !important;
    }
    
    /* Contenedores de Tarjetas Premium (Estructura Base) */
    [data-testid="metric-container"], .stMetric {
        padding: 1.5rem !important;
        border-radius: 12px !important;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    [data-testid="metric-container"]:hover, .stMetric:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05) !important;
    }
    
    /* MODO LIGHT (Forzar Blanco Puro y Contraste) */
    html[data-theme="light"] [data-testid="metric-container"], 
    html[data-theme="light"] .stMetric {
        background-color: #ffffff !important;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05) !important;
    }
    html[data-theme="light"] [data-testid="stMetricLabel"] {
        color: #64748b !important;
    }
    html[data-theme="light"] [data-testid="stMetricValue"] {
        color: #1e3a8a !important;
    }
    html[data-theme="light"] [data-testid="stMetricDelta"] {
        font-weight: 800 !important;
    }
    html[data-theme="light"] [data-testid="stMetricDelta"] div[dir="ltr"] {
        color: #059669 !important; /* Verde profundo sobre blanco */
    }

    /* MODO DARK (Revertido a estética original - Glassmorphism) */
    html[data-theme="dark"] [data-testid="metric-container"],
    html[data-theme="dark"] .stMetric {
        background-color: rgba(30, 41, 59, 0.7) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2) !important;
    }
    html[data-theme="dark"] [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }
    html[data-theme="dark"] [data-testid="stMetricValue"] {
        color: #60a5fa !important;
    }
    html[data-theme="dark"] [data-testid="stMetricDelta"] {
        font-weight: 500 !important;
    }

    /* Estilos mejorados para multiselect */
    .stMultiSelect div[data-baseweb="tag"] {
        background-color: #f1f5f9 !important;
        border-radius: 8px !important;
        color: #1e293b !important;
        padding-left: 8px !important;
    }

    /* Plotly Tooltips con mejor visibilidad */
    .hoverlayer .hovertext rect {
        fill: #0f172a !important;
        stroke: #334155 !important;
    }

    /* Estilos para Iconos SVG */
    .svg-icon {
        display: inline-flex;
        align-self: center;
        margin-right: 8px;
        color: var(--text-primary);
        vertical-align: middle;
    }
    .svg-icon svg {
        fill: none;
        stroke: currentColor;
    }
    
    /* Espaciado para botones con iconos */
    .stButton button {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }

    /* Mejora de Contraste para Deltas en Modo Light (vs Ref.) */
    [data-testid="stMetricDelta"] {
        font-weight: 700 !important;
    }
    
    /* Forzar colores más vibrantes en Light Mode para los deltas */
    [data-testid="stMetricDelta"] div[data-testid="stIcon"] + div {
        color: inherit !important;
    }

    /* Asegurar que el delta sea legible sobre fondo blanco */
    [data-testid="stMetricDelta"] {
        background-color: rgba(0, 0, 0, 0.03) !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }

    /* No aplicar fondo extra en Dark Mode para no romper estética */
    @media (prefers-color-scheme: dark) {
        [data-testid="stMetricDelta"] {
            background-color: transparent !important;
        }
    }
    html[data-theme="dark"] [data-testid="stMetricDelta"] {
        background-color: transparent !important;
    }

    /* Estilos mejorados para multiselect */
    .stMultiSelect div[data-baseweb="tag"] {
        background-color: #f1f5f9 !important;
        border-radius: 8px !important;
        color: #1e293b !important;
        padding-left: 8px !important;
    }

    /* Plotly Tooltips con mejor visibilidad */
    .hoverlayer .hovertext rect {
        fill: #0f172a !important;
        stroke: #334155 !important;
    }

    /* Estilos para Iconos SVG */
    .svg-icon {
        display: inline-flex;
        align-self: center;
        margin-right: 8px;
        color: var(--text-primary);
        vertical-align: middle;
    }
    .svg-icon svg {
        fill: none;
        stroke: currentColor;
    }
    
    /* Espaciado para botones con iconos */
    .stButton button {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }

    /* Force Dark Mode if Streamlit's Internal Theme is Set to Dark */
    html[data-theme="dark"] [data-testid="metric-container"],
    html[data-theme="dark"] .stMetric {
        background-color: rgba(30, 41, 59, 0.7) !important;
        border: 1px solid rgba(255, 255, 255, 0.1) !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2) !important;
    }
    html[data-theme="dark"] [data-testid="stMetricValue"] {
        color: #60a5fa !important;
    }
    html[data-theme="dark"] [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
    }

    /* Sparklines: deshabilitar interactividad por completo */
    .sparkline-container iframe,
    .sparkline-container div {
        pointer-events: none !important;
    }
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown(f"### {render_svg('settings', 18)} Configuración", unsafe_allow_html=True)

# Mapeo de Entidades
map_display_to_real = {str(row['Nombre de Entidad']): str(row['Nombre de Entidad']) for _, row in all_entities.iterrows()}

def parse_period(p):
    meses = {'Ene':1,'Feb':2,'Mar':3,'Abr':4,'May':5,'Jun':6,'Jul':7,'Ago':8,'Sep':9,'Oct':10,'Nov':11,'Dic':12}
    try:
        m, a = p.split('-')
        return int(a), meses.get(m, 0)
    except:
        return 0, 0

with st.sidebar.expander("Configuración de Entidad", expanded=True):
    selected_display = st.selectbox(
        "Seleccione Entidad (EEFF):", 
        nombres_display, 
        index=nombres_display.index(st.session_state.selected_display) if st.session_state.selected_display in nombres_display else 0,
        key="selected_display_widget"
    )
    # Actualizar session_state
    st.session_state.selected_display = selected_display
    cliente_seleccionado = map_display_to_real[selected_display]

    periodos_disponibles = sorted(df_enriched['Periodo'].dropna().unique().tolist(), key=parse_period, reverse=True)
    ultimo_periodo = periodos_disponibles[0] if periodos_disponibles else "N/D"
    periodo_seleccionado = st.selectbox("Período:", periodos_disponibles)
    st.caption(f"Último dato disponible: `{ultimo_periodo}`")

with st.sidebar.expander("Comparativa de Mercado", expanded=True):
    # Lógica de Referencias Simplificada
    ref_mercado = st.selectbox(
        "Referencia Principal:",
        options=["Sistema Total", "Mismo Grupo", "Ninguna"],
        index=0
    )
    
    # Peer filtering logic
    entidad_data = df_enriched[df_enriched['Nombre de Entidad'] == cliente_seleccionado]
    grupo_cliente = entidad_data['grupo_sistema'].iloc[0] if not entidad_data.empty else None
    
    only_same_group = st.toggle("Sugerir solo mismo grupo", value=True)
    
    if only_same_group and grupo_cliente:
        filtered_peer_entities = df_enriched[df_enriched['grupo_sistema'] == grupo_cliente]['Nombre de Entidad'].unique()
        peer_options = [n for n in nombres_display if map_display_to_real.get(n) in filtered_peer_entities and n != selected_display]
    else:
        peer_options = [n for n in nombres_display if n != selected_display]
        
    selected_peers_display = st.multiselect("Bancos adicionales (Peers):", peer_options, max_selections=3)
    peers_seleccionados = [map_display_to_real[p] for p in selected_peers_display]
    
    metrica_bench = st.radio("Cálculo de Benchmark:", ["Mediana", "Promedio"], horizontal=True)

with st.sidebar.expander("Selección de Indicadores"):
    secciones_sel = {}
    for sec in SECCIONES:
        secciones_sel[sec] = st.checkbox(sec, value=True)
    
    def get_seccion_name(col):
        for sec, fn in SECCIONES.items():
            if fn(col):
                return sec
        return "Otros"

    ratios_por_seccion = [r for r in ratios_disponibles if secciones_sel.get(get_seccion_name(r), True)]
    
    ratios_filtrados = st.multiselect(
        "Indicadores específicos:",
        options=ratios_por_seccion,
        default=ratios_por_seccion[:3] if len(ratios_por_seccion)>3 else ratios_por_seccion,
        format_func=get_label
    )

with st.sidebar.expander("Mantenimiento"):
    if db_mode:
        st.info("🔒 Modo Demo activo — la base de datos es de solo lectura y no puede ser modificada.")

    col_upd, col_res = st.columns(2)

    if col_upd.button(
        "Actualizar",
        use_container_width=True,
        disabled=db_mode,
        help="No disponible en Modo Demo" if db_mode else "Descarga los últimos datos del BCRA"
    ):
        with st.spinner("Descargando datos..."):
            try:
                main.main()
                st.cache_data.clear()
                st.toast("Base de datos sincronizada")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    if col_res.button("Reset", use_container_width=True):
        st.cache_data.clear()
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.toast("Caché y filtros restablecidos")
        st.rerun()

# RENDERIZAR LOGO EN EL PLACEHOLDER (ahora sí, con el dato actualizado)
try:
    # Filtrar por el nombre real de la entidad
    selected_rows = df_enriched[df_enriched['Nombre de Entidad'] == cliente_seleccionado]
    if not selected_rows.empty:
        entidad_raw = selected_rows.iloc[0]
        raw_code = entidad_raw['Codigo_Entidad']
        codigo_str = str(int(raw_code)).zfill(5)
        local_logo_path = os.path.join("logos", f"{codigo_str}.png")
        
        # 1. Intentar local
        if os.path.exists(local_logo_path):
            logo_placeholder.image(local_logo_path, width=150)
        # 2. Intentar remoto desde el maestro de logos
        else:
            remote_url = logo_map.get(int(raw_code))
            if remote_url and pd.notna(remote_url):
                logo_placeholder.image(remote_url, width=150)
except:
    pass


# --- PREPARACIÓN PARA EXPORTACIÓN PDF ---
st.sidebar.markdown("---")
# Botón de exportación en el sidebar
if st.sidebar.button("Generar Reporte PDF", use_container_width=True, help="Exporta la vista actual a un PDF profesional."):
    st.session_state.trigger_pdf = True
    st.rerun()

# Convertir ratios a numérico en el dataset completo primero
for c in ratios_disponibles:
    df_enriched[c] = pd.to_numeric(df_enriched[c], errors='coerce')

# 1. Datos de la Entidad Principal
df_hist_p = df_enriched[df_enriched['Nombre de Entidad'] == cliente_seleccionado].copy()
df_hist_p['sort_key'] = df_hist_p['Periodo'].apply(parse_period)
df_hist_p = df_hist_p.sort_values('sort_key')
df_p_periodo = df_hist_p[df_hist_p['Periodo'] == periodo_seleccionado].copy()

# 2. Datos de Peers Seleccionados
df_peers_periodo = {}
for peer in peers_seleccionados:
    df_peers_periodo[peer] = df_enriched[(df_enriched['Nombre de Entidad'] == peer) & (df_enriched['Periodo'] == periodo_seleccionado)]

# 3. Benchmarks Filtrados por Métrica Seleccionada
df_bench_metrica = df_benchmarks[df_benchmarks['Metrica'] == metrica_bench]
df_bench_periodo = df_bench_metrica[df_bench_metrica['Periodo'] == periodo_seleccionado]

mean_cliente = df_p_periodo[ratios_disponibles].iloc[0].to_dict() if not df_p_periodo.empty else {}
mean_sistema = df_bench_periodo[df_bench_periodo['Agrupacion'] == 'Sistema Total'].iloc[0].to_dict() if not df_bench_periodo[df_bench_periodo['Agrupacion'] == 'Sistema Total'].empty else {}

# Lógica de Referencia Principal
if ref_mercado == "Sistema Total":
    ref_label = "Sistema"
    df_ref = df_bench_periodo[df_bench_periodo['Agrupacion'] == 'Sistema Total']
elif ref_mercado == "Mismo Grupo":
    ref_label = grupo_cliente
    df_ref = df_bench_periodo[df_bench_periodo['Agrupacion'] == grupo_cliente]
else:
    ref_label = "N/A"
    df_ref = pd.DataFrame()

mean_ref = df_ref.iloc[0].to_dict() if not df_ref.empty else {}
if not df_ref.empty:
    ref_label = ref_label
else:
    if ref_mercado != "Ninguna":
        # Fallback inteligente y notificación
        st.sidebar.warning(f"⚠️ Sin datos para '{ref_label}'. Benchmarks basados en 'Sistema Total'.")
        df_ref = df_bench_periodo[df_bench_periodo['Agrupacion'] == 'Sistema Total']
        mean_ref = df_ref.iloc[0].to_dict() if not df_ref.empty else {}
        ref_label = "Sistema" 



# -----------------
# 2. PANEL PRINCIPAL
# -----------------
def create_sparkline(df, column, color="#0047AB"):
    if df.empty or column not in df.columns:
        return None
    
    # Limpiar datos para el gráfico
    data = df[column].ffill().bfill()
    if data.empty:
        return None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=data,
        mode='lines',
        line=dict(color=color, width=2),
        fill='tozeroy',
        fillcolor=f'rgba{tuple(list(int(color.lstrip("#")[i:i+2], 16) for i in (0, 2, 4)) + [0.1])}', # Mismo color con opacidad
        hoverinfo='none'
    ))
    
    fig.update_layout(
        height=40,
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        showlegend=False
    )
    return fig

tab_dash, tab_eecc, tab_deudores, tab_rank = st.tabs([
    f"{render_svg('layout', 18)} Dashboard General",
    f"{render_svg('file-text', 18)} Estados Contables",
    f"{render_svg('users', 18)} Situación de Deudores",
    f"{render_svg('list', 18)} Ranking de Entidades"
])


with tab_dash:
    st.markdown(f'<h1 style="font-size:2.4rem; font-weight:800; letter-spacing:-0.02em; margin-bottom:4px;">{render_svg("building", 36)} {cliente_seleccionado}</h1>', unsafe_allow_html=True)
    st.markdown(f"**Grupo:** `{grupo_cliente}` | **Período:** `{periodo_seleccionado}` | **Referencia:** `{ref_label}`")
    st.markdown("<br>", unsafe_allow_html=True)

    # KPIs con Lógica de Éxito Automática
    col_k1, col_k2, col_k3, col_k4, col_k5 = st.columns(5)
    kpi_keys = {
        "Apalancamiento": "C1 - Apalancamiento (en veces)",
        "Calidad Cartera": "A9 - Total Cartera Irregular / Total Financiaciones (%)",
        "Eficiencia": "E1 - Absorción de Gastos de Ad. con Volúmen de Negocio (%)",
        "Rentabilidad": "RG1 - Retorno sobre Activos ( ROA) (%)",
        "Liquidez": "L1-Liquidez Tit c/cot, POS. CALL, LELIQ, LEFIs (netas) (%)"
    }

    # Definiciones para Tooltips
    help_texts = {
        "Apalancamiento": "**C1 - Apalancamiento (en veces)**: Indicador Inverso (Menor es mejor). Relación entre Activos y Patrimonio Neto. Un menor apalancamiento indica mayor solidez.",
        "Calidad Cartera": "**A9 - Cartera Irregular / Financiaciones (%)**: Indicador Inverso (Menor es mejor). Cartera Irregular sobre Financiaciones Totales. Un ratio bajo indica mejor calidad crediticia.",
        "Eficiencia": "**E1 - Absorción de Gastos de Ad. / Volúmen (%)**: Indicador Inverso (Menor es mejor). Absorción de Gastos Administrativos con Volumen de Negocio. Menor ratio = mayor eficiencia operativa.",
        "Rentabilidad": "**RG1 - Retorno sobre Activos (ROA) (%)**: Indicador Directo (Mayor es mejor). ROA (Return on Assets): Rentabilidad neta sobre el total de activos de la entidad.",
        "Liquidez": "**L1 - Liquidez con Títulos, Call, Leliqs (%)**: Indicador Directo (Mayor es mejor). Disponibilidad de fondos líquidos (Efectivo, Leliqs, Títulos) sobre depósitos y pasivos."
    }

    radar_indices = []
    for col_w, (sec_name, ind_key) in zip([col_k1, col_k2, col_k3, col_k4, col_k5], kpi_keys.items()):
        val = mean_cliente.get(ind_key)
        ref = mean_ref.get(ind_key) if not df_ref.empty else None
    
        if pd.isna(val):
            val_str, delta, delta_color = "N/D", None, "normal"
        else:
            val_str = f"{float(val):.2f}%" if "%" in ind_key or "ROA" in ind_key else f"{float(val):.2f}"
            diff = float(val) - float(ref) if ref is not None and not pd.isna(ref) else 0
            delta = round(diff, 2)
        
            # Inteligencia de color simplificada
            is_higher_better = METRIC_SUCCESS_DIRECTION.get(ind_key, True)
            if abs(delta) < 0.01:
                delta_color = "off"
            else:
                delta_color = "normal" if is_higher_better else "inverse"
            
        with col_w:
            st.metric(
                label=sec_name, 
                value=val_str, 
                delta=f"{delta:+.2f} vs Ref." if ref is not None else None,
                delta_color=delta_color,
                help=help_texts.get(sec_name)
            )
            spark_fig = create_sparkline(df_hist_p, ind_key, color=PEER_COLORS["Seleccionado"])
            if spark_fig:
                st.plotly_chart(spark_fig, use_container_width=True, config={'displayModeBar': False, 'staticPlot': True})
    
        # Solo añadir al radar si la sección está activa
        if secciones_sel.get(sec_name, True):
            radar_indices.append((sec_name, ind_key))

    st.markdown("---")

    # Fila 1: Barras y Radar
    col1, col2 = st.columns([0.6, 0.4])


    with col1:
        st.markdown(f"### {render_svg('bar-chart', 24)} Comparativa (Valores Absolutos)", unsafe_allow_html=True)
    
        if not ratios_filtrados:
            st.info("Seleccione al menos un indicador en el sidebar para visualizar la comparativa.")
        else:
            data_bars = []
        
            # Entidad principal
            entidades_list = [(cliente_seleccionado, mean_cliente, PEER_COLORS["Seleccionado"])]
        
            # Peers (Usar colores categóricos)
            for i, p_name in enumerate(peers_seleccionados):
                df_p = df_peers_periodo.get(p_name, pd.DataFrame())
                if not df_p.empty:
                    p_vals = df_p[ratios_disponibles].iloc[0].to_dict()
                    color = PEER_COLORS["Peers"][i % len(PEER_COLORS["Peers"])]
                    entidades_list.append((p_name, p_vals, color))
        
            # Referencia
            if not df_ref.empty:
                entidades_list.append((ref_label, mean_ref, PEER_COLORS["Sistema"] if ref_mercado == "Sistema Total" else PEER_COLORS["Grupo"]))

            for nombre_ent, dic_vals, color in entidades_list:
                for ratio in ratios_filtrados:
                    val = dic_vals.get(ratio)
                    if not pd.isna(val):
                        data_bars.append({
                            "Indicador": get_label(ratio),
                            "Sección": get_seccion_name(ratio),
                            "Entidad": nombre_ent,
                            "Valor": float(val),
                            "Color": color
                        })

            if data_bars:
                df_bars = pd.DataFrame(data_bars)
            
                fig_bars = px.bar(
                    df_bars, 
                    y="Indicador", 
                    x="Valor", 
                    color="Entidad", 
                    orientation='h',
                    barmode="group",
                    facet_col="Sección", 
                    height=500,
                    text="Valor",
                    color_discrete_sequence=df_bars.drop_duplicates('Entidad')['Color'].tolist()
                )
            
                fig_bars.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            
                fig_bars.update_layout(
                    legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
                    margin=dict(t=40, b=40, l=350, r=10),
                    hovermode="y unified",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
            
                fig_bars.update_xaxes(showgrid=False, zeroline=False, showticklabels=False, title="")
                fig_bars.update_yaxes(showgrid=False, tickfont=dict(size=10))
                st.plotly_chart(fig_bars, use_container_width=True, config={'displayModeBar': False})

    with col2:
        st.markdown(f"### {render_svg('target', 24)} Radar: Desempeño Relativo", unsafe_allow_html=True)
    
        if ref_mercado == "Ninguna":
            st.info("Seleccione una referencia de mercado (Sistema Total o Mismo Grupo) para habilitar el Radar.")
        elif not radar_indices:
            st.info("Seleccione al menos una sección en el sidebar para visualizar el Radar.")
        else:
            st.caption(f"100% = {ref_label if not df_ref.empty else 'Sistema'}. ↗ Expandido = mejor desempeño relativo.")
    
            cat_names = [x[0] for x in radar_indices]
            radar_cats = cat_names + [cat_names[0]]
        
            def get_radar_relative_vals(dic, ref_dic):
                vals = []
                for sec_name, ind_key in radar_indices:
                    v = float(dic.get(ind_key, 0) or 0)
                    s = float(ref_dic.get(ind_key, 1) or 1)
                    if abs(s) < 0.001: s = 1
                    rel = (v / s) * 100
                
                    # Invertir para las métricas donde menos es mejor
                    higher_is_better = METRIC_SUCCESS_DIRECTION.get(ind_key, True)
                    if not higher_is_better:
                        rel = 100 + (100 - rel) # Si saca 40% (mejor), se vuelve 160% (expande)
                    
                    vals.append(min(max(rel, 0), 250))
                return vals + [vals[0]]

            fig_radar = go.Figure()
        
            # Referencia (Círculo 100%)
            fig_radar.add_trace(go.Scatterpolar(
                r=[100]*len(radar_cats), 
                theta=radar_cats, 
                line=dict(color=PEER_COLORS["Sistema"], width=2, dash='dash'),
                name=f"Base ({ref_label})"
            ))

            # Principal
            fig_radar.add_trace(go.Scatterpolar(
                r=get_radar_relative_vals(mean_cliente, mean_ref if not df_ref.empty else mean_sistema),
                theta=radar_cats, 
                fill='toself', 
                name=cliente_seleccionado, 
                line=dict(color=PEER_COLORS["Seleccionado"], width=4),
                fillcolor=f'rgba(37, 99, 235, 0.35)',
                hovertemplate="<b>%{theta}</b><br>Desempeño Relativo: %{r:.1f}%<extra></extra>"
            ))

            # Peers en el Radar
            for i, p_name in enumerate(peers_seleccionados):
                df_p = df_peers_periodo.get(p_name, pd.DataFrame())
                if not df_p.empty:
                    p_vals = df_p[ratios_disponibles].iloc[0].to_dict()
                    color = PEER_COLORS["Peers"][i % len(PEER_COLORS["Peers"])]
                    fig_radar.add_trace(go.Scatterpolar(
                        r=get_radar_relative_vals(p_vals, mean_ref if not df_ref.empty else mean_sistema),
                        theta=radar_cats, 
                        name=p_name, 
                        line=dict(color=color, width=2, dash='dot'),
                        hovertemplate="<b>%{theta}</b><br>Desempeño Relativo: %{r:.1f}%<extra></extra>"
                    ))

            fig_radar.update_layout(
                polar=dict(
                    bgcolor='rgba(0,0,0,0)',
                    radialaxis=dict(visible=True, range=[0, 250], showgrid=True, gridcolor='rgba(128,128,128,0.1)', tickvals=[0, 100, 250], ticktext=['0', '100% (Base)', 'Max'], showline=False),
                    angularaxis=dict(tickfont=dict(size=11, weight='bold'), gridcolor='rgba(128,128,128,0.1)', showline=False)
                ),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                height=450, 
                margin=dict(t=30, b=20, l=60, r=60),
                legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5)
            )
            st.plotly_chart(fig_radar, use_container_width=True, config={'displayModeBar': False})

    st.markdown("---")

    # Fila 2: Evolución Histórica
    st.markdown(f"### {render_svg('trending-up', 24)} Evolución Histórica Comparativa", unsafe_allow_html=True)
    if not df_hist_p.empty and ratios_disponibles:
        ratio_trend = st.selectbox("Seleccione ratio para ver comparación histórica:", ratios_disponibles, format_func=get_label)
        data_trend = []
    
        # Colores para la tendencia
        color_trend_map = {cliente_seleccionado: PEER_COLORS["Seleccionado"]}
        line_dash_map = {cliente_seleccionado: 'solid'}
    
        # Entidad Principal
        for _, row in df_hist_p.iterrows():
            data_trend.append({"Periodo": row['Periodo'], "Entidad": cliente_seleccionado, "Valor": row[ratio_trend], "sort_key": row['sort_key']})
    
        # Peers Históricos
        for i, p_name in enumerate(peers_seleccionados):
            df_h = df_enriched[df_enriched['Nombre de Entidad'] == p_name].copy()
            df_h['sort_key'] = df_h['Periodo'].apply(parse_period)
            df_h = df_h.sort_values('sort_key')
            color = PEER_COLORS["Peers"][i % len(PEER_COLORS["Peers"])]
            color_trend_map[p_name] = color
            line_dash_map[p_name] = 'solid'
            for _, row in df_h.iterrows():
                data_trend.append({"Periodo": row['Periodo'], "Entidad": p_name, "Valor": row[ratio_trend], "sort_key": row['sort_key']})

        # Referencia Histórica
        if not df_ref.empty:
            df_b_hist = df_benchmarks[df_benchmarks['Metrica'] == metrica_bench].copy()
            df_b_hist['sort_key'] = df_b_hist['Periodo'].apply(parse_period)
            df_b_hist = df_b_hist.sort_values('sort_key')
        
            # Filtrar agrupación según la referencia seleccionada
            agrup_ref = 'Sistema Total' if ref_mercado == "Sistema Total" else grupo_cliente
            df_b_ref = df_b_hist[df_b_hist['Agrupacion'] == agrup_ref]
        
            ref_trend_label = f"Ref: {ref_label}"
            color_trend_map[ref_trend_label] = PEER_COLORS["Sistema"] if ref_mercado == "Sistema Total" else PEER_COLORS["Grupo"]
            line_dash_map[ref_trend_label] = 'dash'
        
            for _, row in df_b_ref.iterrows():
                data_trend.append({"Periodo": row['Periodo'], "Entidad": ref_trend_label, "Valor": row[ratio_trend], "sort_key": row['sort_key']})

        df_plot_trend = pd.DataFrame(data_trend)
    
        # Agregar bandas IQR si hay referencia seleccionada (Sistema Total o Mismo Grupo)
        if not df_ref.empty and ref_mercado in ("Sistema Total", "Mismo Grupo"):
            agrup_bands = 'Sistema Total' if ref_mercado == "Sistema Total" else grupo_cliente
            df_p25 = df_benchmarks[(df_benchmarks['Metrica'] == 'P25') & (df_benchmarks['Agrupacion'] == agrup_bands)].copy()
            df_p75 = df_benchmarks[(df_benchmarks['Metrica'] == 'P75') & (df_benchmarks['Agrupacion'] == agrup_bands)].copy()
            df_p25['sort_key'] = df_p25['Periodo'].apply(parse_period)
            df_p75['sort_key'] = df_p75['Periodo'].apply(parse_period)
            df_p25 = df_p25.sort_values('sort_key')
            df_p75 = df_p75.sort_values('sort_key')
        
            p25_vals = df_p25[['Periodo', ratio_trend]].rename(columns={ratio_trend: 'P25'})
            p75_vals = df_p75[['Periodo', ratio_trend]].rename(columns={ratio_trend: 'P75'})
            df_bands = pd.merge(p25_vals, p75_vals, on='Periodo', how='inner')
        else:
            df_bands = pd.DataFrame()

        fig_line = go.Figure()

        # Añadir bandas IQR primero (para que queden de fondo)
        if not df_bands.empty:
            fig_line.add_trace(go.Scatter(
                x=df_bands["Periodo"], y=df_bands["P75"],
                mode='lines', line=dict(width=0), showlegend=False, hoverinfo='skip'
            ))
            fig_line.add_trace(go.Scatter(
                x=df_bands["Periodo"], y=df_bands["P25"],
                mode='lines', line=dict(width=0), fill='tonexty', fillcolor='rgba(128,128,128,0.1)', 
                name="Rango Normal (P25-P75)", hoverinfo='skip'
            ))

        # Añadir lineas originales
        for entity in df_plot_trend['Entidad'].unique():
            df_ent = df_plot_trend[df_plot_trend['Entidad'] == entity]
            fig_line.add_trace(go.Scatter(
                x=df_ent['Periodo'], 
                y=df_ent['Valor'], 
                mode='lines', # Sin marcadores
                name=entity,
                line=dict(color=color_trend_map.get(entity, '#000'))
            ))

        for trace in fig_line.data:
            if trace.name and trace.name != "Rango Normal (P25-P75)":
                trace.line.dash = line_dash_map.get(trace.name, 'solid')
                if trace.name == cliente_seleccionado:
                    trace.line.width = 4
                else:
                    trace.line.width = 2
                    if trace.name.startswith("Ref"):
                        trace.line.dash = 'dash'
                    else:
                        trace.line.dash = 'dot' # Peers con dot para distinguirlos de la Referencia

        fig_line.update_layout(
            height=450,
            xaxis=dict(type='category', showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)'),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
            hovermode="x unified",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
    
        st.plotly_chart(fig_line, use_container_width=True, config={'displayModeBar': False})

        # TABLA DE DETALLE CON PERCENTILES
        # -----------------
        st.markdown("---")
        st.markdown(f"### {render_svg('table', 24)} Detalle de Datos: {cliente_seleccionado} (Histórico)", unsafe_allow_html=True)
        df_tabla_full = df_hist_p[['Periodo'] + ratios_filtrados].copy()
    
        # Agregar filas de referencia (Percentiles del último periodo seleccionado)
        df_p25 = df_benchmarks[(df_benchmarks['Periodo'] == periodo_seleccionado) & (df_benchmarks['Metrica'] == 'P25') & (df_benchmarks['Agrupacion'] == 'Sistema Total')]
        df_p75 = df_benchmarks[(df_benchmarks['Periodo'] == periodo_seleccionado) & (df_benchmarks['Metrica'] == 'P75') & (df_benchmarks['Agrupacion'] == 'Sistema Total')]
    
        st.dataframe(df_tabla_full.style.format("{:.2f}", subset=ratios_filtrados, na_rep="N/D"), use_container_width=True)
    
        with st.expander(f"Referencias del Mercado (Sistema Total - {periodo_seleccionado})"):
            ref_data = []
            for m in ['P25', 'Mediana', 'Promedio', 'P75']:
                row_ref = df_benchmarks[(df_benchmarks['Periodo'] == periodo_seleccionado) & (df_benchmarks['Metrica'] == m) & (df_benchmarks['Agrupacion'] == 'Sistema Total')]
                if not row_ref.empty:
                    vals = row_ref[ratios_filtrados].iloc[0].to_dict()
                    vals['Métrica'] = m
                    ref_data.append(vals)
            if ref_data:
                df_ref_plot = pd.DataFrame(ref_data)
                st.table(df_ref_plot.set_index('Métrica').style.format("{:.2f}"))

with tab_eecc:
    st.markdown(f"## {render_svg('file-text', 28)} Estados Contables")
    st.markdown(f"**Entidad:** `{cliente_seleccionado}` | **Período:** `{periodo_seleccionado}`")
    
    # Filtrar datos de EECC para la entidad y periodo
    df_eecc_p = df_eecc[(df_eecc['Nombre de Entidad'] == cliente_seleccionado) & (df_eecc['Periodo'] == periodo_seleccionado)]
    
    if df_eecc_p.empty:
        st.warning("No hay datos contables disponibles para esta selección.")
    else:
        eecc_data = df_eecc_p.iloc[0]
        
        # Preparar histórico para gráficos evolutivos
        df_eecc_hist = df_eecc[df_eecc['Nombre de Entidad'] == cliente_seleccionado].copy()
        df_eecc_hist['sort_key'] = df_eecc_hist['Periodo'].apply(parse_period)
        df_eecc_hist = df_eecc_hist.sort_values('sort_key')
        
        # 1. Composición de Balance (Activo vs Pasivo + PN)
        col_bl1, col_bl2 = st.columns(2)
        
        with col_bl1:
            st.markdown("### Composición del Activo")
            # Definir cuentas de activo (simplificado)
            activo_cols = {
                "Disponibilidades": "EFECTIVO Y DEPOSITO EN BANCOS",
                "Títulos Públicos": "TÍTULOS PÚBLICOS Y PRIVADOS",
                "Préstamos": "PRÉSTAMOS",
                "Otros Créditos": "OTROS CRED.POR INTERM.FINAN.",
                "Bienes de Uso": "PROPIEDAD, PLANTAS Y EQUIPO"
            }
            # Filtrar solo las que existen en las columnas
            activo_vals = {k: eecc_data.get(v, 0) for k, v in activo_cols.items() if v in eecc_data}
            activo_vals["Otros Activos"] = eecc_data.get("A C T I V O", 0) - sum(activo_vals.values())
            
            fig_act = px.pie(
                values=list(activo_vals.values()), 
                names=list(activo_vals.keys()),
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            fig_act.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, showlegend=True)
            st.plotly_chart(fig_act, use_container_width=True)

        with col_bl2:
            st.markdown("### Composición de Pasivo + PN")
            pasivo_cols = {
                "Depósitos": "DEPÓSITOS",
                "Préstamos BCRA": "EN ENTIDADES FINANCIERAS", # Simplificación
                "Otras Obligaciones": "OBLIGACIONES DIVERSAS"
            }
            pasivo_vals = {k: eecc_data.get(v, 0) for k, v in pasivo_cols.items() if v in eecc_data}
            pasivo_vals["Patrimonio Neto"] = eecc_data.get("P A T R I M O N I O   N E T O", 0)
            pasivo_vals["Otros Pasivos"] = eecc_data.get("P A S I V O", 0) - sum([v for k,v in pasivo_vals.items() if k != "Patrimonio Neto"])
            
            fig_pas = px.pie(
                values=list(pasivo_vals.values()), 
                names=list(pasivo_vals.keys()),
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_pas.update_layout(margin=dict(t=0, b=0, l=0, r=0), height=300, showlegend=True)
            st.plotly_chart(fig_pas, use_container_width=True)

        st.markdown("---")
        
        # 2. Detalle de Cuentas Principales
        st.markdown(f"### {render_svg('grid', 22)} Principales Cuentas", unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Activo Total", f"$ {eecc_data['A C T I V O']/1e3:,.0f} M", help="Monto expresado en millones de ARS")
        c2.metric("Pasivo Total", f"$ {eecc_data['P A S I V O']/1e3:,.0f} M", help="Monto expresado en millones de ARS")
        c3.metric("Patrimonio Neto", f"$ {eecc_data['P A T R I M O N I O   N E T O']/1e3:,.0f} M", help="Monto expresado en millones de ARS")
        res_ej = eecc_data.get('R D O S. I N T E G R A L E S  A C U M.  D E L  P E R I O D O', 0)
        c4.metric("Resultado Ejercicio", f"$ {res_ej/1e3:,.0f} M", help="Monto expresado en millones de ARS")

        st.markdown("---")
        # 3. Evolución de Composición del Activo (Normalized Stacked Area)
        st.markdown(f"### {render_svg('layers', 24)} Composición Histórica del Activo (%)", unsafe_allow_html=True)
        
        # Preparar datos normalizados
        active_hist_data = []
        for _, row in df_eecc_hist.iterrows():
            total = row.get("A C T I V O", 1)
            if total == 0: total = 1
            
            # Usar las mismas columnas definidas arriba para consistencia
            vals = {k: row.get(v, 0) for k, v in activo_cols.items() if v in row}
            for k, v in vals.items():
                active_hist_data.append({"Periodo": row["Periodo"], "Cuenta": k, "Porcentaje": (v / total) * 100})
            
            # Resto
            otros = total - sum(vals.values())
            active_hist_data.append({"Periodo": row["Periodo"], "Cuenta": "Otros Activos", "Porcentaje": (otros / total) * 100})
            
        df_active_area = pd.DataFrame(active_hist_data)
        fig_active_area = px.area(
            df_active_area, x="Periodo", y="Porcentaje", color="Cuenta",
            color_discrete_sequence=px.colors.qualitative.Prism
        )
        fig_active_area.update_layout(
            height=400,
            hovermode="x unified",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(ticksuffix="%", range=[0, 100], gridcolor='rgba(128,128,128,0.1)', title=""),
            xaxis=dict(showgrid=False, title="")
        )
        st.plotly_chart(fig_active_area, use_container_width=True, config={'displayModeBar': False})

        st.markdown("---")
        # 4. Gráfico de Evolución de Activo, Pasivo y Patrimonio (Valores Absolutos)
        st.markdown(f"### {render_svg('trending-up', 24)} Evolución de Activo, Pasivo y Patrimonio")
        
        fig_eecc_hist = go.Figure()
        fig_eecc_hist.add_trace(go.Scatter(x=df_eecc_hist['Periodo'], y=df_eecc_hist['A C T I V O']/1e3, name="Activo", line=dict(color=PEER_COLORS["Seleccionado"], width=3)))
        fig_eecc_hist.add_trace(go.Scatter(x=df_eecc_hist['Periodo'], y=df_eecc_hist['P A S I V O']/1e3, name="Pasivo", line=dict(color=PEER_COLORS["Negative"], width=3)))
        fig_eecc_hist.add_trace(go.Scatter(x=df_eecc_hist['Periodo'], y=df_eecc_hist['P A T R I M O N I O   N E T O']/1e3, name="Patrimonio Neto", line=dict(color=PEER_COLORS["Positive"], width=3)))
        
        fig_eecc_hist.update_layout(
            height=400,
            hovermode="x unified",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)', title="Monto (Millones $ M)")
        )
        st.plotly_chart(fig_eecc_hist, use_container_width=True)

        st.markdown("---")
        st.markdown(f"### {render_svg('table', 24)} Detalle de Cuentas Contables (Histórico)", unsafe_allow_html=True)
        # Seleccionamos las columnas más relevantes para la tabla
        cols_table = ["Periodo", "A C T I V O", "P A S I V O", "P A T R I M O N I O   N E T O", "DEPÓSITOS", "PRÉSTAMOS", "R D O S. I N T E G R A L E S  A C U M.  D E L  P E R I O D O"]
        cols_present = [c for c in cols_table if c in df_eecc_hist.columns]
        st.dataframe(df_eecc_hist[cols_present].style.format({c: "{:,.0f}" for c in cols_present if c != "Periodo"}), use_container_width=True)

with tab_deudores:
    st.markdown(f"## {render_svg('users', 28)} Situación de Deudores")
    st.markdown(f"**Entidad:** `{cliente_seleccionado}` | **Período:** `{periodo_seleccionado}`")
    
    # Selector de Categoría (Filtro solicitado)
    cat_opciones = ["Totales", "Comercial", "Consumo", "Asimilable a Consumo"]
    selected_cat = st.radio("Filtro por Categoría de Cartera:", cat_opciones, horizontal=True, help="Totales = Comercial + Consumo + Asimilable a Consumo")
    
    # Mapeo de columnas e indicadores según categoría seleccionada
    cat_map = {
        "Totales": {
            "col_fin": "TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)",
            "prefix": "TF.Sit.",
            "labels": {
                "1": "1: Normal", "2": "2: Seguimiento", "3": "3: Problemas", "4": "4: Alto Riesgo", "5": "5: Irrecuperable"
            },
            "cols_sit": {
                "1": "TF.Sit.1: En situación normal (%)",
                "2": "TF.Sit.2: Con seguimiento especial/Riesgo bajo (%)",
                "3": "TF.Sit.3: Con problemas/Riesgo medio (%)",
                "4": "TF.Sit.4: Con alto riesgo de insolvencia/Riesgo alto (%)",
                "5": "TF.Sit.5: Irrecuperable (%)"
            }
        },
        "Comercial": {
            "col_fin": "CARTERA COMERCIAL ($)",
            "prefix": "C.COM.Sit.",
            "labels": {
                "1": "1: Normal", "2": "2: Seguimiento", "3": "3: Problemas", "4": "4: Insolvencia", "5": "5: Irrecuperable"
            },
            "cols_sit": {
                "1": "C.COM.Sit.1: En situación normal (%)",
                "2": "C.COM.Sit.2: Con seguimiento especial (%)",
                "3": "C.COM.Sit.3: Con problemas (%)",
                "4": "C.COM.Sit.4: Con alto riesgo de insolvencia (%)",
                "5": "C.COM.Sit.5: Irrecuperable (%)"
            }
        },
        "Consumo": {
            "col_fin": "CARTERA DE CONSUMO O VIVIENDA ($)",
            "prefix": "C.CON.Sit.",
            "labels": {
                "1": "1: Normal", "2": "2: Seguimiento", "3": "3: Problemas", "4": "4: Insolvencia", "5": "5: Irrecuperable"
            },
            "cols_sit": {
                "1": "C.CON.Sit.1: En situación normal (%)",
                "2": "C.CON.Sit.2: Riesgo bajo (%)",
                "3": "C.CON.Sit.3: Riesgo medio (%)",
                "4": "C.CON.Sit.4: Riesgo alto (%)",
                "5": "C.CON.Sit.5: Irrecuperable (%)"
            }
        },
        "Asimilable a Consumo": {
            "col_fin": "CARTERA COMERCIAL ASIMILABLE A CONSUMO ($)",
            "prefix": "C.CAC.Sit.",
            "labels": {
                "1": "1: Normal", "2": "2: Seguimiento", "3": "3: Problemas", "4": "4: Insolvencia", "5": "5: Irrecuperable"
            },
            "cols_sit": {
                "1": "C.CAC.Sit.1: En situación normal (%)",
                "2": "C.CAC.Sit.2: Riesgo bajo (%)",
                "3": "C.CAC.Sit.3: Riesgo medio (%)",
                "4": "C.CAC.Sit.4: Riesgo alto (%)",
                "5": "C.CAC.Sit.5: Irrecuperable (%)"
            }
        }
    }
    
    mapping = cat_map[selected_cat]
    col_fin_sel = mapping["col_fin"]
    cols_sit_sel = mapping["cols_sit"]
    labels_sel = mapping["labels"]
    
    df_deud_p = df_deudores[(df_deudores['Nombre de Entidad'] == cliente_seleccionado) & (df_deudores['Periodo'] == periodo_seleccionado)]
    
    if df_deud_p.empty:
        st.warning("No hay datos de deudores disponibles para esta selección.")
    else:
        deud_data = df_deud_p.iloc[0]
        
        # 1. KPIs de Calidad
        d1, d2, d3 = st.columns(3)
        total_fin_total = deud_data.get('TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)', 1)
        total_fin_sel = deud_data.get(col_fin_sel, 0)
        prev = deud_data.get('Previsiones por riesgo de incobrabilidad constituídas', 0)
        
        d1.metric(f"Cartera {selected_cat}", f"$ {total_fin_sel:,.0f} M", help="Monto expresado en millones de ARS")
        d2.metric("Previsiones (Totales)", f"$ {prev:,.0f} M", help="Monto expresado en millones de ARS")
        # El índice de cobertura lo mantenemos sobre el total para ser conservadores si no hay previsiones por segmento
        cobertura = (prev / total_fin_total * 100) if total_fin_total > 0 else 0
        d3.metric("Cobertura de Cartera (Total)", f"{cobertura:.2f}%")
        
        st.markdown("---")
        
        # 2. Distribución de Cartera por Tipo
        col_dt1, col_dt2 = st.columns(2)
        
        with col_dt1:
            st.markdown("### Por Tipo de Cartera")
            carteras = {
                "Comercial": deud_data.get('CARTERA COMERCIAL ($)', 0),
                "Comercial (Asim. Consumo)": deud_data.get('CARTERA COMERCIAL ASIMILABLE A CONSUMO ($)', 0),
                "Consumo o Vivienda": deud_data.get('CARTERA DE CONSUMO O VIVIENDA ($)', 0)
            }
            fig_cart = px.bar(
                x=list(carteras.keys()), 
                y=list(carteras.values()),
                color=list(carteras.keys()),
                labels={'x': 'Tipo', 'y': 'Monto (Millones $ M)'},
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_cart.update_layout(height=350, showlegend=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_cart, use_container_width=True)
            
        with col_dt2:
            st.markdown(f"### Clasificación de Situación ({selected_cat})")
            
            # Usar las columnas de situación según la categoría seleccionada
            situaciones = {
                labels_sel["1"]: deud_data.get(cols_sit_sel["1"], 0),
                labels_sel["2"]: deud_data.get(cols_sit_sel["2"], 0),
                labels_sel["3"]: deud_data.get(cols_sit_sel["3"], 0),
                labels_sel["4"]: deud_data.get(cols_sit_sel["4"], 0),
                labels_sel["5"]: deud_data.get(cols_sit_sel["5"], 0)
            }
            
            # Filtrar solo si hay datos para evitar ceros visuales pesados
            fig_sit = px.pie(
                values=list(situaciones.values()), 
                names=list(situaciones.keys()),
                color_discrete_sequence=["#10B981", "#F59E0B", "#F97316", "#EF4444", "#7F1D1D"]
            )
            fig_sit.update_layout(height=350, margin=dict(t=20, b=20, l=0, r=0))
            st.plotly_chart(fig_sit, use_container_width=True)

        st.markdown("---")
        # 3. Evolución de la Cartera Irregular (Demo simplificada con los ratios si estuvieran disponibles en el tiempo)
        st.markdown("### Evolución Histórica de Previsiones")
        df_deud_hist = df_deudores[df_deudores['Nombre de Entidad'] == cliente_seleccionado].copy()
        df_deud_hist['sort_key'] = df_deud_hist['Periodo'].apply(parse_period)
        df_deud_hist = df_deud_hist.sort_values('sort_key')
        
        fig_deud_hist = px.area(
            df_deud_hist, 
            x='Periodo', 
            y='Previsiones por riesgo de incobrabilidad constituídas',
            title="Previsiones Acumuladas (Millones $ M)",
            color_discrete_sequence=[PEER_COLORS["Seleccionado"]]
        )
        fig_deud_hist.update_layout(
            height=400, 
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False),
            yaxis=dict(gridcolor='rgba(128,128,128,0.1)')
        )
        st.plotly_chart(fig_deud_hist, use_container_width=True)

        st.markdown("---")
        st.markdown(f"### {render_svg('shield', 24)} Análisis Avanzado de Riesgo", unsafe_allow_html=True)
        
        c_adv1, c_adv2 = st.columns(2)
        
        with c_adv1:
            # Índice de Cobertura (Previsiones / Cartera Irregular)
            # Calculamos Cartera Irregular como Sit 3 + 4 + 5 usando el prefijo de la categoría
            df_deud_hist['Cartera_Irregular_pct'] = df_deud_hist[[cols_sit_sel["3"], 
                                                               cols_sit_sel["4"], 
                                                               cols_sit_sel["5"]]].sum(axis=1)
            
            # Monto irregular = % * Cartera Seleccionada
            df_deud_hist['Monto_Irregular'] = df_deud_hist['Cartera_Irregular_pct'] / 100 * df_deud_hist[col_fin_sel]
            
            # Ratio Cobertura = Previsiones Totales / Monto Irregular (de la categoría)
            df_deud_hist['Ratio_Cobertura'] = (df_deud_hist['Previsiones por riesgo de incobrabilidad constituídas'] / df_deud_hist['Monto_Irregular'] * 100).replace([float('inf'), -float('inf')], 0).fillna(0)
            
            fig_cob = px.line(
                df_deud_hist, 
                x='Periodo', 
                y='Ratio_Cobertura',
                title=f"Evolución Índice de Cobertura - {selected_cat} (%)",
                labels={'Ratio_Cobertura': '% Cobertura'},
                markers=True,
                color_discrete_sequence=["#10B981"]
            )
            fig_cob.add_hline(y=100, line_dash="dash", line_color="gray", annotation_text="Cobertura 100%")
            fig_cob.update_layout(height=350, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_cob, use_container_width=True)
            
        with c_adv2:
            # Mix de Garantías (Garantizado vs No Garantizado)
            garantizado = deud_data.get('TOTAL GARANTIZADO - Garantías Preferidas A y B ($)', 0)
            no_garantizado = total_fin_total - garantizado
            
            fig_gar = px.pie(
                values=[garantizado, max(0, no_garantizado)],
                names=["Garantizado (Pref. A/B)", "Sin Garantía Preferida"],
                title="Composición por Tipo de Garantía",
                hole=0.4,
                color_discrete_sequence=["#6366F1", "#CBD5E1"]
            )
            fig_gar.update_layout(height=350, margin=dict(t=50, b=20, l=0, r=0))
            st.plotly_chart(fig_gar, use_container_width=True)

        st.markdown("---")
        # 4. Evolución de la Cartera Irregular (Tendencia de Mora)
        st.markdown(f"### {render_svg('shield-alert', 24)} Evolución Histórica de Mora (%)")
        
        df_deud_hist['Mora_Pct'] = df_deud_hist[[cols_sit_sel["3"], cols_sit_sel["4"], cols_sit_sel["5"]]].sum(axis=1)
        fig_mora_trend = px.line(
            df_deud_hist, 
            x='Periodo', 
            y='Mora_Pct',
            title=f"Tendencia de Irregularidad (Sit. 3+4+5) - {selected_cat}",
            labels={'Mora_Pct': '% Mora'},
            markers=True,
            color_discrete_sequence=[PEER_COLORS["Negative"]]
        )
        fig_mora_trend.update_layout(height=400, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig_mora_trend, use_container_width=True)
        st.markdown(f"### {render_svg('table', 24)} Detalle de Situación de Deudores (Histórico)", unsafe_allow_html=True)
        # Seleccionamos las columnas más relevantes para la tabla
        cols_table_deud = ["Periodo", "TOTAL DE FINANCIACIONES Y GARANTIAS OTORGADAS ($)", "Previsiones por riesgo de incobrabilidad constituídas", "TF.Sit.1: En situación normal (%)", "TF.Sit.5: Irrecuperable (%)", "CARTERA COMERCIAL ($)", "CARTERA DE CONSUMO O VIVIENDA ($)"]
        cols_present_deud = [c for c in cols_table_deud if c in df_deud_hist.columns]
        st.dataframe(df_deud_hist[cols_present_deud].style.format({c: "{:,.2f}" if "%" in c else "{:,.0f}" for c in cols_present_deud if c != "Periodo"}), use_container_width=True)

with tab_rank:
    ratio_rank = st.selectbox("Seleccione indicador para rankear:", ratios_disponibles, 
                               index=ratios_disponibles.index(ratios_filtrados[0]) if ratios_filtrados else 0,
                               format_func=get_label)
    
    # 1. Filtro por Grupo en Ranking
    only_group_rank = st.checkbox("Filtrar solo entidades del mismo grupo", value=False, help=f"Filtra para comparar únicamente con bancos del grupo: {grupo_cliente}")
    
    df_rank = df_enriched[df_enriched['Periodo'] == periodo_seleccionado][['Nombre de Entidad', ratio_rank, 'grupo_sistema']].copy()
    df_rank = df_rank.drop_duplicates(subset=['Nombre de Entidad'], keep='first')
    df_rank = df_rank.dropna(subset=[ratio_rank])
    
    if only_group_rank and grupo_cliente:
        df_rank = df_rank[df_rank['grupo_sistema'] == grupo_cliente]
    
    # Ordenar automáticamente según dirección de éxito
    # Lógica de éxito automática para el ranking
    higher_is_better = METRIC_SUCCESS_DIRECTION.get(ratio_rank, True)
    
    # Ordenar según la lógica de éxito
    df_rank = df_rank.sort_values(ratio_rank, ascending=not higher_is_better)
    
    # Gráfico de Ranking con Colores Premium
    df_rank = df_rank.reset_index(drop=True)
    df_rank['Ranking'] = range(1, len(df_rank) + 1)
    df_rank['Color'] = df_rank['Nombre de Entidad'].apply(lambda x: 'Seleccionado' if x == cliente_seleccionado else 'Otros')
    # Usar get_label para acortar nombres si son muy largos
    df_rank['Nombre Ranking'] = df_rank.apply(lambda r: f"#{r['Ranking']} {r['Nombre de Entidad']}", axis=1)
    color_map_rank = {'Seleccionado': PEER_COLORS["Seleccionado"], 'Otros': 'rgba(226, 232, 240, 0.4)'}
    
    fig_rank = px.bar(
        df_rank, 
        x=ratio_rank, 
        y='Nombre Ranking', 
        orientation='h',
        color='Color',
        color_discrete_map=color_map_rank,
        title=f"Ranking: {get_label(ratio_rank)}",
        height=max(400, len(df_rank) * 28 + 100),
        text_auto='.2f',
        category_orders={"Nombre Ranking": df_rank['Nombre Ranking'].tolist()},
        labels={ratio_rank: get_label(ratio_rank)}
    )
    
    fig_rank.update_layout(
        showlegend=False,
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    fig_rank.update_traces(
        marker_line_width=0,
        textposition='outside',
        cliponaxis=False
    )
    
    st.plotly_chart(fig_rank, use_container_width=True, config={'displayModeBar': False})

    st.markdown("---")
    st.markdown(f"### {render_svg('table', 24)} Detalle del Ranking ({periodo_seleccionado})", unsafe_allow_html=True)
    cols_rank = ["Ranking", "Nombre de Entidad", ratio_rank, "grupo_sistema"]
    st.dataframe(df_rank[cols_rank].style.format({ratio_rank: "{:.2f}"}), use_container_width=True, hide_index=True)

# Lógica de Exportación Final (al final del script para tener acceso a todas las variables)
if 'download_pdf' in st.session_state and st.session_state.download_pdf:
    # Se activó la exportación. Recolectamos todo.
    try:
        kpi_report_data = []
        for sec_name, ind_key in kpi_keys.items():
            val = mean_cliente.get(ind_key)
            ref_val = mean_ref.get(ind_key)
            val_str = (f"{float(val):.2f}%" if "%" in ind_key or "ROA" in ind_key else f"{float(val):.2f}") if not pd.isna(val) else "N/D"
            delta = round(float(val) - float(ref_val), 2) if ref_val is not None and not pd.isna(ref_val) and not pd.isna(val) else 0
            higher_is_better = METRIC_SUCCESS_DIRECTION.get(ind_key, True)
            status = "normal" if higher_is_better else "inverse"
            kpi_report_data.append({"label": sec_name, "value": val_str, "delta": f"{delta:+.2f}", "status": status})

        # Exportar imágenes temporales
        with tempfile.TemporaryDirectory() as tmp_dir:
            paths = {}
            if 'fig_bars' in locals():
                p = os.path.join(tmp_dir, "bars.png")
                fig_bars.write_image(p, scale=2)
                paths['bars'] = p
            if 'fig_radar' in locals():
                p = os.path.join(tmp_dir, "radar.png")
                fig_radar.write_image(p, scale=2)
                paths['radar'] = p
            if 'fig_line' in locals():
                p = os.path.join(tmp_dir, "trend.png")
                fig_line.write_image(p, scale=2)
                paths['trend'] = p
            
            trend_indicator_name = locals().get('ratio_trend')
            pdf_bytes = generate_pdf_report(cliente_seleccionado, periodo_seleccionado, ref_mercado, kpi_report_data, paths, trend_indicator=trend_indicator_name)
            
            st.sidebar.download_button(
                label="Descargar Mi Reporte PDF",
                data=pdf_bytes,
                file_name=f"Reporte_BCRA_{cliente_seleccionado}_{periodo_seleccionado}.pdf",
                mime="application/pdf",
                use_container_width=True
            )
            st.sidebar.success("¡Reporte listo para descargar!")
            st.session_state.download_pdf = False # Reset
    except Exception as e:
        st.sidebar.error(f"Error generando PDF: {e}")

# Activar el proceso cuando se pulsa el botón arriba
if st.session_state.get('trigger_pdf', False):
    st.session_state.download_pdf = True
    st.session_state.trigger_pdf = False
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown(f"### {render_svg('database', 18)} Origen de Datos", unsafe_allow_html=True)
st.sidebar.toggle("Modo Demo (Snapshot)", key="db_mode", help="Usa una copia estática de la base de datos para pruebas rápidas.")
st.sidebar.markdown("---")
st.sidebar.caption(f"Datos: BCRA API · Benchmark: {metrica_bench}")
