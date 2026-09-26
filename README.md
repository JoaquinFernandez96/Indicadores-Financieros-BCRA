# Indicadores Financieros BCRA

Dashboard interactivo para visualizar y analizar indicadores financieros de entidades bancarias publicados por el Banco Central de la República Argentina (BCRA).

---

## Descripción

La aplicación extrae automáticamente datos del sitio del BCRA (estados contables, situación de deudores e indicadores del sistema financiero), los consolida en una base de datos SQLite local y los presenta en un dashboard web con filtros, comparativas y exportación a PDF.

---

## Estructura del proyecto

```
├── app.py                  # Dashboard Streamlit (punto de entrada visual)
├── main.py                 # Pipeline de extracción y procesamiento
├── scraper.py              # Orquestador asíncrono del scraping
├── data_processing.py      # Cruce, normalización y enriquecimiento de datos
├── database_manager.py     # Capa de acceso a SQLite
├── report_engine.py        # Generación de reportes PDF
├── scrapers/
│   ├── api_client.py       # Cliente para las APIs REST del BCRA
│   ├── async_fetcher.py    # Cliente HTTP asíncrono con reintentos y HTTP/2
│   └── html_parser.py      # Parseadores de compatibilidad histórica
├── tests/                  # Suite de pruebas unitarias y de integración
├── static/icons/           # Íconos SVG para el dashboard
├── logos/                  # Logos de entidades financieras
├── requirements.txt        # Dependencias de producción
└── requirements-dev.txt    # Dependencias de desarrollo y testing
```

---

## Instalación

**Requisitos:** Python 3.10+

```bash
# Clonar el repositorio
git clone https://github.com/JoaquinFernandez96/Indicadores-Financieros-BCRA.git
cd Indicadores-Financieros-BCRA

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependencias de producción
pip install -r requirements.txt

# (Opcional) Instalar dependencias de desarrollo y tests
pip install -r requirements-dev.txt
```

---

## Uso

### 1. Ejecutar el pipeline de datos

Descarga de forma asíncrona y concurrente todos los datos del BCRA y genera `bcra_dashboard.db`.

```bash
python main.py
```

### 2. Lanzar el dashboard

```bash
streamlit run app.py
```

El dashboard queda disponible en `http://localhost:8501`.

### 3. Ejecutar las pruebas

```bash
pytest
```

---

## Fuentes de datos

| Dato | Fuente |
|------|--------|
| Listado de entidades | BCRA — API REST pública (`/api/endpoints/entidades-financieras.php?action=list`) |
| Indicadores del sistema financiero | BCRA — API REST pública (`/api/endpoints/indicadores-economicos.php`) |
| Estados contables por entidad | BCRA — API REST pública (`/api/endpoints/entidades-financieras-estados-contables.php`) |
| Situación de deudores | BCRA — API REST pública (`/api/endpoints/entidades-financieras-situacion-deudores.php`) |

---

## Funcionalidades

- Visualización de indicadores por entidad y por sistema total
- Comparativa entre entidades (benchmarks)
- Filtros por período, tipo de entidad y grupo
- Exportación de reportes en PDF
- Base de datos local con actualización incremental y concurrencia asíncrona

---

## Dependencias principales

| Librería | Uso |
|----------|-----|
| `streamlit` | Dashboard web interactivo |
| `plotly` | Gráficos interactivos y radar charts |
| `pandas` | Procesamiento y cruce de datos |
| `httpx[http2]` | Descarga asíncrona concurrente con HTTP/2 |
| `tenacity` | Reintentos automáticos con backoff |
| `fpdf2` | Generación de reportes ejecutivos en PDF |
| `kaleido` | Exportación de gráficos Plotly a imágenes estáticas |

---

## Notas

- Las bases de datos (`bcra_dashboard.db` y `bcra_dashboard_demo.db`) están incluidas en el repositorio. `bcra_dashboard.db` se actualiza al correr `main.py` con datos frescos; `bcra_dashboard_demo.db` es un snapshot estático para pruebas rápidas sin necesidad de ejecutar el scraper.
- La extracción utiliza concurrencia asíncrona (`httpx` + `asyncio`) optimizada con límites de tasa para respetar los servidores del BCRA.
