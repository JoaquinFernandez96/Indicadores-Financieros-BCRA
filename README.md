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
├── scraper.py              # Orquestador del scraping
├── data_processing.py      # Cruce, normalización y enriquecimiento de datos
├── database_manager.py     # Capa de acceso a SQLite
├── report_engine.py        # Generación de reportes PDF
├── scrapers/
│   ├── api_client.py       # Cliente para la API de entidades del BCRA
│   └── html_parser.py      # Parser de tablas HTML (EECC y deudores)
├── static/icons/           # Íconos SVG para el dashboard
├── logos/                  # Logos de entidades financieras
└── requirements.txt
```

---

## Instalación

**Requisitos:** Python 3.9+

```bash
# Clonar el repositorio
git clone https://github.com/JoaquinFernandez96/Indicadores-Financieros-BCRA.git
cd Indicadores-Financieros-BCRA

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## Uso

### 1. Ejecutar el pipeline de datos

Descarga y procesa todos los datos del BCRA. Genera `bcra_dashboard.db`.

```bash
python main.py
```

### 2. Lanzar el dashboard

```bash
streamlit run app.py
```

El dashboard queda disponible en `http://localhost:8501`.

---

## Fuentes de datos

| Dato | Fuente |
|------|--------|
| Indicadores del sistema financiero | BCRA — API pública |
| Estados contables por entidad | BCRA — HTML scraping |
| Situación de deudores | BCRA — HTML scraping |

---

## Funcionalidades

- Visualización de indicadores por entidad y por sistema total
- Comparativa entre entidades (benchmarks)
- Filtros por período, tipo de entidad y grupo
- Exportación de reportes en PDF
- Base de datos local con actualización incremental

---

## Dependencias principales

| Librería | Uso |
|----------|-----|
| `streamlit` | Dashboard web |
| `plotly` | Gráficos interactivos |
| `pandas` | Procesamiento de datos |
| `requests` / `beautifulsoup4` | Scraping |
| `PyPDF2` | Lectura de PDFs del BCRA |

---

## Notas

- La base de datos (`bcra_dashboard.db`) no está incluida en el repositorio. Se genera al correr `main.py`.
- El scraping respeta los tiempos de respuesta del servidor del BCRA. La primera ejecución puede demorar varios minutos dependiendo de la cantidad de entidades.
