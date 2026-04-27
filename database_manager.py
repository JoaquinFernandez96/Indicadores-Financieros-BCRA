import sqlite3
import pandas as pd
import os

DB_PATH = "bcra_dashboard.db"

class DatabaseManager:
    def __init__(self, db_path=DB_PATH, read_only=False):
        self.read_only = read_only
        if read_only:
            # Open via URI in read-only mode — SQLite will reject any write attempt at the driver level
            uri = f"file:{db_path}?mode=ro"
            self.conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        else:
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
            self.create_tables()

    def create_tables(self):
        cursor = self.conn.cursor()
        
        # 1. Tabla de Observaciones (Lo que antes eran varios CSVs _long)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_entidad INTEGER,
                seccion TEXT,
                periodo TEXT,
                indicador TEXT,
                valor REAL,
                fuente TEXT,
                retrieved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(codigo_entidad, seccion, periodo, indicador)
            )
        ''')

        # 2. Tabla de Entidades (Metadata)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS entities (
                codigo_entidad INTEGER PRIMARY KEY,
                nombre TEXT,
                logo_url TEXT,
                grupo_sistema TEXT
            )
        ''')

        # 3. Tabla de Benchmarks
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS benchmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agrupacion TEXT,
                metrica TEXT,
                periodo TEXT,
                indicador TEXT,
                valor REAL,
                UNIQUE(agrupacion, metrica, periodo, indicador)
            )
        ''')

        # 4. Tabla de Grupos de Entidades (Mapping)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS entity_groups (
                codigo_entidad INTEGER PRIMARY KEY,
                grupo TEXT
            )
        ''')
        
        # 5. Asegurar columnas opcionales en 'observations' (Migración automática)
        try:
            cursor.execute("SELECT fuente FROM observations LIMIT 1")
        except sqlite3.OperationalError:
            print("Añadiendo columna 'fuente' a observations (migración)...")
            cursor.execute("ALTER TABLE observations ADD COLUMN fuente TEXT")

        try:
            cursor.execute("SELECT retrieved_at FROM observations LIMIT 1")
        except sqlite3.OperationalError:
            print("Añadiendo columna 'retrieved_at' a observations (migración)...")
            cursor.execute("ALTER TABLE observations ADD COLUMN retrieved_at DATETIME")

        # 6. Eliminar columna 'es_cliente' de entities si existe (migración)
        cols_info = cursor.execute("PRAGMA table_info(entities)").fetchall()
        col_names = [c[1] for c in cols_info]
        if 'es_cliente' in col_names:
            print("Migrando entities: eliminando columna 'es_cliente'...")
            cursor.execute('''
                CREATE TABLE entities_new (
                    codigo_entidad INTEGER PRIMARY KEY,
                    nombre TEXT,
                    logo_url TEXT,
                    grupo_sistema TEXT
                )
            ''')
            cursor.execute('''
                INSERT INTO entities_new (codigo_entidad, nombre, logo_url, grupo_sistema)
                SELECT codigo_entidad, nombre, logo_url, grupo_sistema FROM entities
            ''')
            cursor.execute('DROP TABLE entities')
            cursor.execute('ALTER TABLE entities_new RENAME TO entities')

        self.conn.commit()

    def save_observations(self, df):
        """Guarda un DataFrame de observaciones en la base de datos usando UPSERT."""
        if df.empty: return
        
        # Normalizar columnas
        df = df.rename(columns={
            'Codigo_Entidad': 'codigo_entidad', 'Seccion': 'seccion',
            'Periodo': 'periodo', 'Indicador': 'indicador', 'Valor': 'valor'
        })
        
        cols = ['codigo_entidad', 'seccion', 'periodo', 'indicador', 'valor', 'fuente']
        # Si la columna 'fuente' no existe en el DF, agregarla como None o vacío
        if 'fuente' not in df.columns:
            df['fuente'] = None
            
        df_to_save = df[cols].copy()
        
        # Para SQLite, una forma eficiente de hacer UPSERT con Pandas es 
        # insertar en una tabla temporal y luego mover con INSERT OR REPLACE
        cursor = self.conn.cursor()
        try:
            # Crear tabla temporal
            df_to_save.to_sql('temp_obs', self.conn, if_exists='replace', index=False)
            
            # Mover a la tabla principal con REPLACE
            cursor.execute('''
                INSERT OR REPLACE INTO observations (codigo_entidad, seccion, periodo, indicador, valor, fuente, retrieved_at)
                SELECT codigo_entidad, seccion, periodo, indicador, valor, fuente, CURRENT_TIMESTAMP FROM temp_obs
            ''')
            cursor.execute('DROP TABLE temp_obs')
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error en save_observations: {e}")

    def save_entities(self, df):
        """Guarda metadata de entidades asegurando que los grupos no se sobrescriban."""
        if df.empty: return
        
        # Columnas estándar
        cols = ['codigo_entidad', 'nombre', 'logo_url', 'grupo_sistema']
        for col in cols:
            if col not in df.columns:
                df[col] = None
        
        cursor = self.conn.cursor()
        try:
            df[cols].to_sql('temp_entities', self.conn, if_exists='replace', index=False)
            
            # 1. Insertar las que no existen
            cursor.execute('''
                INSERT INTO entities (codigo_entidad, nombre, logo_url, grupo_sistema)
                SELECT t.codigo_entidad, t.nombre, t.logo_url,
                       COALESCE(g.grupo, t.grupo_sistema, 'Otros')
                FROM temp_entities t
                LEFT JOIN entity_groups g ON t.codigo_entidad = g.codigo_entidad
                WHERE t.codigo_entidad NOT IN (SELECT codigo_entidad FROM entities)
            ''')
            
            # 2. Actualizar las existentes (respetando grupos)
            cursor.execute('''
                UPDATE entities
                SET
                    nombre = COALESCE((SELECT nombre FROM temp_entities WHERE codigo_entidad = entities.codigo_entidad), entities.nombre),
                    logo_url = COALESCE((SELECT logo_url FROM temp_entities WHERE codigo_entidad = entities.codigo_entidad), entities.logo_url),
                    grupo_sistema = COALESCE(
                        (SELECT grupo FROM entity_groups WHERE codigo_entidad = entities.codigo_entidad),
                        (SELECT grupo_sistema FROM temp_entities WHERE codigo_entidad = entities.codigo_entidad),
                        entities.grupo_sistema,
                        'Otros'
                    )
                WHERE codigo_entidad IN (SELECT codigo_entidad FROM temp_entities)
            ''')
            
            cursor.execute('DROP TABLE temp_entities')
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error en save_entities: {e}")

    def save_benchmarks(self, df):
        """Guarda benchmarks calculados."""
        if df.empty: return
        df.to_sql('benchmarks', self.conn, if_exists='replace', index=False)
        self.conn.commit()

    def save_entity_groups(self, df_groups):
        """Guarda el mapeo de grupos de entidades usando UPSERT."""
        if df_groups.empty: return
        cursor = self.conn.cursor()
        try:
            df_groups.to_sql('temp_groups', self.conn, if_exists='replace', index=False)
            cursor.execute('''
                INSERT OR REPLACE INTO entity_groups (codigo_entidad, grupo)
                SELECT codigo_entidad, grupo FROM temp_groups
            ''')
            cursor.execute('DROP TABLE temp_groups')
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Error en save_entity_groups: {e}")

    def get_long_data(self, seccion=None):
        """Retorna datos en formato long con nombres de entidades unidos."""
        query = """
            SELECT o.*, e.nombre 
            FROM observations o
            LEFT JOIN entities e ON o.codigo_entidad = e.codigo_entidad
        """
        if seccion:
            if isinstance(seccion, list):
                secs_str = "','".join(seccion)
                query += f" WHERE o.seccion IN ('{secs_str}')"
            else:
                query += f" WHERE o.seccion = '{seccion}'"
        
        return pd.read_sql(query, self.conn)

    def get_wide_data(self, seccion):
        """Retorna datos pivotados para una sección o lista de secciones."""
        df = self.get_long_data(seccion)
        if df.empty: return df
        
        return df.pivot_table(
            index=['codigo_entidad', 'nombre', 'periodo'],
            columns='indicador',
            values='valor',
            aggfunc='last' # Por si el mismo indicador se repite accidentalmente entre secciones
        ).reset_index()

if __name__ == "__main__":
    # Inicialización simple
    db = DatabaseManager()
    print("Database initialized at", DB_PATH)
