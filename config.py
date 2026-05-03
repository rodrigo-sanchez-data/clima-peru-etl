import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent
PATH_LOG = BASE_DIR / 'clima_peru.log'
PATH_PROCESSED = BASE_DIR / 'data' / 'processed' / 'clima_peru.parquet'

CIUDADES = {
    'Lima':     {'lat': -12.05, 'lon': -77.04},
    'Cusco':    {'lat': -13.53, 'lon': -71.97},
    'Arequipa': {'lat': -16.41, 'lon': -71.54},
}

CAMPOS_CRITICOS = ['datetime', 'temperature_2m', 'precipitation', 'windspeed_10m', 'relativehumidity_2m', 'ciudad']
COLUMNAS_CLAVE = ['datetime', 'ciudad']
CIUDADES_ESPERADAS = {'Lima', 'Cusco', 'Arequipa'}
TEMPERATURA_MIN = -20
TEMPERATURA_MAX = 50

def get_db_conn() -> str:
    requeridos = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
    faltantes = [var for var in requeridos if not os.getenv(var)]
    if faltantes:
        raise EnvironmentError(f'Variables faltantes: {faltantes}')
    
    return (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )