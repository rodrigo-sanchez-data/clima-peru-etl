import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text, quoted_name

logger = logging.getLogger(__name__)

def load_to_parquet(df: pd.DataFrame, path: Path) -> None:
    logger.info(f'[LOAD] Iniciando carga de datos en: {path}')
    try:
        path.parent.mkdir(exist_ok=True, parents=True)
        df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
        logger.info(f'[LOAD] Carga exitosa | Archivo guardado con {len(df):,} registros')
    except Exception:
        logger.exception(f'[LOAD] Error en cargar los datos a parquet')
        raise
    
def load_to_postgres(df: pd.DataFrame, tabla: str, conn_string: str) -> None:
    tabla_segura = quoted_name(tabla, quote=True)
    engine = None
    try:
        engine = create_engine(conn_string, pool_pre_ping=True)
        logger.info(f'[LOAD] Iniciando conexion con la base de datos')
        
        with engine.begin() as conn:
            df.to_sql(tabla, conn, if_exists='append', index=False, method='multi', chunksize=10000)
        
        with engine.connect() as conn:
            total = conn.execute(
                text(f"SELECT COUNT(*) FROM {tabla_segura}",)
            ).scalar()
        
        logger.info(f'[LOAD] Carga exitosa | {len(df):,} registros insertados | Total en tabla: {total:,}')

    except Exception:
        logger.exception(f'[LOAD] Error en cargar en PostgreSQL - Tabla: {tabla}')
        raise
    finally:
        if engine:
            engine.dispose()