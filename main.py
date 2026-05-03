import pandas as pd
import logging
import sys
from config import (
    PATH_LOG, PATH_PROCESSED, CIUDADES, CAMPOS_CRITICOS, COLUMNAS_CLAVE, TEMPERATURA_MIN, 
    TEMPERATURA_MAX, CIUDADES_ESPERADAS, get_db_conn
)
from src.extract import extract_clima
from src.load import load_to_parquet, load_to_postgres
from src.transform import (
    estandarizar_columnas, validar_esquema, convertir_tipos, filtrar_nulos_criticos, filtrar_registros_invalidos,
    remover_duplicados, calcular_features, validar_resultado
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PATH_LOG),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def ejecutar_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    logger.info(f'[PIPELINE] Iniciando capa de transformación')
    return (
        df
        .pipe(estandarizar_columnas)
        .pipe(validar_esquema, CAMPOS_CRITICOS)
        .pipe(convertir_tipos)
        .pipe(filtrar_nulos_criticos, CAMPOS_CRITICOS)
        .pipe(filtrar_registros_invalidos, TEMPERATURA_MIN, TEMPERATURA_MAX)
        .pipe(remover_duplicados, COLUMNAS_CLAVE)
        .pipe(calcular_features)
        .pipe(validar_resultado, CIUDADES_ESPERADAS, CAMPOS_CRITICOS, COLUMNAS_CLAVE, TEMPERATURA_MIN, TEMPERATURA_MAX)
    )

def main() -> None:
    logger.info('[PIPELINE] === Iniciando PIPELINE Climas Peru ===')
    try:
        conn_string = get_db_conn()
        df = extract_clima(CIUDADES)
        n_antes = len(df)
        df_clean = ejecutar_pipeline(df)

        logger.info(f'[PIPELINE] Resumen | Reducción: {(n_antes - len(df_clean)) / n_antes:.2%}')
        load_to_parquet(df_clean, PATH_PROCESSED)
        load_to_postgres(df_clean, 'clima_peru', conn_string)

        logger.info('[PIPELINE] === Pipeline completado con éxito ===')

    except EnvironmentError as e:
        logger.critical(f'[CONFIG] Configuración DB invalida: {e}')
        sys.exit(1)
    except ValueError as e:
        logger.critical(f'[TRANSFORM] Error de validación: {e}')
        sys.exit(1)
    except Exception as e:
        logger.critical(f'[PIPELINE] Error inesperado: {e}', exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()