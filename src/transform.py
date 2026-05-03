import pandas as pd
import logging

logger = logging.getLogger(__name__)

def estandarizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_', regex=False)
        .str.replace(r'[^\w]', '_', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.strip('_')
    )
    logger.info('[TRANSFORM] Nombres de columnas estandarizadas')
    return df

def validar_esquema(df: pd.DataFrame, campos_criticos: list[str]) -> pd.DataFrame:
    faltantes = [col for col in campos_criticos if col not in df.columns]
    if faltantes:
        logger.error(f'[TRANSFORM] Faltan columnas requeridas: {faltantes}')
        raise ValueError(f'Faltan columnas requeridas: {faltantes}')
    logger.info('[TRANSFORM] Esquema validado con éxito')
    return df

def convertir_tipos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns = {'time': 'datetime'})
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    nulos_fecha = df['datetime'].isna().sum()
    if nulos_fecha > 0:
        logger.warning(f'[TRANSFORM] {nulos_fecha:,} fechas invalidas')    

    df['ciudad'] = df['ciudad'].astype('category')
    logger.info('[TRANSFORM] Tipos de datos convertidos')
    return df

def filtrar_nulos_criticos(df: pd.DataFrame, campos_criticos: list[str]) -> pd.DataFrame:
    n_antes = len(df)

    filas_con_nulos = df[campos_criticos].isna().any(axis=1).sum()
    if filas_con_nulos > 0:
        logger.warning(f'[TRANSFORM] Eliminadas {filas_con_nulos:,} filas por nulos en campos criticos')
        df = df.dropna(subset=campos_criticos)
    else:
        logger.info(f'[TRANSFORM] Validación exitosa: Cero nulos en campos criticos')
    logger.info(f'[TRANSFORM] Filtrar Nulos Criticos | Entrantes: {n_antes:,} -> Salientes: {len(df):,}')
    return df

def filtrar_registros_invalidos(df: pd.DataFrame, temperatura_min: float, temperatura_max: float) -> pd.DataFrame:
    n_antes = len(df)

    mask_temperatura = df['temperature_2m'].between(temperatura_min,temperatura_max)
    mask_precipitacion = df['precipitation'] >= 0
    mask_viento = df['windspeed_10m'] >= 0
    mask_humedad = df['relativehumidity_2m'].between(0,100)

    inval_temperatura = (~mask_temperatura).sum()
    inval_precipitacion = (~mask_precipitacion).sum()
    inval_viento = (~mask_viento).sum()
    inval_humedad = (~mask_humedad).sum()

    if inval_temperatura > 0:
        logger.warning(f'[TRANSFORM] temperature_2m: {inval_temperatura:,} registros fuera del rango [{temperatura_min} - {temperatura_max}]')
    if inval_precipitacion > 0:
        logger.warning(f'[TRANSFORM] precipitation: {inval_precipitacion:,} registros negativos')
    if inval_viento > 0:
        logger.warning(f'[TRANSFORM] windspeed_10m: {inval_viento:,} registros negativos')
    if inval_humedad > 0:
        logger.warning(f'[TRANSFORM] relativehumidity_2m: {inval_humedad:,} registros fuera del rango [0 - 100]%')

    df = df[mask_humedad & mask_precipitacion & mask_temperatura & mask_viento]

    logger.info(f'[TRANSFORM] Filtrar Registros Invalidos | Entrantes: {n_antes:,} -> Salientes: {len(df):,}')
    return df

def remover_duplicados(df: pd.DataFrame, columnas_claves: list[str]) -> pd.DataFrame:
    n_antes = len(df)

    n_duplicados = df.duplicated(subset=columnas_claves).sum()
    if n_duplicados > 0:
        logger.warning(f'[TRANSFORM] Eliminadas {n_duplicados:,} filas duplicadas')
        df = df.drop_duplicates(subset=columnas_claves, keep='first')
    else:
        logger.info(f'[TRANSFORM] Validación exitosa: Cero registros duplicados')
    logger.info(f'[TRANSFORM] Remover Duplicados| Entrantes: {n_antes:,} -> Salientes: {len(df):,}')
    return df

def calcular_features(df: pd.DataFrame) -> pd.DataFrame:

    df['sensacion_termica'] = (
        df['temperature_2m'] - (0.4 * (df['temperature_2m'] - 10) * (1 - (df['relativehumidity_2m'] / 100))) - (df['windspeed_10m'] * 0.15)
    ).astype('float64')

    df['fecha'] = df['datetime'].dt.normalize()
    df['hora'] = df['datetime'].dt.hour

    bins_pre = [-0.01, 0.0, 2.5, 10.0, 999]
    labels_pre = ['sin lluvia', 'lluvia leve', 'lluvia moderada', 'lluvia fuerte']
    df['nivel_precipitacion'] = pd.cut(df['precipitation'], bins=bins_pre, labels=labels_pre)
    df['nivel_precipitacion'] = df['nivel_precipitacion'].astype('category')

    bins_vie = [-0.01, 5, 20, 40, 999]
    labels_vie = ['calma', 'brisa', 'viento moderado', 'viento fuerte']
    df['nivel_viento'] = pd.cut(df['windspeed_10m'], bins=bins_vie, labels=labels_vie)
    df['nivel_viento'] = df['nivel_viento'].astype('category')

    logger.info('[TRANSFORM] Features calculadas: fecha, hora, sensacion_termica, nivel_precipitacion, nivel_viento')
    return df

def validar_resultado(
    df: pd.DataFrame,
    ciudades_esperadas: set,
    campos_criticos: list[str],
    columnas_claves: list[str],
    temperatura_min: float,
    temperatura_max: float

) -> pd.DataFrame:
    checks = {
        'df_no_vacio': len(df) > 0,
        'sin_nulos_criticos': df[campos_criticos].notna().all().all(),
        'sin_duplicados': not df[columnas_claves].duplicated().any(),
        'temperatura_valida': (df['temperature_2m'].between(temperatura_min,temperatura_max)).all(),
        'precipitacion_valida': (df['precipitation'] >= 0).all(),
        'viento_valida': (df['windspeed_10m'] >= 0).all(),
        'humedad_valida': (df['relativehumidity_2m'].between(0,100)).all(),
        'ciudades_esperadas': set(df['ciudad'].unique()) == ciudades_esperadas,
        'features_calculadas': all(col in df.columns for col in ['fecha', 'hora', 'sensacion_termica', 'nivel_precipitacion', 'nivel_viento'])
    }

    fallos = [nombre for nombre, resultado in checks.items() if not resultado]
    if fallos:
        logger.error(f'[TRANSFORM] Validacion final fallida en los checks: {fallos}')
        raise ValueError(f'Validacion final fallida en los checks: {fallos}')

    logger.info(f'[TRANSFORM] Validación final OK | Registros limpios: {len(df):,}')
    return df