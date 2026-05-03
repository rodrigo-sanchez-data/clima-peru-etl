import pandas as pd
import logging
import requests 
from requests.exceptions import Timeout, ConnectionError, HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

def _crear_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def _llamar_api(session: requests.Session, ciudad: str, config: dict) -> dict:
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude':  config['lat'],
        'longitude': config['lon'],
        'hourly':    'temperature_2m,precipitation,windspeed_10m,relativehumidity_2m',
        'past_days': 7,
        'timezone':  'America/Lima'
    }
    logger.info(f'[EXTRACT] Consultando clima de: {ciudad}')

    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    except Timeout:
        logger.error(f'[EXTRACT] Timeout - {ciudad}')
        raise
    except ConnectionError:
        logger.error(f'[EXTRACT] Sin conexión - {ciudad}')
        raise
    except HTTPError as e:
        logger.error(f'[EXTRACT] HTTP {response.status_code}: {e}')
        raise
    except ValueError:
        logger.error(f'[EXTRACT] Respuesta JSON inválido - {ciudad}')
        raise

def extract_clima(ciudades: dict) -> pd.DataFrame:

    session = _crear_session()
    dfs = []
    for ciudad, config in ciudades.items():
        try:
            data = _llamar_api(session, ciudad, config)
            df = pd.DataFrame(data['hourly'])
            df['ciudad'] = ciudad
            dfs.append(df)
            logger.info(f'[EXTRACT] {ciudad} OK | {len(df):,} registros')

        except Exception as e:
            logger.error(f'[EXTRACT] Falló {ciudad}, se omite: {e}')
            continue
        finally:
            session.close()
        
    if not dfs:
        logger.error('[EXTRACT] No se obtuvo datos de ninguna ciudad')
        return pd.DataFrame()

    df_total = pd.concat(dfs, ignore_index=True)
    logger.info(f'[EXTRACT] OK | {len(ciudades):,} ciudades | {len(df_total):,} registros totales')

    return df_total