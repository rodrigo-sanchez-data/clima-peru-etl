import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path

PATH_PROCESSED = Path(__file__).parent.parent / 'data' / 'processed' / 'clima_peru.parquet'

st.set_page_config(
    page_title='Clima Perú',
    page_icon='🌤️',
    layout='wide'
)

@st.cache_data
def cargar_datos() -> pd.DataFrame:
    return pd.read_parquet(PATH_PROCESSED)

df = cargar_datos()

st.title('🌤️ Dashboard Clima Perú')
st.markdown('Pipeline ETL con datos de Open-Meteo — Lima, Cusco y Arequipa')

st.sidebar.header('Filtros')

ciudades = st.sidebar.multiselect(
    'Ciudades',
    options=df['ciudad'].cat.categories.tolist(),
    default=df['ciudad'].cat.categories.tolist()
)

fecha_min = df['fecha'].min()
fecha_max = df['fecha'].max()
rango_fechas = st.sidebar.date_input(
    'Rango de fechas',
    value=(fecha_min, fecha_max),
    min_value=fecha_min,
    max_value=fecha_max
)

df_filtrado = df[
    (df['ciudad'].isin(ciudades)) &
    (df['fecha'] >= pd.Timestamp(rango_fechas[0])) &
    (df['fecha'] <= pd.Timestamp(rango_fechas[1]))
]

col1, col2, col3 = st.columns(3)
col1.metric('🌡️ Temperatura promedio', f"{df_filtrado['temperature_2m'].mean():.1f} °C")
col2.metric('💧 Humedad promedio',      f"{df_filtrado['relativehumidity_2m'].mean():.1f} %")
col3.metric('📊 Registros',             f"{len(df_filtrado):,}")

st.divider()

st.subheader('Temperatura por hora')
df_g1 = (
    df_filtrado
    .groupby(['datetime', 'ciudad'], observed=True)['temperature_2m']
    .mean()
    .reset_index()
)
fig1 = px.line(
    df_g1,
    x='datetime',
    y='temperature_2m',
    color='ciudad',
    labels={'datetime': 'Fecha y hora', 'temperature_2m': 'Temperatura (°C)', 'ciudad': 'Ciudad'}
)
st.plotly_chart(fig1, width='stretch')

st.divider()

st.subheader('Comparación de métricas por ciudad')
df_g2 = (
    df_filtrado
    .groupby('ciudad', observed=True)[['temperature_2m', 'relativehumidity_2m', 'windspeed_10m']]
    .mean()
    .reset_index()
    .melt(id_vars='ciudad', var_name='metrica', value_name='promedio')
)
fig2 = px.bar(
    df_g2,
    x='ciudad',
    y='promedio',
    color='metrica',
    barmode='group',
    labels={'ciudad': 'Ciudad', 'promedio': 'Promedio', 'metrica': 'Métrica'}
)
st.plotly_chart(fig2, width='stretch')

st.divider()

st.subheader('Distribución de precipitación por ciudad')
df_g3 = (
    df_filtrado
    .groupby(['ciudad', 'nivel_precipitacion'], observed=True)
    .size()
    .reset_index(name='horas')
)
fig3 = px.bar(
    df_g3,
    x='nivel_precipitacion',
    y='horas',
    color='ciudad',
    barmode='group',
    labels={'nivel_precipitacion': 'Nivel', 'horas': 'Horas', 'ciudad': 'Ciudad'}
)
st.plotly_chart(fig3, width='stretch')