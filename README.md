# Clima Perú — Pipeline ETL + Dashboard

Pipeline ETL que extrae datos climáticos de ciudades peruanas via API pública, los transforma y carga a PostgreSQL, con dashboard interactivo desplegado en Streamlit Cloud.

Construido como proyecto de portafolio en mi transición hacia Data Engineering.

🔗 [Ver dashboard en vivo](https://clima-peru-etl-aodjyzhkcu2c2m7msuxrux.streamlit.app/)

---

## ¿Qué hace este proyecto?

Consulta la API gratuita de Open-Meteo para obtener datos climáticos horarios de Lima, Cusco y Arequipa (últimos 7 días) y los convierte en un dataset limpio y analizable. El pipeline recorre estas etapas en orden:

- Extrae datos horarios de temperatura, precipitación, viento y humedad via API REST con reintentos automáticos
- Estandariza nombres de columnas y tipos de datos
- Filtra registros con nulos en campos críticos y valores fuera de rango físico
- Deduplica por combinación ciudad + datetime
- Calcula features derivadas: sensación térmica, nivel de precipitación y nivel de viento
- Valida 9 checks de calidad antes de cargar el resultado
- Carga el resultado a PostgreSQL y Parquet local
- Expone los datos en un dashboard interactivo con 3 visualizaciones

**Resultado:** Datos climáticos horarios limpios de 3 ciudades peruanas listos para análisis

---

## Arquitectura
```
API Open-Meteo
(Lima, Cusco, Arequipa)
│
▼
[ Extract ]  ← Session + HTTPAdapter + Retry
│
▼
[ estandarizar_columnas    ]
[ validar_esquema          ]  ← rename time → datetime
[ convertir_tipos          ]
[ filtrar_nulos_criticos   ]
[ filtrar_registros_invalidos ]
[ remover_duplicados       ]
[ calcular_features        ]  ← sensacion_termica, niveles
[ validar_resultado        ]  ← 9 checks de calidad final
│
┌────┴────┐
▼         ▼
Parquet   PostgreSQL
│
▼
Dashboard Streamlit
```

Cada etapa es una función pura que recibe y retorna un DataFrame. El encadenamiento usa `.pipe()` de pandas para mantener el flujo legible.

---

## Estructura del proyecto
```
clima-peru-etl/
├── src/
│   ├── extract.py        # Extracción desde API Open-Meteo
│   ├── transform.py      # Transformaciones del pipeline
│   ├── load.py           # Carga a Parquet y PostgreSQL
│   └── dashboard.py      # Dashboard Streamlit
├── data/
│   ├── raw/              # Datos crudos (no versionados)
│   └── processed/        # Resultado procesado
├── config.py             # Constantes, rutas y configuración de BD
├── main.py               # Orquestador del pipeline
├── .env.example          # Plantilla de variables de entorno
├── requirements.txt
└── .gitignore
```

> `clima_peru.log` se genera automáticamente al ejecutar el pipeline.

---

## Tecnologías

| Herramienta | Uso |
|---|---|
| Python | Lenguaje principal |
| Pandas | Transformación y limpieza |
| Requests | Consumo de API REST con reintentos |
| PyArrow | Lectura/escritura Parquet |
| SQLAlchemy | Conexión a PostgreSQL |
| Psycopg2 | Driver PostgreSQL |
| Streamlit | Dashboard interactivo |
| Plotly | Visualizaciones |
| Python-dotenv | Variables de entorno |
| PostgreSQL | Base de datos destino |

---

## Cómo ejecutarlo

### 1. Clonar el repositorio

```bash
git clone https://github.com/rodrigo-sanchez-data/clima-peru-etl.git
cd clima-peru-etl
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
source venv/bin/activate     # Mac/Linux
venv\Scripts\activate        # Windows CMD
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

```bash
cp .env.example .env        # Mac/Linux
copy .env.example .env      # Windows CMD
```

Completar `.env` con las credenciales de tu base de datos:
```
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taxi_nyc
```
> La API de Open-Meteo es gratuita y no requiere API key.

### 4. Ejecutar el pipeline

```bash
python main.py
```

El pipeline extrae los datos de la API, los transforma y carga el resultado en `data/processed/clima_peru.parquet` y PostgreSQL.

### 5. Ejecutar el dashboard localmente

```bash
streamlit run src/dashboard.py
```

---

## Decisiones técnicas que tomé

**`Session + HTTPAdapter + Retry` en lugar de `requests.get` directo**
Las APIs públicas fallan ocasionalmente por timeouts o errores de servidor. Implementé reintentos automáticos con backoff exponencial para los códigos 500, 502, 503 y 504. Un `requests.get` directo fallaría silenciosamente en producción.

**`try/except` por ciudad en el loop de extracción**
Si una ciudad falla, el pipeline continúa con las demás y loggea el error. Sin este manejo, un timeout en Cusco mataría la extracción completa de Lima y Arequipa también.

**`how='left'` implícito — extracción independiente por ciudad**
Cada ciudad se extrae por separado y se concatena al final. Si una falla y se omite, `validar_resultado` lo detecta via el check `ciudades_esperadas` y lanza error antes de cargar data incompleta a PostgreSQL.

**Rename `time → datetime` en `convertir_tipos`, no en `extract`**
La API retorna la columna como `time`. El rename es una decisión semántica del pipeline, no de la fuente — por eso vive en transform. Si la API cambia el nombre del campo, hay un solo lugar donde corregirlo.

**`quoted_name` para el nombre de tabla en PostgreSQL**
El COUNT de verificación post-insert usa `quoted_name` de SQLAlchemy en lugar de un f-string directo. Los bind parameters no funcionan para identificadores SQL — `quoted_name` escapa el nombre correctamente y evita SQL injection.

---

## Resultados

| Métrica | Valor |
|---|---|
| Ciudades | Lima, Cusco, Arequipa |
| Granularidad | Horaria |
| Ventana de datos | Últimos 7 días |
| Checks de calidad | 9 / 9 OK |
| Destinos de carga | Parquet local + PostgreSQL |
| Dashboard | Streamlit Cloud |

---

## Notas

- El `.env` no está versionado. Usar `.env.example` como plantilla.
- El log de cada ejecución se guarda en `clima_peru.log`.
- El dashboard en Streamlit Cloud lee desde el Parquet versionado en el repo.