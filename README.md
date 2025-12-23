# 📊 AppsFlyer ETL Pipeline - YOY

Pipeline de datos ETL (Extract, Transform, Load) para procesar datos de atribución móvil desde **AppsFlyer Data Locker** hacia **Google BigQuery**, orquestado con **Apache Airflow**.

---

## 📋 Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Arquitectura](#arquitectura)
- [Servicios de Google Cloud Utilizados](#servicios-de-google-cloud-utilizados)
- [Dependencias](#dependencias)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuración](#configuración)
- [Flujo de Datos](#flujo-de-datos)
- [Tablas en BigQuery](#tablas-en-bigquery)
- [Funciones Principales](#funciones-principales)
- [Programación de Ejecución](#programación-de-ejecución)
- [Estructura del DAG](#estructura-del-dag)

---

## 🎯 Descripción General

Este proyecto implementa un pipeline ETL automatizado que:

1. **Extrae** datos comprimidos (`.gz`) desde AppsFlyer Data Locker almacenados en Google Cloud Storage
2. **Transforma** y consolida archivos CSV por hora en un único archivo por tipo de evento
3. **Carga** los datos transformados a BigQuery respetando el esquema de la tabla destino

El pipeline procesa datos de atribución para **dos plataformas móviles**:
- 📱 **Android** (`com.icbc.mobile.ds`)
- 🍎 **iOS** (`id1618263486`)

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AppsFlyer Data Locker                              │
│                    (Bucket: appsflyer-data-yoy)                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Archivos .gz por hora (h=0..23)
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow DAGs                                  │
│  ┌─────────────────────────┐    ┌─────────────────────────┐                │
│  │   Android.py            │    │   ios.py                │                │
│  │   - extract_and_prepare │    │   - extract_and_prepare │                │
│  │   - load_csv_to_bq x4   │    │   - load_csv_to_bq x4   │                │
│  └─────────────────────────┘    └─────────────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ CSV consolidados
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    GCS Bucket Intermedio (app_yoy)                          │
│     processed/{date}/t=installs/t=installs.csv                              │
│     processed/{date}/t=inapps/t=inapps.csv                                  │
│     processedios/{date}/t=installs/t=installs.csv                           │
│     ...                                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ Carga con esquema dinámico
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          BigQuery (Dataset: YOY)                             │
│     ┌──────────────────────────┐  ┌──────────────────────────┐             │
│     │  installs_android        │  │  installs_ios            │             │
│     │  inapps_android          │  │  inapps_ios              │             │
│     │  conversions_retarg...   │  │  conversions_retarg...   │             │
│     │  inapps_retargeting...   │  │  inapps_retargeting...   │             │
│     └──────────────────────────┘  └──────────────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ☁️ Servicios de Google Cloud Utilizados

| Servicio | Uso | Bucket/Dataset |
|----------|-----|----------------|
| **Google Cloud Storage** | Almacenamiento de datos crudos de AppsFlyer | `appsflyer-data-yoy` |
| **Google Cloud Storage** | Almacenamiento de datos procesados | `app_yoy` |
| **BigQuery** | Data Warehouse para análisis | Dataset: `YOY` |
| **Cloud Composer / Airflow** | Orquestación del pipeline | - |

**Proyecto GCP**: `icbc-395314`

---

## 📦 Dependencias

```txt
# requirements.txt
apache-airflow>=2.0.0
google-cloud-storage>=2.0.0
google-cloud-bigquery>=3.0.0
pandas>=1.5.0
pendulum>=2.1.0
```

### Librerías Utilizadas

| Librería | Versión | Propósito |
|----------|---------|-----------|
| `pandas` | >= 1.5.0 | Manipulación y transformación de datos CSV |
| `google-cloud-storage` | >= 2.0.0 | Interacción con GCS (lectura/escritura) |
| `google-cloud-bigquery` | >= 3.0.0 | Carga de datos a BigQuery |
| `pendulum` | >= 2.1.0 | Manejo de zonas horarias (America/Bogota) |
| `gzip` | stdlib | Descompresión de archivos .gz |
| `tempfile` | stdlib | Manejo de directorios temporales |
| `shutil` | stdlib | Operaciones de archivos |

---

## 📁 Estructura del Proyecto

```
Appsflyer/
├── Android.py          # DAG para datos de Android
├── ios.py              # DAG para datos de iOS
└── README.md           # Documentación del proyecto
```

---

## ⚙️ Configuración

### Variables de Configuración

| Variable | Valor Android | Valor iOS | Descripción |
|----------|---------------|-----------|-------------|
| `PROJECT_ID` | `icbc-395314` | `icbc-395314` | ID del proyecto en GCP |
| `SRC_BUCKET` | `appsflyer-data-yoy` | `appsflyer-data-yoy` | Bucket origen (Data Locker) |
| `DEST_BUCKET` | `app_yoy` | `app_yoy` | Bucket destino (procesados) |
| `DATASET_ID` | `YOY` | `YOY` | Dataset de BigQuery |
| `APP_ID` | `com.icbc.mobile.ds` | `id1618263486` | Identificador de la aplicación |
| `LOCAL_TZ` | `America/Bogota` | `America/Bogota` | Zona horaria local |

### Tipos de Eventos Procesados

```python
TARGET_SUBFOLDERS = [
    "t=installs",                    # Instalaciones nuevas
    "t=inapps",                      # Eventos in-app
    "t=conversions_retargeting",     # Conversiones de retargeting
    "t=inapps_retargeting",          # Eventos in-app de retargeting
]
```

---

## 🔄 Flujo de Datos

### 1. Extracción (`extract_and_prepare`)

```
AppsFlyer Data Locker (GCS)
│
├── appflyer-/datalocker-gcp/t=installs/dt=2025-01-15/h=0/app_id={APP_ID}/
│   ├── part-00000.csv.gz
│   ├── part-00001.csv.gz
│   └── ...
├── appflyer-/datalocker-gcp/t=installs/dt=2025-01-15/h=1/app_id={APP_ID}/
│   └── ...
└── ... (hasta h=23)
```

**Proceso:**
1. Itera sobre cada tipo de evento (`t=installs`, `t=inapps`, etc.)
2. Por cada hora del día (0-23), descarga todos los blobs `.gz`
3. Descomprime cada archivo `.gz` → `.csv`
4. Concatena todos los CSVs en un único DataFrame
5. Exporta el CSV consolidado al bucket destino

### 2. Transformación (`convert_to_bq_schema`)

Convierte los tipos de datos del DataFrame para coincidir con el esquema de BigQuery:

| Tipo BigQuery | Conversión Pandas |
|---------------|-------------------|
| `STRING` | `.astype(str)` |
| `INTEGER` | `pd.to_numeric(...).astype("Int64")` |
| `FLOAT` | `pd.to_numeric(...)` |
| `TIMESTAMP` | `pd.to_datetime(..., utc=True)` |
| `BOOLEAN` | Mapeo: `True/False/true/false/1/0` |

### 3. Carga (`load_csv_to_bq`)

```python
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",  # Añade datos (no reemplaza)
    schema=schema,                      # Esquema dinámico de la tabla
    source_format="CSV",
    skip_leading_rows=1,                # Ignora el header del CSV
)
```

---

## 📊 Tablas en BigQuery

### Dataset: `YOY`

#### Tablas Android

| Tabla | Evento AppsFlyer | Descripción |
|-------|------------------|-------------|
| `installs_android` | `t=installs` | Instalaciones de la app Android |
| `inapps_android` | `t=inapps` | Eventos in-app (compras, registros, etc.) |
| `conversions_retargeting_android` | `t=conversions_retargeting` | Conversiones de campañas de retargeting |
| `inapps_retargeting_android` | `t=inapps_retargeting` | Eventos in-app de usuarios retargeting |

#### Tablas iOS

| Tabla | Evento AppsFlyer | Descripción |
|-------|------------------|-------------|
| `installs_ios` | `t=installs` | Instalaciones de la app iOS |
| `inapps_ios` | `t=inapps` | Eventos in-app (compras, registros, etc.) |
| `conversions_retargeting_ios` | `t=conversions_retargeting` | Conversiones de campañas de retargeting |
| `inapps_retargeting_ios` | `t=inapps_retargeting` | Eventos in-app de usuarios retargeting |

---

## 🔧 Funciones Principales

### `_get_dates_from_context(context)`

```python
def _get_dates_from_context(context):
    """
    Devuelve run_date (Bogotá) y process_date (AAAA-MM-DD).
    
    Nota: Airflow utiliza logical_date que ya está configurado con -1 día,
    por lo que no se necesita restar un día adicional.
    """
```

**Retorna:**
- `run_date`: Fecha de ejecución en zona horaria local (string)
- `process_date`: Fecha a procesar en formato `YYYY-MM-DD`

---

### `extract_and_prepare(**context)`

**Propósito:** Descarga, descomprime y consolida archivos CSV desde AppsFlyer Data Locker.

**Flujo:**
1. Obtiene las fechas del contexto de Airflow
2. Crea conexión a GCS (cliente de storage)
3. Por cada subcarpeta de eventos:
   - Crea directorio temporal local
   - Descarga blobs de las 24 horas del día
   - Descomprime archivos `.gz`
   - Concatena CSVs con Pandas
   - Sube CSV consolidado al bucket destino
4. Limpia directorios temporales

**Logging:**
- Cantidad de blobs descargados por hora
- Filas procesadas por tipo de evento
- Total de filas procesadas

---

### `convert_to_bq_schema(df, bq_schema)`

**Propósito:** Convierte tipos de datos del DataFrame para coincidir con el esquema de BigQuery.

```python
def convert_to_bq_schema(df: pd.DataFrame, bq_schema: list[SchemaField]) -> pd.DataFrame:
    """
    Convierte columnas del DataFrame según el esquema de BigQuery.
    
    Args:
        df: DataFrame con datos crudos
        bq_schema: Lista de SchemaField de BigQuery
        
    Returns:
        DataFrame con tipos convertidos
    """
```

**Conversiones soportadas:**
- `STRING` → `str`
- `INTEGER` → `Int64` (nullable)
- `FLOAT` → `float64`
- `TIMESTAMP` → `datetime64[ns, UTC]`
- `BOOLEAN` → `boolean` (nullable)

---

### `load_csv_to_bq(sub, **context)`

**Propósito:** Carga el CSV procesado a BigQuery.

**Flujo:**
1. Construye ruta del CSV en GCS
2. Verifica existencia del blob
3. Descarga CSV a directorio temporal
4. Lee CSV con Pandas
5. Obtiene esquema de la tabla destino
6. Convierte tipos con `convert_to_bq_schema()`
7. Ejecuta job de carga a BigQuery (`WRITE_APPEND`)

**Manejo de errores:**
- Si el CSV no existe, lanza `AirflowSkipException` (no falla el DAG)

---

## ⏰ Programación de Ejecución

### DAG Android (`appsflyer_yoy_etl_android`)

| Parámetro | Valor |
|-----------|-------|
| Schedule | `0 8 * * *` (8:00 AM Bogotá) |
| Start Date | 1 de Junio 2025 |
| Catchup | `False` |
| Max Active Runs | 1 |
| Retries | 1 |
| Retry Delay | 10 minutos |

### DAG iOS (`appsflyer_yoy_etl_ios`)

| Parámetro | Valor |
|-----------|-------|
| Schedule | `0 1 * * *` (1:00 AM Bogotá) |
| Start Date | 1 de Junio 2025 |
| Catchup | `False` |
| Max Active Runs | 1 |
| Retries | 1 |
| Retry Delay | 10 minutos |

---

## 🔗 Estructura del DAG

```
                    ┌─────────────────────────┐
                    │   extract_and_prepare   │
                    │   (Descarga y prepara)  │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   load_installs_*       │
                    │   (Carga instalaciones) │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   load_inapps_*         │
                    │   (Carga eventos in-app)│
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   load_conversions_*    │
                    │   (Carga retargeting)   │
                    └───────────┬─────────────┘
                                │
                                ▼
                    ┌─────────────────────────┐
                    │   load_inapps_retarg_*  │
                    │   (Carga in-app retarg) │
                    └─────────────────────────┘
```

**Tipo de dependencia:** Secuencial (cada tarea espera que termine la anterior)

---

## 🏷️ Tags del DAG

```python
tags=["appsflyer", "etl", "yoy"]
```

---

## 📝 Notas Importantes

1. **Zona horaria:** Todo el procesamiento usa `America/Bogota` para consistencia
2. **Logical Date:** Airflow ya resta 1 día en el `logical_date`, no se necesita ajuste adicional
3. **Idempotencia:** El modo `WRITE_APPEND` añade datos; considerar deduplicación si se re-ejecuta
4. **Manejo de memoria:** Se usan directorios temporales que se limpian automáticamente
5. **Esquema dinámico:** El esquema se lee de BigQuery, no está hardcodeado

---

## 🤝 Contribuciones

Para contribuir al proyecto:
1. Fork el repositorio
2. Crea una rama (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -am 'Añade nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Abre un Pull Request

---

## 📄 Licencia

Este proyecto es propiedad de YOY. Todos los derechos reservados.

---

## 👥 Equipo

- **Owner:** data-eng
- **Mantenido por:** Equipo de Ingeniería de Datos

