from __future__ import annotations
import os
import gzip
import shutil
import logging
import tempfile
from datetime import timedelta, datetime

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from google.cloud import storage, bigquery
from google.cloud.bigquery import SchemaField

PROJECT_ID        = "icbc-395314"
SRC_BUCKET        = "appsflyer-data-yoy"
DEST_BUCKET       = "app_yoy"
DATASET_ID        = "YOY"
APP_ID            = "com.icbc.mobile.ds"         # Ajusta si iOS
LOCAL_TZ          = pendulum.timezone("America/Bogota")

TARGET_SUBFOLDERS = [
    "t=installs",
    "t=inapps",
    "t=conversions_retargeting",
    "t=inapps_retargeting",
]
TABLE_NAMES = {
    "t=installs":                "installs_android",
    "t=inapps":                  "inapps_android",
    "t=conversions_retargeting": "conversions_retargeting_android",
    "t=inapps_retargeting":      "inapps_retargeting_android",
}

def _get_dates_from_context(context):
    """
    Devuelve run_date (BogotÃƒÆ’Ã‚Â¡) y process_date (run_date-1, AAAA-MM-DD).
    """
    exec_dt_local = context["execution_date"].in_timezone(LOCAL_TZ)
    run_date      = exec_dt_local.date()                       # datetime.date
    #process_date  = (exec_dt_local - timedelta(days=1)).strftime("%Y-%m-%d")
    process_date  = (exec_dt_local).strftime("%Y-%m-%d")       # Se quita el - timedelta(days=1) porq airflow ya tiene un logicaltime de -1 dia 
    #process_date = "2025-07-11"
    return str(run_date), process_date


# ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Extract & prepare ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
def extract_and_prepare(**context):
    log = logging.getLogger("airflow.task")
    log.info(">>> Iniciando extract_and_prepare")

    run_date, process_date = _get_dates_from_context(context)
    log.info("Run date (local): %s | Process date: %s", run_date, process_date)

    client     = storage.Client(project=PROJECT_ID)
    src_bucket = client.bucket(SRC_BUCKET)
    dst_bucket = client.bucket(DEST_BUCKET)

    with tempfile.TemporaryDirectory(prefix="af_dl_") as workdir:
        total_rows = 0

        for sub in TARGET_SUBFOLDERS:
            safe_subfolder = sub.replace("=", "_")
            local_dir = os.path.join(workdir, safe_subfolder)
            os.makedirs(local_dir, exist_ok=True)
            row_count_sub = 0

            # Descargar y descomprimir por hora
            for hr in range(24):
                prefix = (
                    f"appflyer-/datalocker-gcp/{sub}/"
                    f"dt={process_date}/h={hr}/app_id={APP_ID}/"
                )
                blobs = list(src_bucket.list_blobs(prefix=prefix))
                log.info("Descargando %s blobs de %s | hora %02d", len(blobs), sub, hr)
                for blob in blobs:
                    filename = os.path.basename(blob.name)
                    local_gz = os.path.join(local_dir, filename)
                    blob.download_to_filename(local_gz)
                    with gzip.open(local_gz, "rb") as f_in, open(local_gz[:-3], "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    os.remove(local_gz)

            csv_files = [
                os.path.join(local_dir, f)
                for f in os.listdir(local_dir)
                if f.lower().endswith(".csv")
            ]
            if not csv_files:
                log.warning("Sin CSV en %s para %s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“ se omite", sub, process_date)
                shutil.rmtree(local_dir, ignore_errors=True)
                continue

            frames = [pd.read_csv(f, low_memory=False) for f in csv_files]
            df_final = pd.concat(frames, ignore_index=True)
            row_count_sub += len(df_final)
            total_rows += row_count_sub

            local_csv = os.path.join(local_dir, f"{safe_subfolder}.csv")
            df_final.to_csv(local_csv, index=False)

            gcs_path = f"processed/{process_date}/{sub}/{sub}.csv"
            dst_bucket.blob(gcs_path).upload_from_filename(local_csv)
            log.info("ÃƒÂ¢Ã¢â‚¬â€œÃ‚Âº %s: %s filas ÃƒÂ¢Ã¢â‚¬ Ã¢â‚¬â„¢ gs://%s/%s", sub, row_count_sub, DEST_BUCKET, gcs_path)
            shutil.rmtree(local_dir, ignore_errors=True)

        log.info("Proceso AppsFlyer %s completado ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ Total filas: %s", process_date, total_rows)


# ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ ConversiÃƒÆ’Ã‚Â³n de tipos ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
def convert_to_bq_schema(df: pd.DataFrame, bq_schema: list[SchemaField]) -> pd.DataFrame:
    for field in bq_schema:
        if field.name not in df.columns:
            continue
        col = df[field.name]
        if field.field_type == "STRING":
            df[field.name] = col.astype(str)
        elif field.field_type == "INTEGER":
            df[field.name] = pd.to_numeric(col, errors="coerce").astype("Int64")
        elif field.field_type == "FLOAT":
            df[field.name] = pd.to_numeric(col, errors="coerce")
        elif field.field_type == "TIMESTAMP":
            df[field.name] = pd.to_datetime(col, errors="coerce", utc=True)
        elif field.field_type == "BOOLEAN":
            df[field.name] = col.astype(str).map(
                {"True": True, "False": False, "true": True, "false": False, "1": True, "0": False}
            ).astype("boolean")
    return df


# ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ Load a BigQuery ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬ÃƒÂ¢Ã¢â‚¬ÂÃ¢â€šÂ¬
def load_csv_to_bq(sub: str, **context):
    log = logging.getLogger("airflow.task")
    run_date, process_date = _get_dates_from_context(context)
    log.info("Cargando BigQuery ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¢ subcarpeta: %s | run_date: %s | process_date: %s",
             sub, run_date, process_date)

    gcs_path = f"processed/{process_date}/{sub}/{sub}.csv"
    client_gcs = storage.Client(project=PROJECT_ID)
    bucket = client_gcs.bucket(DEST_BUCKET)
    blob = bucket.blob(gcs_path)

    if not blob.exists():
        log.warning("No existe %s ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Å“ se salta carga", gcs_path)
        raise AirflowSkipException("CSV inexistente")

    with tempfile.TemporaryDirectory(prefix="bq_dl_") as tmpdir:
        local_csv = os.path.join(tmpdir, f"{sub.replace('=', '_')}.csv")
        blob.download_to_filename(local_csv)

        df = pd.read_csv(local_csv, low_memory=False)
        client_bq = bigquery.Client(project=PROJECT_ID)
        table_ref = client_bq.dataset(DATASET_ID).table(TABLE_NAMES[sub])
        table = client_bq.get_table(table_ref)
        schema = [f for f in table.schema if f.name in df.columns]

        df = convert_to_bq_schema(df, schema)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema=schema,
            source_format="CSV",
            skip_leading_rows=1,
        )

        client_bq.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
        log.info("Carga completada en %s.%s.%s", PROJECT_ID, DATASET_ID, TABLE_NAMES[sub])

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="appsflyer_yoy_etl_android",
    default_args=default_args,
    description="ETL AppsFlyer tablas Android",
    schedule_interval="0 8 * * *",                   
    start_date=datetime(2025, 6, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["appsflyer", "etl", "yoy"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_and_prepare",
        python_callable=extract_and_prepare,
    )

    prev = t_extract
    for sub in TARGET_SUBFOLDERS:
        load = PythonOperator(
            task_id=f"load_{TABLE_NAMES[sub]}",
            python_callable=load_csv_to_bq,
            op_kwargs={"sub": sub},
        )
        prev >> load
        prev = load