from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sqlite3
import uuid

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Rutas internas en el contenedor de Airflow
DATA_DIR = Path("/opt/airflow/data")
OUT_DIR = Path("/opt/airflow/outputs")
STATE_DIR = Path("/opt/airflow/state")
TMP_DIR = STATE_DIR / "tmp"
DB_PATH = STATE_DIR / "etl.db"
LOG_PATH = STATE_DIR / "etl.log"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_alumnos_dag",
    description="ETL alumnos (CSV+JSON+XML) con monitoreo en SQLite y logs JSONL",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",        # pon None si prefieres manual
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "alumnos"],
) as dag:

    # ------------------------------------------------------------------
    # Helpers de persistencia (tablas de datos) — quedan aquí para
    # no complicar el utils, y cumplir el entregable de 'tablas cargadas'
    # ------------------------------------------------------------------
    def _save_all_tables_to_sqlite(df_a: pd.DataFrame,
                                   df_c: pd.DataFrame,
                                   df_m: pd.DataFrame,
                                   df_f: pd.DataFrame,
                                   db_path: str) -> None:
        """Crea/replace tablas de datos en SQLite: alumnos, calificaciones, matriculas, dataset_final."""
        with sqlite3.connect(db_path) as conn:
            df_a.to_sql("alumnos", conn, if_exists="replace", index=False)
            df_c.to_sql("calificaciones", conn, if_exists="replace", index=False)
            df_m.to_sql("matriculas", conn, if_exists="replace", index=False)
            df_f.to_sql("dataset_final", conn, if_exists="replace", index=False)
            conn.commit()

    # ------------------------------------------------------------------
    # Tareas del DAG
    # ------------------------------------------------------------------
    def start_run(**context):
        """Genera run_id, registra inicio y asegura carpeta/DB."""
        from etl_utils import sqlite_init

        run_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        context["ti"].xcom_push(key="run_id", value=run_id)
        context["ti"].xcom_push(key="start_ts", value=datetime.utcnow().isoformat())

        TMP_DIR.mkdir(parents=True, exist_ok=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        STATE_DIR.mkdir(parents=True, exist_ok=True)

        # Asegura la tabla de monitoreo
        sqlite_init(str(DB_PATH))
        return run_id

    def extract(**context):
        """Lee CSV/JSON/XML y guarda temporales en Parquet."""
        from etl_utils import extract_sources

        sources = extract_sources(str(DATA_DIR))

        df_a = sources["csv"]
        df_c = sources["json"]
        df_m = sources["xml"]

        # Conteos para monitor
        ti = context["ti"]
        ti.xcom_push(key="n_csv", value=int(len(df_a)))
        ti.xcom_push(key="n_json", value=int(len(df_c)))
        ti.xcom_push(key="n_xml", value=int(len(df_m)))

        # Persistimos temporales
        df_a.to_parquet(TMP_DIR / "a.parquet", index=False)
        df_c.to_parquet(TMP_DIR / "c.parquet", index=False)
        df_m.to_parquet(TMP_DIR / "m.parquet", index=False)

    def transform(**context):
        """Transforma → dataset final consolidado y guarda temporal."""
        from etl_utils import transform_to_dataset

        df_a = pd.read_parquet(TMP_DIR / "a.parquet")
        df_c = pd.read_parquet(TMP_DIR / "c.parquet")
        df_m = pd.read_parquet(TMP_DIR / "m.parquet")

        df_final, _src_metrics = transform_to_dataset({"csv": df_a, "json": df_c, "xml": df_m})
        df_final.to_parquet(TMP_DIR / "final.parquet", index=False)

        context["ti"].xcom_push(key="final_rows", value=int(len(df_final)))

    def load_and_monitor(**context):
        """
        Carga tablas de datos en SQLite, registra monitoreo (etl_monitor)
        y escribe log estructurado (JSON line) en state/etl.log.
        """
        from etl_utils import (
            write_etl_log,
            sqlite_upsert_monitor,
            build_monitor_record,
        )

        ti = context["ti"]
        run_id = ti.xcom_pull(key="run_id", task_ids="start")
        start_ts = ti.xcom_pull(key="start_ts", task_ids="start")
        n_csv = int(ti.xcom_pull(key="n_csv", task_ids="extract") or 0)
        n_json = int(ti.xcom_pull(key="n_json", task_ids="extract") or 0)
        n_xml = int(ti.xcom_pull(key="n_xml", task_ids="extract") or 0)
        final_rows = int(ti.xcom_pull(key="final_rows", task_ids="transform") or 0)

        # Recuperamos temporales
        df_a = pd.read_parquet(TMP_DIR / "a.parquet")
        df_c = pd.read_parquet(TMP_DIR / "c.parquet")
        df_m = pd.read_parquet(TMP_DIR / "m.parquet")
        df_f = pd.read_parquet(TMP_DIR / "final.parquet")

        # 1) Persistencia de tablas de datos
        _save_all_tables_to_sqlite(df_a, df_c, df_m, df_f, str(DB_PATH))

        # 2) Registro de monitoreo + log estructurado
        end_ts = datetime.utcnow().isoformat()
        # duración
        try:
            dt_start = datetime.fromisoformat(start_ts)
            duration = (datetime.fromisoformat(end_ts) - dt_start).total_seconds()
        except Exception:
            duration = None

        src_metrics = {
            "source_csv_rows": n_csv,
            "source_json_rows": n_json,
            "source_xml_rows": n_xml,
        }
        out_metrics = {"output_rows": final_rows}

        record = build_monitor_record(
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_s=duration if duration is not None else 0.0,
            status="OK",
            metrics_sources=src_metrics,
            metrics_output=out_metrics,
        )
        # a) Upsert a etl_monitor
        sqlite_upsert_monitor(str(DB_PATH), record)
        # b) Línea JSON al etl.log
        write_etl_log(str(STATE_DIR), {**record, "level": "INFO", "message": "ETL run OK"})

    def export_artifacts(**context):
        """Exporta CSV final y JSON para MongoDB a /outputs."""
        from etl_utils import export_outputs

        df_f = pd.read_parquet(TMP_DIR / "final.parquet")
        metrics = export_outputs(df_f, str(OUT_DIR))

        # Por si quieres dejar trazabilidad del export también
        from etl_utils import write_etl_log
        write_etl_log(str(STATE_DIR), {"event": "export_done", **metrics, "ts": datetime.utcnow().isoformat()})

    # Definición de tareas
    start = PythonOperator(task_id="start", python_callable=start_run)
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_monitor = PythonOperator(task_id="load_and_monitor", python_callable=load_and_monitor)
    export = PythonOperator(task_id="export_artifacts", python_callable=export_artifacts)

    # Orquestación
    start >> extract_task >> transform_task >> load_monitor >> export
