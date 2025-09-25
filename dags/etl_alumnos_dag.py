from datetime import datetime, timedelta
from pathlib import Path
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator

# Rutas internas en el contenedor de Airflow
DATA_DIR = Path("/opt/airflow/data")
OUT_DIR = Path("/opt/airflow/outputs")
STATE_DIR = Path("/opt/airflow/state")
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
    schedule="@daily",         # cambia a None si quieres ejecutar manual
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["etl", "alumnos"],
) as dag:

    def start_run(**context):
        """Genera run_id y marca inicio."""
        run_id = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        context["ti"].xcom_push(key="run_id", value=run_id)
        context["ti"].xcom_push(key="start_ts", value=datetime.utcnow().isoformat())
        return run_id

    def extract(**context):
        """Lee CSV/JSON/XML y guarda temporales en Parquet."""
        from etl_utils import load_alumnos, load_calificaciones, load_matriculas
        import pandas as pd

        df_a = load_alumnos(DATA_DIR / "alumnos.csv")
        df_c = load_calificaciones(DATA_DIR / "calificaciones.json")
        df_m = load_matriculas(DATA_DIR / "matriculas.xml")

        ti = context["ti"]
        ti.xcom_push(key="n_csv", value=int(len(df_a)))
        ti.xcom_push(key="n_json", value=int(len(df_c)))
        ti.xcom_push(key="n_xml", value=int(len(df_m)))

        tmp = STATE_DIR / "tmp"
        tmp.mkdir(parents=True, exist_ok=True)
        df_a.to_parquet(tmp / "a.parquet", index=False)
        df_c.to_parquet(tmp / "c.parquet", index=False)
        df_m.to_parquet(tmp / "m.parquet", index=False)

    def transform(**context):
        """Transforma → dataset final consolidado."""
        import pandas as pd
        from etl_utils import build_final

        tmp = STATE_DIR / "tmp"
        df_a = pd.read_parquet(tmp / "a.parquet")
        df_c = pd.read_parquet(tmp / "c.parquet")
        df_m = pd.read_parquet(tmp / "m.parquet")

        df_final = build_final(df_a, df_c, df_m)
        df_final.to_parquet(tmp / "final.parquet", index=False)

        context["ti"].xcom_push(key="final_rows", value=int(len(df_final)))

    def load_and_monitor(**context):
        """Carga en SQLite, registra monitoreo y escribe log JSONL."""
        import pandas as pd
        from etl_utils import save_sqlite, insert_monitor, structured_log

        ti = context["ti"]
        run_id = ti.xcom_pull(key="run_id", task_ids="start")
        start_ts = ti.xcom_pull(key="start_ts", task_ids="start")
        n_csv = ti.xcom_pull(key="n_csv", task_ids="extract")
        n_json = ti.xcom_pull(key="n_json", task_ids="extract")
        n_xml = ti.xcom_pull(key="n_xml", task_ids="extract")
        final_rows = ti.xcom_pull(key="final_rows", task_ids="transform")

        tmp = STATE_DIR / "tmp"
        df_a = pd.read_parquet(tmp / "a.parquet")
        df_c = pd.read_parquet(tmp / "c.parquet")
        df_m = pd.read_parquet(tmp / "m.parquet")
        df_f = pd.read_parquet(tmp / "final.parquet")

        save_sqlite(df_a, df_c, df_m, df_f, str(DB_PATH))

        end_ts = datetime.utcnow().isoformat()
        metrics = dict(
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_s=None,  # se calcula en insert_monitor si falta
            status="OK",
            source_csv_rows=n_csv,
            source_json_rows=n_json,
            source_xml_rows=n_xml,
            clean_rows_alumnos=len(df_a),
            clean_rows_califs=len(df_c),
            clean_rows_matriculas=len(df_m),
            final_rows=final_rows,
            errors_count=0,
            warns_count=0,
            notes_out_of_range=int(
                ((df_c["nota"] > 5).sum() + (df_c["nota"] < 0).sum())
            ) if "nota" in df_c.columns else 0,
            mongo_docs_prepared=None,
            log_path=str(LOG_PATH),
        )
        insert_monitor(metrics, str(DB_PATH))
        structured_log({**metrics, "level": "INFO", "msg": "ETL run OK"}, str(LOG_PATH))

    def export_artifacts(**context):
        """Exporta CSV final y JSON para MongoDB."""
        import pandas as pd
        from etl_utils import export_csv, make_mongo_json

        tmp = STATE_DIR / "tmp"
        df_f = pd.read_parquet(tmp / "final.parquet")
        export_csv(df_f, OUT_DIR / "dataset_final.csv")

        df_c = pd.read_parquet(tmp / "c.parquet")
        df_m = pd.read_parquet(tmp / "m.parquet")
        make_mongo_json(df_f, df_c, df_m, OUT_DIR / "estudiantes_mongodb.json")

    start = PythonOperator(task_id="start", python_callable=start_run)
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_monitor = PythonOperator(task_id="load_and_monitor", python_callable=load_and_monitor)
    export = PythonOperator(task_id="export_artifacts", python_callable=export_artifacts)

    start >> extract_task >> transform_task >> load_monitor >> export
