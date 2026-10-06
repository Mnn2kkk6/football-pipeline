"""DAG: extract -> spark transform/load -> dbt run -> dbt test -> export dataset xG.

Chạy hằng ngày; extract idempotent nên chỉ tải trận mới (tức là sau mỗi vòng đấu sẽ tự lấy thêm).
Đổi giải đấu bằng Airflow Variables: competition_id, season_id.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT = "cd /opt/airflow/dbt && /home/airflow/dbt_venv/bin/dbt {cmd} --profiles-dir ."


def _extract():
    from src.extract import extract
    cid = int(Variable.get("competition_id", default_var=43))
    sid = int(Variable.get("season_id", default_var=3))
    return extract(cid, sid)


def _export():
    from src.export_training import export
    export()


with DAG(
    dag_id="football_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["football", "statsbomb"],
) as dag:
    extract = PythonOperator(task_id="extract_to_s3", python_callable=_extract)

    transform = BashOperator(
        task_id="spark_transform_load",
        bash_command="cd /opt/airflow && python -m src.transform_spark",
    )
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))
    export_csv = PythonOperator(task_id="export_xg_training_csv", python_callable=_export)

    extract >> transform >> dbt_run >> dbt_test >> export_csv
