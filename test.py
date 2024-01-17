import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

DEFAULT_ARGS = {
    "owner": "Maxim Galanov",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="nhl_players_games",
    schedule_interval="0 0 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_big_data_nhl"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting NHL players atributes and games they played",
)

RAW_PATH = "/user/maxglnv/data/raw/"
DWH_PATH = "/user/maxglnv/data/dwh/"


def test_to_dwh(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("players_info_to_dwh")
        .getOrCreate()
    )

    df_games_info_new = spark.read.parquet(RAW_PATH + f"games_info/2024-01-16")
    df_games_info_new.repartition(1).write.mode("overwrite").parquet(
            DWH_PATH + f"games_info"
        )


task_test_to_dwh = PythonOperator(
    task_id="test_to_dwh",
    python_callable=test_to_dwh,
    dag=dag,
)

task_test_to_dwh