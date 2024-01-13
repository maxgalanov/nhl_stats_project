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


def players_info_to_dwh(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("players_info_to_dwh")
        .getOrCreate()
    )

    teams_roster = spark.read.parquet(RAW_PATH + "players_info/2024-01-11")
    teams_roster = teams_roster.withColumn("updatedDt", lit("2023-01-11"))
    teams_roster = teams_roster.withColumnRenamed("id_player", "playerId")

    teams_roster.repartition(1).write.mode("overwrite").parquet(
        DWH_PATH + f"players_info"
    )


task_players_info_to_dwh = PythonOperator(
    task_id="players_info_to_dwh",
    python_callable=players_info_to_dwh,
    dag=dag,
)

task_players_info_to_dwh
