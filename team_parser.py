import pandas as pd
import py_scripts.tools as tools
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DEFAULT_ARGS = {
    "owner": "Maxim Galanov",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="hse_de_etl_process",
    schedule_interval="0 0 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_big_data_nhl"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams yearly before every season",
)

RAW_PATH = '/opt/hadoop/airflow/dags/galanov/nhl_stats_project/data/raw/'


def get_teams():
    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    data_teams = tools.get_information("en/team", "https://api.nhle.com/stats/rest/")
    df_teams_pd = pd.DataFrame(data_teams["data"])

    df_teams = spark.createDataFrame(df_teams_pd)
    df_teams = df_teams.select(col("id"), col("fullName"), col("triCode"))

    df_teams.repartition(1).write.mode("overwrite").parquet(RAW_PATH + 'teams')


task_get_teams = PythonOperator(
    task_id="get_teams",
    python_callable=get_teams,
    dag=dag,
)

task_get_teams
