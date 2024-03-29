import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava, Boyarkin",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="nhl_teams",
    schedule_interval="45 5 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_big_data_nhl"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams yearly before every season",
)

RAW_PATH = "/user/maxglnv/data/raw/"
DWH_PATH = "/user/maxglnv/data/dwh/"


def get_information(endpoint, base_url="https://api-web.nhle.com"):
    base_url = f"{base_url}"
    endpoint = f"{endpoint}"
    full_url = f"{base_url}{endpoint}"

    response = requests.get(full_url)

    if response.status_code == 200:
        player_data = response.json()
        return player_data
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")


def get_teams(**kwargs):
    current_date = kwargs["ds"]

    spark = SparkSession.builder.master("local[*]").appName("parse_teams").getOrCreate()

    data_teams = get_information("en/team", "https://api.nhle.com/stats/rest/")
    df_teams_pd = pd.DataFrame(data_teams["data"])
    df_teams = spark.createDataFrame(df_teams_pd)

    df_teams.repartition(1).write.mode("overwrite").parquet(
        RAW_PATH + f"teams/{current_date}"
    )


def teams_to_dwh(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]").appName("teams_to_dwh").getOrCreate()
    )

    df_teams = spark.read.parquet(RAW_PATH + f"teams/{current_date}")
    df_teams = df_teams.select(col("id"), col("fullName"), col("triCode"))

    df_teams.repartition(1).write.mode("overwrite").parquet(DWH_PATH + f"teams")


task_get_teams = PythonOperator(
    task_id="get_teams",
    python_callable=get_teams,
    dag=dag,
)

task_teams_to_dwh = PythonOperator(
    task_id="teams_to_dwh",
    python_callable=teams_to_dwh,
    dag=dag,
)

task_get_teams >> task_teams_to_dwh
