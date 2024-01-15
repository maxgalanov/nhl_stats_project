import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json

DEFAULT_ARGS = {
    "owner": "Galanov, Shiryeava, Boyarkin",
    "email": "maxglnv@gmail.com",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=15),
}

dag = DAG(
    dag_id="nhl_schedule",
    schedule_interval="0 0 1 10 *",
    start_date=days_ago(2),
    catchup=False,
    tags=["hse_big_data_nhl"],
    default_args=DEFAULT_ARGS,
    description="ETL process for getting list of NHL teams yearly before every season",
)

RAW_PATH = "/user/maxglnv/data/raw/"
DWH_PATH = "/user/maxglnv/data/dwh/"


def get_info_pandas(req):
    
    ans = requests.get(req)
    result = json.loads(ans.text)
    away_teams = []
    home_teams = []
    away_result = []
    home_result = []
    date_play = []
    for date_game in result['gamesByDate']:
        for game in date_game['games']:
            if 'score' in game['awayTeam']:
                away_teams.append(game['awayTeam']['name']['default'])
                away_result.append(game['awayTeam']['score'])
                home_teams.append(game['homeTeam']['name']['default'])
                home_result.append(game['homeTeam']['score'])
                date_play.append(date_game['date'])
            else:
                break
    d = {'away_teams': away_teams,
         'home_teams' : home_teams,
         'away_result': away_result,
         'home_result': home_result,
         'date_play': date_play}
    df = pd.DataFrame(d)
    return df


def get_shedule(**kwargs):
    
    current_date = kwargs["ds"]
    spark = SparkSession.builder.master("local[*]").appName("parse_shedule").getOrCreate()
    df_shedule_pd = get_info_pandas("https://api-web.nhle.com/v1/scoreboard/now")
    df_shedule = spark.createDataFrame(df_shedule_pd)
    df_shedule.repartition(1).write.mode("overwrite").parquet(
        RAW_PATH + f"shedule/{current_date}"
    )


def shedule_to_dwh(**kwargs):
    
    current_date = kwargs["ds"]
    spark = (
        SparkSession.builder.master("local[*]").appName("shedule_to_dwh").getOrCreate()
    )
    df_shedule = spark.read.parquet(RAW_PATH + f"shedule/{current_date}")
    df_shedule = df_shedule.select(col("away_teams"),\
                                   col("home_teams"), col("away_result"), col("home_result"), col("date_play"))
    df_shedule.repartition(1).write.mode("overwrite").parquet(DWH_PATH + f"shedule")


task_get_shedule = PythonOperator(
    task_id="get_shedule",
    python_callable=get_shedule,
    dag=dag,
)

task_shedule_to_dwh = PythonOperator(
    task_id="shedule_to_dwh",
    python_callable=shedule_to_dwh,
    dag=dag,
)

task_get_shedule >> task_shedule_to_dwh
