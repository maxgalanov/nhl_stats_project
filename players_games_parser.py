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
    description="ETL process for getting NHL players atributes and games they played",
)

RAW_PATH = '/opt/hadoop/airflow/dags/galanov/nhl_stats_project/data/raw/'

def get_players_info(**kwargs):
    current_date = kwargs['ds']

    spark = SparkSession.builder.master("local[*]").appName("parse_players_info").getOrCreate()

    df_teams = spark.read.parquet(RAW_PATH + 'teams')

    triCode_lst = df_teams.select(col('triCode')).rdd.flatMap(lambda x: x).collect()

    teams_roster = pd.DataFrame()

    for code in triCode_lst:
        try:
            team = tools.get_information(f'/v1/roster/{code}/current')
            players_lst = []
            
            for key, value in team.items():
                players_lst.extend(value)

            df_team = pd.DataFrame(players_lst)
            df_team['triCodeCurrent'] = code

            teams_roster = pd.concat([teams_roster, df_team], ignore_index=True)
        except:
            continue

    # Вытаскиваем данные из json
    teams_roster['firstName'] = teams_roster['firstName'].apply(lambda x: x.get('default', '') if type(x) == dict else '')
    teams_roster['lastName'] = teams_roster['lastName'].apply(lambda x: x.get('default', '') if type(x) == dict else '')
    teams_roster['birthCity'] = teams_roster['birthCity'].apply(lambda x: x.get('default', '') if type(x) == dict else '')
    teams_roster['birthStateProvince'] = teams_roster['birthStateProvince'].apply(lambda x: x.get('default', '') if type(x) == dict else '')
    teams_roster.rename(columns={'id': 'id_player'}, inplace=True)

    df_teams_roster = spark.createDataFrame(teams_roster)

    df_teams_roster.repartition(1).write.mode("overwrite").parquet(RAW_PATH + 'players_info/' + current_date)


task_get_teams = PythonOperator(
    task_id="get_teams",
    python_callable=get_teams,
    dag=dag,
)

task_get_teams
