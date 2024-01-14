import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws
from pyspark.sql.window import Window
import pyspark.sql.functions as sf


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


def get_players_info(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_players_info")
        .getOrCreate()
    )

    df_teams = spark.read.parquet(DWH_PATH + "teams")
    triCode_lst = df_teams.select(col("triCode")).rdd.flatMap(lambda x: x).collect()

    teams_roster = pd.DataFrame()

    for code in triCode_lst:
        try:
            team = get_information(f"/v1/roster/{code}/current")
            players_lst = []

            for key, value in team.items():
                players_lst.extend(value)

            df_team = pd.DataFrame(players_lst)
            df_team["triCodeCurrent"] = code

            teams_roster = pd.concat([teams_roster, df_team], ignore_index=True)
        except:
            continue

    teams_roster["firstName"] = teams_roster["firstName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["lastName"] = teams_roster["lastName"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["birthCity"] = teams_roster["birthCity"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster["birthStateProvince"] = teams_roster["birthStateProvince"].apply(
        lambda x: x.get("default", "") if type(x) == dict else ""
    )
    teams_roster.rename(columns={"id": "playerId"}, inplace=True)

    df_teams_roster = spark.createDataFrame(teams_roster)
    df_teams_roster = df_teams_roster.withColumn("updatedDt", lit(f"{current_date}"))

    df_teams_roster.repartition(1).write.mode("overwrite").parquet(
        RAW_PATH + "players_info/" + current_date
    )


def players_info_to_dwh(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("players_info_to_dwh")
        .getOrCreate()
    )

    df_teams_roster_new = spark.read.parquet(RAW_PATH + f"players_info/{current_date}")

    try:
        df_teams_roster_old = spark.read.parquet(DWH_PATH + f"players_info")

        df_old_players = df_teams_roster_old.join(
            df_teams_roster_new,
            "playerId",
            "leftanti",
        )
        df_all_players = df_teams_roster_new.union(df_old_players)

        df_all_players.repartition(1).write.mode("overwrite").parquet(
            DWH_PATH + f"players_info"
        )
    except:
        df_teams_roster_new.repartition(1).write.mode("overwrite").parquet(
            DWH_PATH + f"players_info"
        )


def get_games_info(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("parse_games_info")
        .getOrCreate()
    )

    df_players = spark.read.parquet(DWH_PATH + "players_info")
    players_lst = df_players.select(col("playerId")).rdd.flatMap(lambda x: x).collect()

    df_games = pd.DataFrame()

    for player in players_lst:
        try:
            player_data = get_information(f"/v1/player/{player}/game-log/now")
            df_player = pd.DataFrame(player_data["gameLog"])
            df_player["playerId"] = player

            df_games = pd.concat([df_games, df_player], ignore_index=True)
        except:
            continue

    df_games["commonName"] = df_games["commonName"].apply(lambda x: x.get("default", "") if type(x) == dict else "")
    df_games["opponentCommonName"] = df_games["opponentCommonName"].apply(lambda x: x.get("default", "") if type(x) == dict else "")
    df_games["gameId"] = df_games.gameId.astype("int")

    df_games_info = spark.createDataFrame(df_games)
    df_games_info = df_games_info.drop(col("commonName"), col("opponentCommonName"))

    df_games_info.repartition(1).write.mode("overwrite").parquet(
        RAW_PATH + "games_info/" + current_date
    )


def games_info_to_dwh(**kwargs):
    current_date = kwargs["ds"]

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("games_info_to_dwh")
        .getOrCreate()
    )

    df_games_info_new = spark.read.parquet(RAW_PATH + f"games_info/{current_date}")

    try:
        df_games_info_old = spark.read.parquet(DWH_PATH + f"games_info")

        df_old_games = df_games_info_old.join(
            df_games_info_new,
            ["playerId", "gameId"],
            "leftanti",
        )
        df_all_games = df_games_info_new.union(df_old_games)

        df_all_games.repartition(1).write.mode("overwrite").parquet(
            DWH_PATH + f"games_info"
        )
    except:
        df_games_info_new.repartition(1).write.mode("overwrite").parquet(
            DWH_PATH + f"games_info"
        )


def players_games_datamart_dwh(**kwargs):

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("games_info_to_dwh")
        .getOrCreate()
    )

    df_teams = spark.read.parquet(DWH_PATH + f"teams")
    df_players_info = spark.read.parquet(DWH_PATH + f"players_info")
    df_games_info = spark.read.parquet(DWH_PATH + f"games_info")

    df_games_info = df_games_info.join(df_players_info, "playerId", "left")\
                            .drop("headshot", "commonName", "opponentCommonName")\
                            .join(df_teams.select("triCode", "fullName"), df_games_info.teamAbbrev == df_teams.triCode, "left")\
                            .drop("triCode").withColumnRenamed("fullName", "teamFullName")\
                            .join(df_teams.select("triCode", "fullName"), df_games_info.opponentAbbrev == df_teams.triCode, "left")\
                            .drop("triCode").withColumnRenamed("fullName", "opponentFullName")\
                            .join(df_teams.select("triCode", "fullName"), df_players_info.triCodeCurrent == df_teams.triCode, "left")\
                            .drop("triCode").withColumnRenamed("fullName", "currentTeamFullName")
    
    df_games_info = df_games_info.withColumn("playersFIO", concat_ws(" ", col("firstName"), col("lastName")))\
                            .drop("firstName", "lastName")

    df_games_info.repartition(1).write.mode("overwrite").parquet(
        DWH_PATH + f"players_games_datamart"
    )


def convert_to_seconds(time_str):
    minutes, seconds = map(int, time_str.split(":"))
    return minutes * 60 + seconds


def skaters_agg_dwh(**kwargs):

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("skaters_agg_to_dwh")
        .getOrCreate()
    )

    df_players_games_datamart = spark.read.parquet(DWH_PATH + f"players_games_datamart")

    convert_to_seconds_udf = spark.udf.register("convert_to_seconds", convert_to_seconds)

    df_skaters_agg = df_players_games_datamart.filter(col("positionCode") != "G")\
        .withColumn("toi", convert_to_seconds_udf("toi"))\
        .groupBy(
            "playerId",
            "playersFIO",
            "triCodeCurrent",
            "currentTeamFullName",
            "teamAbbrev",
            "teamFullName",
            "homeRoadFlag",
            "birthDate",
            "birthCity",
            "birthCountry",
            "birthStateProvince",
            "positionCode",
            "shootsCatches"
        ).agg(
            sf.count("gameId").alias("gamesCNT"),
            sf.sum("goals").alias("goals"),
            sf.sum("points").alias("points"),
            sf.sum("plusMinus").alias("plusMinus"),
            sf.sum("powerPlayGoals").alias("powerPlayGoals"),
            sf.sum("powerPlayPoints").alias("powerPlayPoints"),
            sf.sum("gameWinningGoals").alias("gameWinningGoals"),
            sf.sum("otGoals").alias("otGoals"),
            sf.sum("shots").alias("shots"),
            sf.sum("shifts").alias("shifts"),
            sf.sum("shorthandedGoals").alias("shorthandedGoals"),
            sf.sum("toi").alias("toi"),
            sf.sum("pim").alias("pim"),
        )

    df_skaters_agg = df_skaters_agg.sort(df_skaters_agg.playerId)

    df_skaters_agg.repartition(1).write.mode("overwrite").parquet(
        DWH_PATH + f"skaters_agg"
    )


def goalies_agg_dwh(**kwargs):

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("goalies_agg_to_dwh")
        .getOrCreate()
    )

    df_players_games_datamart = spark.read.parquet(DWH_PATH + f"players_games_datamart")

    convert_to_seconds_udf = spark.udf.register("convert_to_seconds", convert_to_seconds)

    df_goalies_agg = df_players_games_datamart.filter(col("positionCode") == "G")\
        .withColumn("toi", convert_to_seconds_udf("toi"))\
        .groupBy(
            "playerId",
            "playersFIO",
            "triCodeCurrent",
            "currentTeamFullName",
            "teamAbbrev",
            "teamFullName",
            "homeRoadFlag",
            "birthDate",
            "birthCity",
            "birthCountry",
            "birthStateProvince",
            "positionCode",
            "shootsCatches"
        ).agg(
            sf.count("gameId").alias("gamesCNT"),
            sf.sum("goals").alias("goals"),
            sf.sum("gamesStarted").alias("gamesStarted"),
            sf.sum("shotsAgainst").alias("shotsAgainst"),
            sf.sum("goalsAgainst").alias("goalsAgainst"),
            sf.sum("shutouts").alias("shutouts"),
            sf.sum("toi").alias("toi"),
            sf.sum("pim").alias("pim"),
        )

    df_goalies_agg = df_goalies_agg.sort(df_goalies_agg.playerId)

    df_goalies_agg.repartition(1).write.mode("overwrite").parquet(
        DWH_PATH + f"goalies_agg"
    )


task_get_players_info = PythonOperator(
    task_id="get_players_info",
    python_callable=get_players_info,
    dag=dag,
)

task_players_info_to_dwh = PythonOperator(
    task_id="players_info_to_dwh",
    python_callable=players_info_to_dwh,
    dag=dag,
)

task_get_games_info = PythonOperator(
    task_id="get_games_info",
    python_callable=get_games_info,
    dag=dag,
)

task_games_info_to_dwh = PythonOperator(
    task_id="games_info_to_dwh",
    python_callable=games_info_to_dwh,
    dag=dag,
)

task_players_games_datamart_dwh = PythonOperator(
    task_id="players_games_datamart_dwh",
    python_callable=players_games_datamart_dwh,
    dag=dag,
)

task_skaters_agg_dwh = PythonOperator(
    task_id="skaters_agg_dwh",
    python_callable=skaters_agg_dwh,
    dag=dag,
)

task_goalies_agg_dwh = PythonOperator(
    task_id="goalies_agg_dwh",
    python_callable=goalies_agg_dwh,
    dag=dag,
)


task_get_players_info >> task_players_info_to_dwh >> task_get_games_info >> task_games_info_to_dwh >> task_players_games_datamart_dwh
task_players_games_datamart_dwh >> [task_skaters_agg_dwh, task_goalies_agg_dwh]
