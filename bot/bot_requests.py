import pandas as pd
import requests
import json

def get_result_by_now():
    ans = requests.get("https://api-web.nhle.com/v1/scoreboard/now")
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