import sqlalchemy
from sqlalchemy import text
import psycopg2
from psycopg2.extensions import register_adapter
from psycopg2.extras import json
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import json
from typing import Generator
import requests
import re
import pandas as pd
import logging
import csv
import time
import json
from pybaseball import statcast_single_game, statcast_pitcher, playerid_reverse_lookup, playerid_lookup

def setup_db():
    database_username = 'postgres'
    database_password = quote_plus("postgres")
    #database_ip       = '192.168.1.169:6543'
    database_ip       = '172.19.0.2:5432'
    database_name     = 'mlb_dw'
    database_engine = sqlalchemy.create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}?options=-csearch_path%3Ddbo,raw'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name), pool_timeout = 28800, max_overflow = -1)
    connection = database_engine.connect()
    print("made connection")
    connection.close()
    return database_engine

def get_schedule():
    r = requests.get(url='https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2023-03-30&endDate=2023-07-06')
    r_json = r.json()
    return r_json

def extract_games(schedule: json) -> Generator[dict, None, None]:
    for date in schedule['dates']:
        for game in date['games']:
            yield {'date':date['date'], 'game_pk':game['gamePk']}

def get_box_score(game_id):
    resp = requests.get(url=f'https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore')
    box_score_json = resp.json()
    return box_score_json

def load_box_score(date: str, game_id: int, box_score_json, engine):
    players_list = []
    away_team = box_score_json['teams']['away']['players']
    home_team = box_score_json['teams']['home']['players']
    for k,v in away_team.items():
        players_list.append({'date':date,'game':game_id, 'player_id':k,'all_data':v})
    for k,v in home_team.items():
        players_list.append({'date':date,'game':game_id, 'player_id':k,'all_data':v})
    with engine.connect() as connection:
        for player in players_list:
            register_adapter(dict,json)
            player['stats'] = player['all_data']['stats']['batting']
            player['season_stats'] = player['all_data']['seasonStats']['batting']
            print(player['stats'])
            print(date)
            table = "box_scores"
            connection.execute(text(f"INSERT INTO {table} (game_date,game,player,v_stats, v_season_stats) VALUES ('{date}',{game_id}, {int(player['player_id'][2:8])}, '{json.dumps(player['stats'])}','{json.dumps(player['season_stats'])}')"))
            connection.commit()
    connection.close()

database_engine = setup_db()
schedule_2023 = get_schedule()
games_2023 = extract_games(schedule_2023)
#subset = list(games_2023)[:2]

def load_box_score_pitchers(date: str, game_id: int, box_score_json, engine):
    players_list = []
    away_team = box_score_json['teams']['away']['players']
    home_team = box_score_json['teams']['home']['players']
    for k,v in away_team.items():
        players_list.append({'date':date,'game':game_id, 'player_id':k,'all_data':v})
    for k,v in home_team.items():
        players_list.append({'date':date,'game':game_id, 'player_id':k,'all_data':v})
    with engine.connect() as connection:
        for player in players_list:
            if player['all_data']['stats']['pitching'] != {}:
                register_adapter(dict,json)
                player['stats'] = player['all_data']['stats']['pitching']
                player['season_stats'] = player['all_data']['seasonStats']['pitching']
                print(player['stats'])
                print(date)
                table = "box_scores_pitchers"
                connection.execute(text(f"INSERT INTO {table} (game_date,game,player,v_stats, v_season_stats) VALUES ('{date}',{game_id}, {int(player['player_id'][2:8])}, '{json.dumps(player['stats'])}','{json.dumps(player['season_stats'])}')"))
                connection.commit()
    connection.close()


def main():
    for game in games_2023:
        box_score = get_box_score(game['game_pk'])
        load_box_score_pitchers(game['date'], game['game_pk'],box_score,database_engine)
        print(game['date'], game['game_pk'])