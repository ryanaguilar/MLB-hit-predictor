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
    database_ip       = '172.19.0.2'
    database_name     = 'mlb_dw'
    database_connection = sqlalchemy.create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}?options=-csearch_path%3Ddbo,raw'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name), pool_timeout = 28800, max_overflow = -1)
    connection = database_connection.connect()
    print("made connection")
    connection.close()
    return database_connection

def extract_games(schedule: json) -> Generator[dict, None, None]:
    for date in schedule['dates']:
        for game in date['games']:
            yield {'date':date['date'], 'gamePk':game['gamePk']}

def load_box_score(game_id: int, box_score_json, database_connection):
    players_list = []
    away_team = box_score_json['teams']['away']['players']
    home_team = box_score_json['teams']['home']['players']
   #both_teams = away_team + home_team
    for k,v in away_team.items():
        players_list.append({'game':game_id, 'player_id':k,'all_data':v})
    with database_connection.connect() as connection:
        for player in players_list:
            register_adapter(dict,json)
            player['stats'] = player['all_data']['stats']['batting']
            player['season_stats'] = player['all_data']['seasonStats']['batting']
            print(player['stats'])
            table = "box_scores"
            connection.execute(text(f"INSERT INTO {table} (game,player,v_stats, v_season_stats) VALUES ({game_id}, {int(player['player_id'][2:8])}, '{json.dumps(player['stats'])}','{json.dumps(player['season_stats'])}')"))
            connection.commit()
    connection.close()