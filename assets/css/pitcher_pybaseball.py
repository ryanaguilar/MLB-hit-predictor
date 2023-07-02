import sqlalchemy
import psycopg2
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
from pybaseball import statcast_single_game

database_username = 'raguilar'
database_password = quote_plus("Lb$bYdu^2P%gv%D6cfqNJ7@aAhE&jT7r")
database_ip       = 'localhost'
database_name     = 'mlb_dw'

with open('./data/sched_2014.json', 'r') as infile:
    sched_2014 = infile.read()
    sched_2014_json = json.loads(sched_2014)

def extract_games(schedule: json) -> Generator[dict, None, None]:
    for date in schedule['dates']:
        for game in date['games']:
            yield {'date':date['date'], 'gamePk':game['gamePk']}

ext_games_2014 = extract_games(sched_2014_json) 

database_connection = sqlalchemy.create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name), pool_timeout = 28800, max_overflow = -1)
for date in ext_games_2014:
    connection = database_connection.connect()
    print("made connection")
    try:
        data = statcast_single_game(date['gamePk'])
        data.to_sql('game_data', connection, if_exists='append')
    except Exception as e:
        print(f"error={e}, gamePk={date['gamePk']}")
        connection.close()
    connection.close()