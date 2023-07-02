import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import logging
import csv
import time

logger = logging.getLogger('mlbdw_logger')
logging.basicConfig(level=logging.DEBUG)

lookup = {
    'ARI': {'full_name': 'Arizona Diamondbacks', 'alt_name': 'ARI'},
    'ATL': {'full_name': 'Atlanta Braves', 'alt_name': 'ATL'},
    'BAL': {'full_name': 'Baltimore Orioles', 'alt_name': 'BAL'},
    'BOS': {'full_name': 'Boston Red Sox', 'alt_name': 'BOS'},
    'CHC': {'full_name': 'Chicago Cubs', 'alt_name': 'CHC'},
    'CHW': {'full_name': 'Chicago White Sox', 'alt_name': 'CHA'},
    'CIN': {'full_name': 'Cincinnati Reds', 'alt_name': 'CIN'},
    'CLE': {'full_name': 'Cleveland Indians', 'alt_name': 'CLE'},
    'COL': {'full_name': 'Colorado Rockies', 'alt_name': 'COL'},
    'DET': {'full_name': 'Detroit Tigers', 'alt_name': 'DET'},
    'FLA': {'full_name': 'Florida Marlins', 'alt_name': 'FLA'},
    'HOU': {'full_name': 'Houston Astros', 'alt_name': 'HOU'},
    'KCR': {'full_name': 'Kansas City Royals', 'alt_name': 'KAN'},
    'LAA': {'full_name': 'Los Angeles Angels of Anaheim', 'alt_name': 'ANA'},
    'LAD': {'full_name': 'Los Angeles Dodgers', 'alt_name': 'LAD'},
    'MIA': {'full_name': 'Miami Marlins', 'alt_name': 'MIA'},
    'MIL': {'full_name': 'Milwaukee Brewers', 'alt_name': 'MIL'},
    'MIN': {'full_name': 'Minnesota Twins', 'alt_name': 'MIN'},
    'NYM': {'full_name': 'New York Mets', 'alt_name': 'NYN'},
    'NYY': {'full_name': 'New York Yankees', 'alt_name': 'NYY'},
    'OAK': {'full_name': 'Oakland Athletics', 'alt_name': 'OAK'},
    'PHI': {'full_name': 'Philadelphia Phillies', 'alt_name': 'PHI'},
    'PIT': {'full_name': 'Pittsburgh Pirates', 'alt_name': 'PIT'},
    'SDP': {'full_name': 'San Diego Padres', 'alt_name': 'SDN'},
    'SFG': {'full_name': 'San Francisco Giants', 'alt_name': 'SFG'},
    'SEA': {'full_name': 'Seattle Mariners', 'alt_name': 'SEA'},
    'STL': {'full_name': 'St Louis Cardinals', 'alt_name': 'STL'},
    'TBR': {'full_name': 'Tampa Bay Rays', 'alt_name': 'TBA'},
    'TEX': {'full_name': 'Texas Rangers', 'alt_name': 'TEX'},
    'TOR': {'full_name': 'Toronto Blue Jays', 'alt_name': 'TOR'},
    'WAS': {'full_name': 'Washington Nationals', 'alt_name': 'WSN'},
    'WSN': {'full_name': 'Washington Nationals', 'alt_name': 'WAS'}
    }

def get_html(date: str, team: str, opp: str, logger: logging.Logger, lookup) -> str:
    year = date.split('-')[0]
    month = date.split('-')[1].replace('0','')
    day = date.split('-')[2].replace('0','')
    headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/112.0',
               'Referer':f'https://www.baseball-reference.com/boxes/?month={month}&day={day}&year={year}'}
    formatted_date = date.replace('-','')
    url = f'https://www.baseball-reference.com/boxes/{team}/{team}{formatted_date}0.shtml'
    response = requests.get(url=url, headers=headers)
    #print(response)
    log_line = f'url={url}, response={response.status_code}'
    logger.debug(msg=log_line)
    if response.status_code == 404:
        time.sleep(500/1000)
        logger.debug('Trying alternate 3 letter code...')
        alt_code = lookup[team]['alt_name']
        url = f'https://www.baseball-reference.com/boxes/{alt_code}/{alt_code}{formatted_date}0.shtml'
        response = requests.get(url=url, headers=headers)
        if response.status_code == 404:
            time.sleep(500/1000)
            logger.debug(msg="First team tried was not home team, trying again...")
            url = f'https://www.baseball-reference.com/boxes/{opp}/{opp}{formatted_date}0.shtml'
            response = requests.get(url=url, headers=headers)
            #print(response)
            log_line_2nd_try = f'ur={url}, response={response.status_code}'
            logging.debug(msg=log_line_2nd_try)
            if response.status_code == 404:
                time.sleep(500/1000)
                logger.debug('Trying alternate 3 letter code...')
                alt_code = lookup[opp]['alt_name']
                url = f'https://www.baseball-reference.com/boxes/{alt_code}/{alt_code}{formatted_date}0.shtml'
                response = requests.get(url=url, headers=headers)
    data_html = response.text
    return data_html

def get_pitching_table(data_html: str, team_name_re: str, logger: logging.Logger) -> pd.DataFrame:
    t = re.findall(f'div class=\"table_container\" id=\"div_{team_name_re}[\s\S]+?</table>', data_html)
    log_line = f'team_name={team_name_re}, list_length={len(t)}'
    logging.debug(msg=log_line)
    try:
        df = pd.read_html(t[1])
    except IndexError as e:
        logging.error(msg=str(e))
    return df[0]

def team_name_lookup(team_code: str, logger: logging.Logger, lookup) -> str:
    team_name = lookup[team_code]['full_name']
    team_name = team_name.replace(' ','')
    log_line=f'formatted_team_name={team_name}'
    logging.debug(msg=log_line)
    return team_name

def get_starting_pitcher(df: pd.DataFrame, date: str, batting_team: str, pitching_team: str, logger: logging.Logger) -> list:
    pitcher_list = (list(df.iloc[0]))
    metadata_list = [date, batting_team, pitching_team]
    starting_pitcher = metadata_list + pitcher_list
    return starting_pitcher

def write_starting_pitcher(filename: str, starting_pitcher: str, logger: logging.Logger):
    with open(filename, 'a') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(starting_pitcher)

#def main(date: str, team: str, opp: str, logger: logging.Logger):
def main(game_log_file: str, logger: logging.Logger, start_index = 0):
    with open(game_log_file, 'r') as infile:
        player_games = []
        game_log_reader = csv.reader(infile)
        for row in game_log_reader:
            player_games.append(row)
    for i,player_game in enumerate(player_games[start_index:]):
        date = player_game[2]
        team = player_game[3]
        opp = player_game[4]
        data_html = get_html(date, team, opp, logger, lookup)
        team_name_re = team_name_lookup(opp, logger, lookup)
        pitching_table = get_pitching_table(data_html, team_name_re, logger)
        starting_pitcher = get_starting_pitcher(pitching_table, date, team, opp, logger)
        write_starting_pitcher('./data/pitching_game_log_14-19.csv', starting_pitcher, logger)
        end_index = str(start_index + i)
        logger.debug(msg=f'end_index={end_index}')

main('./data/GameLogs_1419_2.csv', logger, start_index=38)