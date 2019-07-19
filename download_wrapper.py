import os
import requests
from fpltools.download import load_credentials, retrieve_data
from fpltools.download import retrieve_player_details
from fpltools.utils import SaveData, API_URLS_FULL

# CONVERT TO ARGUMENTS ETC.
SEASON_ID = 's201920'
# LINK_BASE = 'https://fantasy.premierleague.com/api/'
LOGIN_URL = 'https://users.premierleague.com/accounts/login/'
SAVE_DIR = 'data'

session = requests.session()

credentials = load_credentials(os.path.join('reference',
                                            'credentials.txt'))
session.post(LOGIN_URL, data=credentials)

try:
    datafile_all = retrieve_data(API_URLS_FULL["misc"])
    datafile_ent = retrieve_data(API_URLS_FULL["entries"])
    datafile_trn = retrieve_data(API_URLS_FULL["transfers"])
    # datafile_dyn = retrieve_data(API_URLS_FULL['dynamic'])
except:
    print("Unable to load data requiring credentials. Skipping these.")

# Main data
datafile_main = retrieve_data(API_URLS_FULL['static'])

# Fixtures
datafile_fixtures = retrieve_data(API_URLS_FULL['fixtures'])

# Complete player variable set
datafile_players_deep = retrieve_player_details(API_URLS_FULL['player'],
                                                datafile_main['elements'],
                                                verbose=True)

out_data = {'data_main': datafile_main,
            'data_fixtures': datafile_fixtures,
            'data_players_deep': datafile_players_deep}

sd = SaveData(datafile_main, seasonid=SEASON_ID, save_base=SAVE_DIR)
sd.save_wrapper_to_disk(out_data)

