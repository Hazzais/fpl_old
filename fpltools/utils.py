import os
import pickle
from datetime import datetime

import pandas as pd

# Links and implementation ideas from:
# https://github.com/amosbastian/fpl/blob/master/fpl/constants.py and
# https://github.com/janerikcarlsen/fpl-cli/blob/master/fplcli/urls.py and
# https://www.reddit.com/r/FantasyPL/comments/9mnq4f/accessing_api_with
# authentication/
# TODO: Check these
API_URLS_BASE = 'https://fantasy.premierleague.com/api/'
API_URLS_FULL = {
    'event': '{}fixtures/?event={{}}/'.format(API_URLS_BASE),
    'me': '{}me/'.format(API_URLS_BASE),
    'entry': '{}entry/{{}}/'.format(API_URLS_BASE),  # playerid? (e.g. 956841)
    'X_history': '{}entry/{{}}/history/'.format(API_URLS_BASE),
    'gameweeks': '{}events/'.format(API_URLS_BASE),
    'gameweek_fixtures': '{}fixtures/?event={{}}/'.format(API_URLS_BASE),
    'gameweek_current': '{}event/{{}}/live/'.format(API_URLS_BASE),
    'dynamic': '{}bootstrap-dynamic/'.format(API_URLS_BASE),
    'live': '{}live/'.format(API_URLS_BASE),  # event/{{gw}}/live
    'history': '{}history/'.format(API_URLS_BASE),  # event/{{gw}}/history
    'fixtures': '{}fixtures/'.format(API_URLS_BASE),
    'player': '{}element-summary/{{}}/'.format(API_URLS_BASE),
    'static': '{}bootstrap-static/'.format(API_URLS_BASE),
    'user_history': '{}entry/{{}}/history/'.format(API_URLS_BASE),
    'user_picks': '{}entry/{{}}/event/{{}}/picks/'.format(API_URLS_BASE),
    'user_team': '{}my-team/{{}}/'.format(API_URLS_BASE),
    'user_transfers': '{}entry/{{}}/transfers/'.format(API_URLS_BASE),
    'transfers': '{}transfers/'.format(API_URLS_BASE),
    'teams': '{}teams/'.format(API_URLS_BASE),

    # 'picks': '{}picks'.format(API_URLS_BASE),
    # 'leagues_entered': '{}leagues-entered'.format(API_URLS_BASE),
    # 'my-team': '{}my-team'.format(API_URLS_BASE),
    # 'entries': '{}entries'.format(API_URLS_BASE),
    # 'element_types': '{}element-types'.format(API_URLS_BASE),
    # 'region': '{}region'.format(API_URLS_BASE),
    # 'event': '{}event'.format(API_URLS_BASE),
    # 'entries': '{}entries'.format(API_URLS_BASE),
    # 'misc': '{}bootstrap'.format(API_URLS_BASE),  # auth
    # 'head_to_head': '{}leagues-entries-and-h2h-matches/league/{{}}?page={{}}'
    #     .format(API_URLS_BASE),
    # 'leagues_classic': '{}leagues-classic/{{}}/standings/'
    #     .format(API_URLS_BASE),
    # 'leagues_head_to_head': 'leagues-h2h/{{}}/standings/'
    #     .format(API_URLS_BASE),
    # 'players': '{}elements'.format(API_URLS_BASE),
    # 'settings': '{}game-settings'.format(API_URLS_BASE),
    # 'user': '{}entry/{{}}'.format(API_URLS_BASE),
    # 'user_cup': '{}entry/{{}}/cup'.format(API_URLS_BASE),
    # 'watchlist': '{}watchlist'.format(API_URLS_BASE),
}

LOGIN_URL = "https://users.premierleague.com/accounts/login/"


# TODO: should return string not int - update elsewhere to make correct use of
def get_datetime_string():
    return round(datetime.now().timestamp())


def get_next_gameweek(data):
    d = pd.DataFrame(data)
    s = d.loc[d.is_next]
    if len(s) == 0:
        raise RuntimeError("Cannot determine gameweek. Season may have ended.")
    elif len(s) > 1:
        raise RuntimeError("More than one gameweek is marked.")
    else:
        return int(s.id.values)


def get_current_gameweek(data):
    d = pd.DataFrame(data)
    s = d.loc[d.is_current]
    if len(s) == 0:
        raise RuntimeError("Cannot determine gameweek. Season may not yet have"
                           " begun.")
    elif len(s) > 1:
        raise RuntimeError("More than one gameweek is marked.")
    return int(s.id.values)


def get_previous_gameweek(data):
    d = pd.DataFrame(data)
    s = d.loc[d.is_previous]
    if len(s) == 0:
        raise RuntimeError("Cannot determine gameweek. Season may not yet have"
                           " begun.")
    elif len(s) > 1:
        raise RuntimeError("More than one gameweek is marked.")
    return int(s.id.values)


class SaveData:

    def __init__(self, data, seasonid=None, save_base='data'):
        self.timestamp = get_datetime_string()
        self.gameweek = get_next_gameweek(data['events'])
        self.seasonid = seasonid
        self.save_base = save_base
        self.save_dir = os.path.join(self.save_base, self.seasonid,
                                     'GW' + str(self.gameweek))
        if not os.path.exists(self.save_dir):
            os.makedirs(self.save_dir)

    def save_wrapper_to_disk(self, save_data):
        for k, v in save_data.items():
            self.save_to_disk(v, k)

    def save_to_disk(self, data, outname):
        outfile = os.path.join(self.save_dir, outname + '_GW' +
                               str(self.gameweek).zfill(2) + '_' +
                               str(self.timestamp) + '.pkl')
        with open(outfile, 'wb') as out:
            pickle.dump(data, out)
