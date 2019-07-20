import os
import pickle
import pandas as pd
import numpy as np
import sqlite3
from copy import deepcopy
import re
from fpltools.utils import get_current_gameweek, get_next_gameweek,\
    get_previous_gameweek


def get_latest(in_dir, file_or_folder, regex_pattern=r'.*'):
    if file_or_folder == 'file':
        found = [s for s in os.listdir(in_dir)
                 if os.path.isfile(os.path.join(in_dir, s))
                 and re.match(regex_pattern, s, re.IGNORECASE)]
    elif file_or_folder == 'folder':
        found = [s for s in os.listdir(in_dir)
                 if os.path.isdir(os.path.join(in_dir, s))
                 and re.match(regex_pattern, s, re.IGNORECASE)]
    else:
        raise ValueError('file_or_folder incorrectly specified')
    if len(found) == 0:
        raise RuntimeError('No data files (or folders) exist in specified '
                           + in_dir + '.')
    return found[-1]


def raw_data_load(in_dir,
                  in_prefix,
                  season_id='latest',
                  gameweek='latest',
                  datetime_id='latest'):

    if season_id == 'latest':
        season_id = get_latest(in_dir, 'folder', regex_pattern=r's\d{6}')

    if gameweek == 'latest':
        gameweek = get_latest(os.path.join(in_dir, season_id), 'folder',
                              regex_pattern=r'^GW\d+').replace('GW', '')

    # TODO: Feels a bit ugly to use timestamp string on filename to choose the
    #       latest file. Maybe go back to using time created using os. Similar
    #       case to the above two as well.
    if datetime_id == 'latest':
        datetime_id_long = get_latest(os.path.join(in_dir,
                                                   season_id,
                                                   'GW' + str(gameweek)),
                                      'file',
                                      regex_pattern=r'^' + in_prefix)
        datetime_id = os.path.splitext(datetime_id_long.split('_')[-1])[0]

    use_file = os.path.join(in_dir, season_id, 'GW' + str(gameweek),
                            in_prefix + '_GW' + str(gameweek).zfill(2) + '_' +
                            datetime_id +
                            '.pkl')

    with open(use_file, 'rb') as read:
        data_raw = pickle.load(read)
    return data_raw


data_main = raw_data_load('data',
                              'data_main',
                          's201819',
                          37)

data_players = raw_data_load('data',
                              'data_players_deep',
                          's201819',
                          37)

data_fixtures = raw_data_load('data',
                              'data_fixtures',
                          's201819',
                          37)

a_data_main = raw_data_load('data',
                              'data_main')

a_data_players = raw_data_load('data',
                              'data_players_deep')

a_data_fixtures = raw_data_load('data',
                              'data_fixtures')


def replace_nonetype_in_dict(thedict):
    # Replace values which are lists or NoneTypes with numpy nans
    return {k: (np.nan if v == None or isinstance(v, (list,)) else v)
            for k, v in thedict.items()}


def get_events(data):
    # Get all gameweek data

    cols_order = ['id',
                  'name',
                  'finished',
                  'data_checked',
                  'average_entry_score',
                  'highest_score',
                  'highest_scoring_entry',
                  'is_current',
                  'is_next',
                  'is_previous',
                  'deadline_time',
                  'deadline_time_epoch',
                  'deadline_time_formatted',
                  'deadline_time_game_offset',
                  ]


    # Convert dict to DataFrame
    events = pd.DataFrame(columns=cols_order)
    for pl in data:
        event_id = pl['id']
        event_row = pd.DataFrame(pl, index=[event_id])
        events = pd.concat([events, event_row], sort=False)

    events.rename(columns={'id': 'gameweek'}, inplace=True)
    return events


def get_players(data):
    # get current gameweek player data

    # order columns
    col_order = ['id',
                 'code',
                 'element_type',
                 'first_name',
                 'second_name',
                 'team',
                 'team_code',
                 'chance_of_playing_next_round',
                 'chance_of_playing_this_round',
                 'total_points',
                 'now_cost',
                 'selected_by_percent',
                 'status',
                 'news',
                 'news_added',
                 'minutes',
                 'points_per_game',
                 'goals_scored',
                 'assists',
                 'bonus',
                 'goals_conceded',
                 'bps',
                 'cost_change_event',
                 'cost_change_start',
                 'event_points',
                 'form',
                 ]

    # Add unordered columns to end of ordered columns
    cols_all = col_order + [col for col in data[0].keys()
                            if col not in col_order]

    # Convert dictionary with key as ID to DataFrame - cannot immediately call
    # pd.DataFrame on dictionary as this causes an issue later on
    players = pd.DataFrame(columns=cols_all)
    for pl in data:
        player_id = pl['id']
        player_row = pd.DataFrame(pl, index=[player_id])
        players = pd.concat([players, player_row], sort=False)

    players.rename(columns={'id': 'player_id',
                            'code': 'player_id_long',
                            'element_type': 'position_id',
                            'team': 'team_id',
                            'team_code': 'team_id_long'}, inplace=True)

    return players


def cols_to_front(data, cols):
    extra_columns = [col for col in data.columns if col not in cols]
    return data[cols + extra_columns]

def get_gameweek_fixtures(data, gameweek: int):
    outlist = [x for x in data_fixtures if x['event'] == gameweek]
    outdf = pd.DataFrame(outlist)
    outdf['kickoff_datetime'] = pd.to_datetime(outdf['kickoff_time'],
                                               errors='coerce')
    outdf['min_date'] = outdf['kickoff_datetime'].min()
    outdf['event_day'] = (outdf['kickoff_datetime'].dt.date - outdf['min_date'].dt.date).dt.days + 1
    outdf['fixture_id_long'] = outdf['code'].astype(str)
    outdf['fixture_id'] = outdf['id'].astype(str)
    outdf['team_h'] = outdf['team_h'].astype(str)
    outdf['team_a'] = outdf['team_a'].astype(str)
    outdf.rename(columns={'event': 'gameweek'}, inplace=True)
    outdf.drop(columns=['min_date', 'code', 'id'], inplace=True)
    return cols_to_front(outdf, ['gameweek', 'fixture_id_long', 'fixture_id'])


def get_teams(data):
    # Get teams

    cols_order = ['id',
                     'short_name',
                     'name',
                     'strength',
                     'code',
                     'strength_overall_home',
                     'strength_overall_away',
                     'strength_attack_home',
                     'strength_attack_away',
                     'strength_defence_away',
                     'strength_defence_home',
                     'points',
                     'draw',
                     'loss',
                     'win',
                     'form',
                     'position',
                     'played',
                     'team_division',
                     'unavailable',
                     'link_url',
                     ]

    # This complicated loop is needed to convert the dict to a DataFrame due to
    # the different types of values in it.
    teams = pd.DataFrame(columns=cols_order)
    for pl in data:

        # For each team (initial row)
        team_id = pl['id']

        # Add new row to initially empty DF
        teams.loc[team_id, 'id'] = team_id

        # For each item in the team's dictionary
        for k, v in pl.items():

            # Get data for coming fixture from list containing dict. If two
            # elements (double gameweek), take only the first (scope of this
            # project )
            if k=='current_event_fixture' and v:
                v_curr = {'curr_game_' + x: y for (x, y) in pl[k][0].items()}
                for k2, v2 in v_curr.items():
                    teams.loc[team_id, k2] = v2
            # If no current fixture (team has no game this gameweek), ignore
            elif k=='current_event_fixture':
                pass
            # Get data for next fixture from list containing dict. If two
            # elements (double gameweek), take only the first (scope of this
            # project )
            elif k=='next_event_fixture' and v:
                v_next = {'next_game_' + x: y for (x, y) in pl[k][0].items()}
                for k2, v2 in v_next.items():
                    teams.loc[team_id, k2] = v2
            # If no next fixture (team has no game this gameweek), ignore
            elif k=='next_event_fixture':
                pass
            elif v==None:
                v = np.nan
                teams.loc[team_id, k] = v
            else:
                teams.loc[team_id, k] = v

    teams['team_id'] = teams['id'].astype(int).astype(str)
    teams['team_id_long'] = teams['code'].astype(int).astype(str)

    teams.drop(columns=['id', 'code'], inplace=True)

    return cols_to_front(teams, ['team_id', 'team_id_long'])


def get_players_deep(data):
    # Get detailed player details

    cols = ['element',
            'round',
            'fixture',
            'selected',
            'value',
            'total_points',
            'minutes',
            'goals_scored',
            'bonus',
            'opponent_team',
            ]

    # Need to use same data to get two outputs, a past and future dataset
    player_history = pd.DataFrame(columns=cols)
    player_future = pd.DataFrame()
    for player_id, pdict in data.items():
        # pdict is a dictionary containing 'future' and 'history' values for
        # each player
        player_history = player_history.append(pd.DataFrame(pdict['history']),
                                               sort=False)
        temp_future = pd.DataFrame(pdict['fixtures'])
        temp_future['player_id'] = player_id
        player_future = player_future.append(temp_future)

    player_history.rename(columns={'element': 'player_id',
                                   'round': 'gameweek',
                                   'fixture': 'fixture_id',
                                   'id': 'playergw_id'
                                        },
                          inplace=True)
    player_future.rename(columns={'id': 'fixture_id',
                                  'code': 'fixture_id_long',
                                  'event': 'gameweek'
                                        },
                         inplace=True)

    player_history['playergw_id'] = player_history['playergw_id'].astype(int)
    player_history['playergw_id'] = player_history['playergw_id'].astype(str)

    player_future['team_h'] = player_future['team_h'].astype(str)
    player_future['team_a'] = player_future['team_a'].astype(str)

    player_history['fixture_id'] = player_history['fixture_id'].astype(str)
    player_future['fixture_id_long'] = player_future['fixture_id_long']\
        .astype(str)
    player_future['fixture_id'] = player_future['fixture_id'].astype(str)

    player_history = cols_to_front(player_history,
                                   ['player_id', 'gameweek', 'playergw_id',
                                    'fixture_id'])
    player_future = cols_to_front(player_future,
                                  ['player_id', 'gameweek', 'fixture_id_long',
                                   'fixture_id'])


    return player_history, player_future


events = get_events(data_main['events'])
next_gameweek = get_next_gameweek(data_main['events'])
current_gameweek = get_current_gameweek(data_main['events'])
previous_gameweek = get_previous_gameweek(data_main['events'])

next_fixtures = get_gameweek_fixtures(data_fixtures, next_gameweek)

def get_positions(data):
    positions = pd.DataFrame(data['element_types'])
    positions['position_id'] = positions['id'].astype(str)
    positions.drop(columns=['id'], inplace=True)
    return cols_to_front(positions, ['position_id'])

positions = get_positions(data_main)
player_summary = get_players(data_main['elements'])
teams = get_teams(data_main['teams'])
player_history, player_future = get_players_deep(data_players)
total_players = data_main['total-players']

# Key ID variables:
#   - gameweek (cat)
#   - code (int) => fixture_id_long (cat)
#   - [fixture] id (int) => fixture_id (cat)
#   - team (int) => team_id (cat) [same with team_a and team_h]
#


# TODO: This class is a bit of a so-called 'God' object so refactor this when
#       ready. May affect implementation (calls) of the class in other code
#       so bear this in mind.
class PlayerDataFrame():

    def __init__(self, data, gameweek_start=1, gameweek_end='latest',
                 prev_matches_consider=3):
        self.data = data
        self.gameweek_start = gameweek_start
        self.gameweek_end = gameweek_end
        self.prev_matches_consider = prev_matches_consider

    def __repr__(self):
        pass

    def __str__(self):
        pass

    def transform(self, data):
        pass



