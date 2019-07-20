import os
import pickle
import pandas as pd
import numpy as np
import sqlite3
from copy import deepcopy
import re


def raw_data_load(in_dir,
                  in_prefix,
                  season_id='latest',
                  gameweek='latest',
                  datetime_id='latest'):

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

