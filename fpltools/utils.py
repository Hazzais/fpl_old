import os
import pickle
from datetime import datetime


def get_datetime_string():
    return round(datetime.now().timestamp())


def get_current_gameweek(data):
    return data['current-event']


class SaveData:

    def __init__(self, data, seasonid=None, save_base='data'):
        self.timestamp = get_datetime_string()
        self.gameweek = get_current_gameweek(data)
        self.seasonid = seasonid
        self.save_base = save_base

    def save_wrapper_to_disk(self, save_data):
        for k, v in save_data:
            self.save_to_disk(k, v)

    def save_to_disk(self, data, outname):
        save_dir = os.path.join(self.save_base, self.seasonid,
                                'GW' + str(self.gameweek))
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        outfile = os.path.join(save_dir, outname + '_GW' +
                               str(self.gameweek).zfill(2) + '_' +
                               str(self.timestamp) + '.pkl')
        with open(outfile, 'wb') as out:
            pickle.dump(data, out)
