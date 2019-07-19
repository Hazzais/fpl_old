import json
import requests


def load_credentials(file):
    """ Load credentials from external text file.

    Structure taken from https://stackoverflow.com/questions/4803999/
        how-to-convert-a-file-into-a-dictionary
    """
    credentials = {}
    with open(file, 'r') as f:
        for line in f:
            (k, v) = line.split(',')
            credentials[k] = v
    return credentials


def retrieve_data(link):
    """ Retrieve the JSON data from the official FPL API and return as a dict
    """
    response = requests.get(link)
    json_data = json.loads(response.text)
    return json_data


def retrieve_player_details(link, player_ids, verbose=False):
    # More complicated - for each player - retrieve a dictionary of their data
    players_full = {}
    for i, pl in enumerate(player_ids):
        if verbose and i % 10 == 0:
            print("Player number: " + str(i) + " of " + str(len(player_ids)))

        player_id = pl['id']
        players_full[player_id] = retrieve_data(link.format(player_id))

    return players_full
