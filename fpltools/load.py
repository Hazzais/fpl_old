"""
TODO: Full docstrings
TODO: Improve comments
TODO: investigate OOP rather than functional approach
Key ID variables:
  - gameweek (cat)
  - code (int) => fixture_id_long (cat)
  - [fixture] id (int) => fixture_id (cat)
  - team (int) => team_id (cat) [same with team_a and team_h]
"""

import os
import pickle
import pandas as pd
import numpy as np
from copy import deepcopy
import re


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


def replace_nonetype_in_dict(thedict):
    # Replace values which are lists or NoneTypes with numpy nans
    return {k: (np.nan if v is None or isinstance(v, (list,)) else v)
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
        try:
            del pl['chip_plays']
        except KeyError:
            pass
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
    outlist = [x for x in data if x['event'] == gameweek]
    outdf = pd.DataFrame(outlist)
    outdf['kickoff_datetime'] = pd.to_datetime(outdf['kickoff_time'],
                                               errors='coerce')
    outdf['min_date'] = outdf['kickoff_datetime'].min()
    outdf['event_day'] = (outdf['kickoff_datetime'].dt.date
                          - outdf['min_date'].dt.date).dt.days + 1
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
                  'link_url'
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
            if k == 'current_event_fixture' and v:
                v_curr = {'curr_game_' + x: y for (x, y) in pl[k][0].items()}
                for k2, v2 in v_curr.items():
                    teams.loc[team_id, k2] = v2
            # If no current fixture (team has no game this gameweek), ignore
            elif k == 'current_event_fixture':
                pass
            # Get data for next fixture from list containing dict. If two
            # elements (double gameweek), take only the first (scope of this
            # project )
            elif k == 'next_event_fixture' and v:
                v_next = {'next_game_' + x: y for (x, y) in pl[k][0].items()}
                for k2, v2 in v_next.items():
                    teams.loc[team_id, k2] = v2
            # If no next fixture (team has no game this gameweek), ignore
            elif k == 'next_event_fixture':
                pass
            elif v is None:
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
                                   'fixture': 'fixture_id'},
                          inplace=True)
    player_future.rename(columns={'id': 'fixture_id',
                                  'code': 'fixture_id_long',
                                  'event': 'gameweek'},
                         inplace=True)

    player_future['team_h'] = player_future['team_h'].astype(str)
    player_future['team_a'] = player_future['team_a'].astype(str)

    player_history['fixture_id'] = player_history['fixture_id'].astype(str)
    player_future['fixture_id_long'] = player_future['fixture_id_long']\
        .astype(str)

    player_history = cols_to_front(player_history,
                                   ['player_id', 'gameweek', 'fixture_id'])
    player_future = cols_to_front(player_future,
                                  ['player_id', 'gameweek', 'fixture_id_long'])

    return player_history, player_future


def get_fixtures(data):
    # Get all fixtures data
    fixtures = pd.DataFrame()

    # Make dict into DataFrame - stats can cause problems and is done
    # separately
    for vdict in data:
        vdict_nostats = deepcopy(vdict)
        del vdict_nostats['stats']

        game_id = vdict['code']
        df_row = pd.DataFrame(vdict_nostats, index=[game_id])

        fixtures = fixtures.append(df_row)

    fixtures.rename(columns={'code': 'fixture_id_long',
                             'id': 'fixture_id',
                             'event': 'gameweek'}, inplace=True)

    fixtures['team_h'] = fixtures['team_h'].astype(str)
    fixtures['team_a'] = fixtures['team_a'].astype(str)
    fixtures['fixture_id_long'] = fixtures['fixture_id_long'].astype(str)
    fixtures['fixture_id'] = fixtures['fixture_id'].astype(str)
    fixtures['gameweek'] = fixtures['gameweek'].astype(str)

    return cols_to_front(fixtures, ['fixture_id', 'fixture_id_long',
                                    'gameweek'])


def get_positions(data):
    positions = pd.DataFrame(data['element_types'])
    positions['position_id'] = positions['id'].astype(str)
    positions.drop(columns=['id'], inplace=True)
    return cols_to_front(positions, ['position_id'])


def add_fixture_team(data, fixtures):
    # Add team. For merging some casting of keys to integers is needed due to
    # the way values are being stored. Merge based on the fixture and whether
    # the player played home or away that fixture.
    data_out = data.copy()
    # data_out['fixture_id'] = data_out['fixture_id'].astype(int)
    data_out = data_out.merge(fixtures[['fixture_id', 'team_a', 'team_h']],
                              how='left', on='fixture_id')
    temp_team_id = np.where(data_out['was_home'],
                            data_out['team_h'],
                            data_out['team_a'])
    # Add team based upon the fixture and home/away
    data_out.insert(1, 'team_id', temp_team_id)
    return data_out


def team_detailed_data(fixtures, player_full_set, prev_matches_consider=3,
                       gameweek_upper=None):

    # Make a new dataframe containing one row per team per game showing stats
    fixt_cols = ['gameweek',
                 'fixture_id',
                 'team_h',
                 'team_h_difficulty',
                 'team_h_score',
                 'team_a',
                 'team_a_difficulty',
                 'team_a_score',
                 'kickoff_time',
                 'event_day',
                 'game_datetime']

    # Subset to gameweeks to use
    # use_fixtures = fixtures.loc[
    #     (fixtures['gameweek'] >= gameweek_start_true)
    #     & (fixtures['gameweek'] <= gameweek_end + 1)]
    if gameweek_upper is not None:
        use_fixtures = fixtures.loc[(fixtures['gameweek'] <=
                                     gameweek_upper + 1)]
    else:
        use_fixtures = fixtures.copy()

    use_fixtures['game_datetime'] = pd.to_datetime(
        use_fixtures['kickoff_time'])
    min_gw_dates = use_fixtures.groupby('gameweek')['game_datetime'].min()\
        .reset_index().rename(columns={'game_datetime': 'first_ko'})
    use_fixtures = use_fixtures.merge(min_gw_dates, on='gameweek', how='left')
    use_fixtures['day_game'] = use_fixtures['game_datetime'].dt.day
    use_fixtures['day_min'] = use_fixtures['first_ko'].dt.day
    use_fixtures['event_day'] =\
        use_fixtures['day_game'] - use_fixtures['day_min']
    use_fixtures.drop(columns=['day_game', 'day_min', 'first_ko'],
                      inplace=True)

    # Need to concatenate home and away data to get both teams
    team_fixtures_results_home = use_fixtures[fixt_cols].rename(
        columns={'team_h': 'team_id',
                 'team_a': 'opponent_team',
                 'team_h_difficulty': 'team_difficulty',
                 'team_a_difficulty': 'opponent_difficulty',
                 'team_h_score': 'team_scored',
                 'team_a_score': 'team_conceded'})
    team_fixtures_results_home['is_home'] = True
    team_fixtures_results_away = use_fixtures[fixt_cols].rename(
        columns={'team_a': 'team_id',
                 'team_h': 'opponent_team',
                 'team_a_difficulty': 'team_difficulty',
                 'team_h_difficulty': 'opponent_difficulty',
                 'team_a_score': 'team_scored',
                 'team_h_score': 'team_conceded'})
    team_fixtures_results_away['is_home'] = False

    team_fixtures_results = pd.concat(
        [team_fixtures_results_home, team_fixtures_results_away], sort=False)
    team_fixtures_results.gameweek = team_fixtures_results.gameweek.astype(int)
    team_fixtures_results.sort_values(['team_id', 'gameweek', 'game_datetime'],
                                      inplace=True)
    team_fixtures_results.gameweek = team_fixtures_results.gameweek.astype(str)

    # originally single row was here ####

    # Add additional stats including goals, results, points, and number of
    # players and scorers.
    tsc = ['team_scored', 'team_conceded']
    ts = 'team_scored'
    tc = 'team_conceded'
    team_fixtures_results[tsc] = team_fixtures_results[tsc].astype(float)
    team_fixtures_results['team_win'] =\
        team_fixtures_results[ts] > team_fixtures_results[tc]
    team_fixtures_results['team_draw'] =\
        team_fixtures_results[ts] == team_fixtures_results[tc]
    team_fixtures_results['team_loss'] =\
        team_fixtures_results[ts] < team_fixtures_results[tc]
    team_fixtures_results['points'] = np.where(
        ~team_fixtures_results[tc].isna(),
        3 * team_fixtures_results['team_win'] + team_fixtures_results[
            'team_draw'],
        np.nan)

    # Determine number of players playing and scoring
    unique_scorers = player_full_set.loc[
        player_full_set.goals_scored >= 1, ['team_id', 'player_id',
                                            'gameweek']]
    n_scorers = unique_scorers.groupby(
        ['team_id', 'gameweek']).size().reset_index().rename(
        columns={0: 'unique_scorers'})
    n_scorers['gameweek'] = n_scorers['gameweek'].astype(str)
    unique_players = player_full_set.loc[
        player_full_set.minutes > 0, ['team_id', 'player_id', 'gameweek',
                                      'total_points']]
    unique_players['total_points'] = unique_players['total_points'].astype(int)

    # Get number and mean points per team per game
    total_scores = unique_players.groupby(['team_id', 'gameweek'])[
        'total_points'].agg(
        ['mean', 'sum']).reset_index().rename(
        columns={'mean': 'team_mean_points', 'sum': 'team_total_points'})
    total_scores['gameweek'] = total_scores['gameweek'].astype(str)

    # Add the above to the results dataframe
    team_fixtures_results = team_fixtures_results.merge(total_scores,
                                                        how='left',
                                                        on=['team_id',
                                                            'gameweek'])
    team_fixtures_results = team_fixtures_results.merge(n_scorers, how='left',
                                                        on=['team_id',
                                                            'gameweek'])
    team_fixtures_results.loc[~team_fixtures_results['team_scored'].isna(),
                              'unique_scorers'] = \
        team_fixtures_results.loc[~team_fixtures_results[
            'team_scored'].isna(), 'unique_scorers'].fillna(0)

    team_fixtures_results['gameweek_int'] = team_fixtures_results['gameweek']\
        .astype(int)
    team_fixtures_results['dtime'] = pd.to_datetime(
        team_fixtures_results['kickoff_time'], errors='coerce')
    team_fixtures_results.sort_values(['team_id', 'gameweek_int', 'dtime'],
                                      inplace=True)

    # Determine the average stats value across the last several games for each
    # team
    roll_cols = ['roll_team_scored',
                 'roll_team_conceded',
                 'roll_team_points',
                 'roll_unique_scorers',
                 'roll_mean_points',
                 'roll_total_points']
    team_fixtures_results[roll_cols] = team_fixtures_results. \
        groupby('team_id')[
        'team_scored', 'team_conceded', 'points', 'unique_scorers',
        'team_mean_points', 'team_total_points'].apply(
        lambda x: x.rolling(center=False, window=prev_matches_consider).mean())

    # originally stats df was here ####

    return team_fixtures_results


def add_remaining_gameweeks(data, data_summary, data_future, data_fixtures,
                            total_players):
    players = data.copy()
    players_summary = data_summary.copy()
    players_future = data_future.copy()
    fixtures = data_fixtures.copy()

    # Columns from future player fixtures to add
    keep_cols = ['player_id',
                 'gameweek',
                 'fixture_id',
                 'is_home',
                 'kickoff_time',
                 'kickoff_time_formatted',
                 'team_a',
                 'team_h']

    # Combine previous gameweeks and unplayed ones. There are some columns
    # which will not be in the future gameweeks which can be determined from
    # other columns. For example, determine the players teams and opponents
    # from the row matches' home and away teams and the home/away flag.
    players = pd.concat((players, players_future[keep_cols]), sort=False)
    players.loc[(players.team_id.isna()) & (players.is_home), 'team_id'] =\
        players.loc[(players.team_id.isna()) & (players.is_home), 'team_h']
    players.loc[(players.team_id.isna()) &
                (players.is_home == False), 'team_id'] =\
        players.loc[(players.team_id.isna()) & (players.is_home == False),
                    'team_a']
    players.loc[(players.opponent_team.isna()) &
                (players.is_home), 'opponent_team'] =\
        players.loc[(players.opponent_team.isna()) &
                    (players.is_home), 'team_a']
    players.loc[(players.opponent_team.isna()) &
                (players.is_home == False), 'opponent_team'] =\
        players.loc[(players.opponent_team.isna()) &
                    (players.is_home == False), 'team_h']
    players.loc[(players.team_id.isna()), 'was_home'] =\
        players.loc[(players.team_id.isna()) & (players.is_home), 'is_home']

    # Add a flag to highlight whether a gameweek has started and finished yet
    players = players.merge(fixtures[['fixture_id', 'started', 'finished']],
                            on='fixture_id')

    # Need to sort to get order of games for each player. Use the kickoff date
    # time as a third sort variable to account for double gameweeks.
    players['dtime'] = pd.to_datetime(players['kickoff_time'], errors='coerce')
    players.sort_values(['player_id', 'gameweek', 'dtime', 'fixture_id'],
                        inplace=True)

    # A row for the next game (max one per gameweek per player) per player and
    # add a flag to indicate this for later combination with main dataset.
    next_game_per_player = players.loc[~players.started,
                    ['player_id', 'gameweek', 'fixture_id']]\
        .groupby(['player_id', 'gameweek', 'fixture_id']).head(1)
    next_game_per_player['next_game'] = True

    # Add to the next game rows the estimated percentage ownership, absolute
    # ownership, and transfer stats
    add_latest = players_summary[['player_id',
                                  'now_cost',
                                  'selected_by_percent',
                                  'chance_of_playing_this_round',
                                  'chance_of_playing_next_round',
                                  'status',
                                  'news',
                                  'transfers_in',
                                  'transfers_out']].copy()

    # Calculate number selecting from total players and mean
    selected_as_pct = add_latest['selected_by_percent'].astype(float) / 100
    add_latest.loc[:, 'selected'] = np.round(total_players * selected_as_pct)\
        .astype(int)
    add_latest.rename(columns={'now_cost': 'value'}, inplace=True)
    add_latest['transfers_balance'] = \
        add_latest['transfers_in'] - add_latest['transfers_out']
    add_latest.drop(columns=['selected_by_percent'], inplace=True)

    # Add stats to next game row here
    next_game_per_player = next_game_per_player.merge(add_latest,
                                                      how='left',
                                                      on='player_id')

    # Finally, merge on these new stats. As these might already exist in the
    # previous gameweeks data, need to take the first non-missing.
    # TODO: make this approach better.
    players = players.merge(next_game_per_player, how='left',
                             on=['player_id', 'gameweek', 'fixture_id'])
    players['next_game'].fillna(False, inplace=True)
    players['selected'] = players.selected_x.combine_first(players.selected_y)
    players['value'] = players.value_x.combine_first(players.value_y)
    players['transfers_balance'] =\
        players.transfers_balance_x.combine_first(players.transfers_balance_y)
    players['transfers_in'] =\
        players.transfers_in_x.combine_first(players.transfers_in_y)
    players['transfers_out'] =\
        players.transfers_out_x.combine_first(players.transfers_out_y)
    drop_cols = [col for col in players.columns if col.endswith(('_x', '_y'))]
    players.drop(columns=drop_cols + ['dtime', 'is_home'], inplace=True)
    return players


def add_lagged_columns(data):
    player_full_set = data.copy()
    # Columns in which we need to lag the values (i.e. bring to player's next
    # row). I.e. these features for a game should be those from the previous
    # game
    lag_cols = ['total_points',
                'minutes',
                'goals_scored',
                'bonus',
                'opponent_team',
                'assists',
                'attempted_passes',
                'big_chances_created',
                'big_chances_missed',
                'bps',
                'clean_sheets',
                'clearances_blocks_interceptions',
                'completed_passes',
                'creativity',
                'dribbles',
                'ea_index',
                'errors_leading_to_goal',
                'errors_leading_to_goal_attempt',
                'fouls',
                'goals_conceded',
                'ict_index',
                'influence',
                'key_passes',
                'kickoff_time',
                'kickoff_time_formatted',
                'offside',
                'open_play_crosses',
                'own_goals',
                'penalties_conceded',
                'penalties_missed',
                'penalties_saved',
                'recoveries',
                'red_cards',
                'saves',
                'tackled',
                'tackles',
                'target_missed',
                'team_a_score',
                'team_h_score',
                'threat',
                'was_home',
                'winning_goals',
                'yellow_cards'
                ]

    # Columns we may potentially predict. Add prefix to mark them.
    target_cols = ['total_points',
                   'goals_scored',
                   'goals_conceded',
                   'minutes']
    target_cols_rename = {col: 'target_' + str(col) for col in target_cols}

    # Get rid of columns. All those columns to delete include the original
    # names we are lagging
    del_cols = [col for col in lag_cols if col not in target_cols] +\
               ['loaned_in', 'loaned_out', 'team_a', 'team_h']

    # Add prefix to mark lagged columns as values from the previous gameweek
    lagged_cols = ['prev_' + str(col) for col in lag_cols]

    # Perform the lag, drop the original columns, and rename the targets
    player_full_set[lagged_cols] =\
        player_full_set.groupby('player_id')[lag_cols].shift(1)
    player_full_set.drop(columns=del_cols, inplace=True)
    player_full_set.rename(columns=target_cols_rename, inplace=True)
    return player_full_set


def add_team_details(data, team_fixtures_results):
    player_full_set = data.copy()
    # Add team details to the player dataset
    tfr_cols = ['fixture_id',
                'team_id',
                'team_difficulty',
                'opponent_team',
                'opponent_difficulty',
                'kickoff_time',
                'event_day',
                'is_home']

    player_full_set = player_full_set.merge(team_fixtures_results[tfr_cols],
                                            how='left',
                                            on=['team_id', 'fixture_id'])

    player_full_set['prev_team_score'] =\
        np.where(player_full_set.prev_was_home,
                 player_full_set.prev_team_h_score,
                 player_full_set.prev_team_a_score)
    player_full_set['prev_opponent_score'] =\
        np.where(player_full_set.prev_was_home is False,
                 player_full_set.prev_team_h_score,
                 player_full_set.prev_team_a_score)
    player_full_set['prev_win'] =\
        player_full_set.prev_team_score > player_full_set.prev_opponent_score
    player_full_set['prev_draw'] =\
        player_full_set.prev_team_score == player_full_set.prev_opponent_score
    player_full_set['prev_loss'] =\
        player_full_set.prev_team_score < player_full_set.prev_opponent_score

    player_full_set.drop(columns=['prev_was_home',
                                  'prev_team_a_score',
                                  'prev_team_h_score'], inplace=True)
    return player_full_set


def add_player_reference_data(data, player_summary, positions):
    player_full_set = data.copy()
    positions_copy = positions[['position_id', 'singular_name_short']].copy()
    positions_copy.rename(columns={'singular_name_short': 'position'},
                          inplace=True)
    # Add player summary columns, including reference info like names as well
    # as position. This is constant data throughout the season
    cols_player_details = ['player_id',
                           'position_id',
                           'first_name',
                           'second_name',
                           ]

    player_full_set =\
        player_full_set.merge(player_summary[cols_player_details],
                              how='left',
                              on='player_id')
    # Add position
    # Something strange is happening when merging the keys when they are a
    # string (not even categoricals) so temporarily convert to ints
    player_full_set['position_id'] = player_full_set['position_id'].astype(int)
    positions_copy['position_id'] = positions_copy['position_id'].astype(int)
    player_full_set =\
        player_full_set.merge(positions_copy,
                              how='left',
                              on='position_id').drop(columns=['position_id'])
    return player_full_set


def add_team_reference_data(data, data_teams):
    player_full_set = data.copy()
    teams = data_teams.copy()
    # Add team reference data and strength
    cols_teams = ['team_id',
                  'short_name',
                  'name',
                  'strength',
                  ] + [col for col in teams if col.startswith('strength_')]
    rn_dict = {'short_name': 'team_short',
               'name': 'team_name',
               'strength': 'team_strength'}
    player_full_set =\
        player_full_set.merge(teams[cols_teams].rename(columns=rn_dict),
                              how='left',
                              on='team_id')

    # Split up team strength between home and away for each player's home and
    # away fixtures
    player_full_set['team_strength_ha_overall'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_overall_home'],
                 player_full_set['strength_overall_away'])
    player_full_set['team_strength_ha_attack'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_attack_home'],
                 player_full_set['strength_attack_away'])
    player_full_set['team_strength_ha_defence'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_defence_home'],
                 player_full_set['strength_defence_away'])
    player_full_set.drop(columns=[col for col in player_full_set
                                  if col.startswith('strength_')],
                         inplace=True)

    # Do the above but for the opponent
    rn_dict_opp = {'short_name': 'opponent_team_short',
                   'name': 'opponent_team_name',
                   'strength': 'opponent_team_strength',
                   'team_id': 'opponent_team'}
    player_full_set =\
        player_full_set.merge(teams[cols_teams].rename(columns=rn_dict_opp),
                              how='left',
                              on='opponent_team')
    player_full_set['opponent_strength_ha_overall'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_overall_home'],
                 player_full_set['strength_overall_away'])
    player_full_set['opponent_strength_ha_attack'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_attack_home'],
                 player_full_set['strength_attack_away'])
    player_full_set['opponent_strength_ha_defence'] =\
        np.where(player_full_set.is_home,
                 player_full_set['strength_defence_home'],
                 player_full_set['strength_defence_away'])
    player_full_set.drop(columns=[col for col in player_full_set
                                  if col.startswith('strength_')],
                         inplace=True)
    return player_full_set


def add_time_features(data):
    player_full_set = data.copy()
    # Make features from kickoff times - first convert text to datetime
    player_full_set['kickoff_datetime'] =\
        pd.to_datetime(player_full_set['kickoff_time'], errors='coerce')
    player_full_set['prev_kickoff_datetime'] =\
        pd.to_datetime(player_full_set['prev_kickoff_time'], errors='coerce')

    # Function to bin hours
    def hour_to_bin(h):
        # TODO: there should be a better way to do this
        if h < 12:
            r = 'morning'
        elif h < 15:
            r = 'midday'
        elif h < 19:
            r = 'afternoon'
        else:
            r = 'evening'
        return r

    # Determine the hour, bin, and weekday of this and the previous game
    player_full_set['kickoff_hour'] =\
        player_full_set['kickoff_datetime'].dt.hour
    player_full_set['kickoff_hour_bin'] =\
        player_full_set['kickoff_hour'].apply(hour_to_bin)
    player_full_set['kickoff_weekday'] =\
        player_full_set['kickoff_datetime'].dt.weekday
    player_full_set['prev_kickoff_hour'] =\
        player_full_set['prev_kickoff_datetime'].dt.hour
    player_full_set['prev_kickoff_hour_bin'] =\
        player_full_set['prev_kickoff_hour'].apply(hour_to_bin)
    player_full_set['prev_kickoff_weekday'] =\
        player_full_set['prev_kickoff_datetime'].dt.weekday

    # Convert features into cyclic ones (hours)
    h_const = 2 * np.pi / 24
    player_full_set['kickoff_hour_cos'] =\
        np.cos(h_const * player_full_set['kickoff_hour'].astype(float))
    player_full_set['kickoff_hour_sin'] =\
        np.sin(h_const * player_full_set['kickoff_hour'].astype(float))
    player_full_set['prev_kickoff_hour_cos'] =\
        np.cos(h_const * player_full_set['prev_kickoff_hour'].astype(float))
    player_full_set['prev_kickoff_hour_sin'] =\
        np.sin(h_const * player_full_set['prev_kickoff_hour'].astype(float))

    # Convert features into cyclic ones (weekdays)
    w_const = 2 * np.pi / 7
    player_full_set['kickoff_weekday_cos'] =\
        np.cos(w_const * player_full_set['kickoff_weekday'].astype(float))
    player_full_set['kickoff_weekday_sin'] =\
        np.sin(w_const * player_full_set['kickoff_weekday'].astype(float))
    player_full_set['prev_kickoff_weekday_cos'] =\
        np.cos(w_const * player_full_set['prev_kickoff_weekday'].astype(float))
    player_full_set['prev_kickoff_weekday_sin'] =\
        np.sin(w_const * player_full_set['prev_kickoff_weekday'].astype(float))
    return player_full_set


def add_rolling_stats(data, data_teams, prev_matches_consider=3):
    player_full_set = data.copy()
    # =team_fixtures_results
    # Full team stats per gameweek including previous averages
    roll_cols = ['roll_team_scored',
                 'roll_team_conceded',
                 'roll_team_points',
                 'roll_unique_scorers',
                 'roll_mean_points',
                 'roll_total_points']

    add_team_cols = ['points',
                     'team_mean_points',
                     'team_total_points',
                     'unique_scorers',
                     ]
    team_stats_add = data_teams[
        ['team_id', 'gameweek'] + add_team_cols + roll_cols].copy()
    team_stats_add[add_team_cols + roll_cols] = \
        team_stats_add.groupby(['team_id'])[add_team_cols + roll_cols].shift(1)
    rn_dict = {'points': 'team_prev_result_points',
               'team_mean_points': 'team_prev_mean_points',
               'team_total_points': 'team_prev_total_points',
               'unique_scorers': 'team_prev_unique_scorers'}
    team_stats_add.rename(columns=rn_dict,
                          inplace=True)
    team_stats_add['gameweek'] = team_stats_add['gameweek'].astype(int)
    player_full_set['gameweek'] = player_full_set['gameweek'].astype(int)
    team_stats_add['team_id'] = team_stats_add['team_id'].astype(int)
    player_full_set['team_id'] = player_full_set['team_id'].astype(int)

    # Add previous and rolling team stats
    player_full_set = player_full_set.merge(team_stats_add.groupby(['team_id',
                                                                    'gameweek']
                                                                   ).head(1),
                                            how='left',
                                            on=['team_id', 'gameweek'])
    player_full_set['gameweek'] = player_full_set['gameweek'].astype(str)
    player_full_set['team_id'] = player_full_set['team_id'].astype(str)

    # Add change in value
    player_full_set['value_change'] =\
        player_full_set.groupby('player_id')['value'].diff(1)

    # New variables created by average of previous ones. Create a 'form'
    # variable from previous bps. Also see how much they previously played and
    # scored.
    new_cols = ['custom_form', 'roll_minutes', 'roll_goals_scored']
    player_full_set[new_cols] = player_full_set.groupby('player_id')[
        'prev_bps',
        'prev_minutes',
        'prev_goals_scored']\
        .apply(lambda x: x.rolling(center=False, window=prev_matches_consider)
               .mean())
    return player_full_set


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

    def _add_fixture_team(self):
        pass
