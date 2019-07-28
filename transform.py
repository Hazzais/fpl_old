"""
TODO: Add argparse etc.
TODO: Add unit testing - important for automated workflow.
"""
import os
import fpltools.load as tr
from fpltools.utils import get_current_gameweek, get_next_gameweek,\
    get_previous_gameweek, get_datetime_string

GW_START = 1
GW_NOW = 37  # 'latest' when live
GW_END = None  # keep as None unless a max gameweek in the output is needed
SEASON_ID = 's201819'  # switch to 'latest' when live

if __name__ == "__main__":

    # Convert arguments to appropriate numeric if originally assigned as
    # 'latest'.
    # Needs to be done here as these are used later on
    # (not just in raw_data_load which also uses get_latest function).
    if SEASON_ID == 'latest':
        SEASON_ID = tr.get_latest('data', 'folder', regex_pattern=r's\d{6}')

    if GW_NOW == 'latest':
        GW_NOW = int(tr.get_latest(os.path.join('data', SEASON_ID), 'folder',
                                   regex_pattern=r'^GW\d+').replace('GW', ''))

    # Load pickled files
    data_main = tr.raw_data_load('data',
                                 'data_main',
                                 SEASON_ID,
                                 GW_NOW)

    data_players = tr.raw_data_load('data',
                                    'data_players_deep',
                                    SEASON_ID,
                                    GW_NOW)

    data_fixtures = tr.raw_data_load('data',
                                     'data_fixtures',
                                     SEASON_ID,
                                     GW_NOW)

    # Gameweek-level data
    # TODO: export? Run in intermediate task, export, and import and use here?
    events = tr.get_events(data_main['events'])

    # Determine relevant gameweeks. Does not always need to be consistent with
    # constants above.
    next_gameweek = get_next_gameweek(data_main['events'])
    current_gameweek = get_current_gameweek(data_main['events'])
    previous_gameweek = get_previous_gameweek(data_main['events'])

    # Current human players signed up to Fantasy Premier League
    total_players = data_main['total-players']

    # Fixture-level data. Different input data necessitates different functions
    next_fixtures = tr.get_gameweek_fixtures(data_fixtures, next_gameweek)
    all_fixtures = tr.get_fixtures(data_fixtures)

    # Can now set maximum GW for output if constant is left as default None
    if GW_END is None:
        GW_END = all_fixtures['gameweek'].astype(int).max()

    # Position-level data
    positions = tr.get_positions(data_main)

    # Player summary data - this is player-level data but for the upcoming
    # gameweek. It is less useful than later data but has a few extra variables
    player_summary = tr.get_players(data_main['elements'])

    # Team-level data
    teams = tr.get_teams(data_main['teams'])

    # More detailed player-level data
    player_history, player_future = tr.get_players_deep(data_players)

    # Use player_history as the base for the output.
    # Add team fixtures.
    player_history = tr.add_fixture_team(player_history, all_fixtures)

    # Determine team-level data which looks at teams' players performance
    team_fixtures_results = tr.team_detailed_data(all_fixtures, player_history,
                                                  prev_matches_consider=3)

    # Make the player output data by combining historical and future data for
    # each player.
    player_output = tr.add_remaining_gameweeks(player_history, player_summary,
                                               player_future, all_fixtures,
                                               total_players)

    # Add previous gameweek variables for each player to next row (i.e. prev_
    # stats).
    player_output = tr.add_lagged_columns(player_output)

    # Add the enhanced team-level data
    player_output = tr.add_team_details(player_output, team_fixtures_results)

    # Add further player-level data not already existing
    player_output = tr.add_player_reference_data(player_output,
                                                 player_summary, positions)

    # Add reference data for players' teams
    player_output = tr.add_team_reference_data(player_output, teams)

    # Handle time features to make them more usable
    player_output = tr.add_time_features(player_output)

    # Add rolling stats
    player_output = tr.add_rolling_stats(player_output,
                                         team_fixtures_results,
                                         prev_matches_consider=3)

    # Not totally necessary, but I like the columns ordered so it's easier to
    # inspect the data
    all_cols = sorted(list(player_output.columns))

    # Important columns will go first in this order
    imp_col_order = [
        'player_id',
        'first_name',
        'second_name',
        'position',
        'team_id',
        'team_short',
        'team_name',
        'team_difficulty',
        'gameweek',
        'kickoff_time',
        'kickoff_hour',
        'kickoff_hour_cos',
        'kickoff_hour_sin',
        'kickoff_hour_bin',
        'kickoff_weekday',
        'kickoff_weekday_cos',
        'kickoff_weekday_sin',
        'event_day',
        'fixture_id',
        'is_home',
        'opponent_team',
        'opponent_team_short',
        'opponent_team_name',
        'opponent_team_strength',
        'opponent_difficulty',
        'opponent_strength_ha_overall',
        'opponent_strength_ha_attack',
        'opponent_strength_ha_defence',
        'target_total_points',
        'target_minutes',
        'target_goals_scored',
        'target_goals_conceded',
        'selected',
        'value',
        'value_change',
        'custom_form',
        'transfers_balance',
        'transfers_in',
        'transfers_out',
        'team_strength',
        'team_strength_ha_overall',
        'team_strength_ha_attack',
        'team_strength_ha_defence',
    ]

    # Add important columns and then those left over
    new_col_order = imp_col_order + [col for col in all_cols if
                                     col not in imp_col_order]

    # Those columns which should be treated as numeric
    cols_to_numeric = ['target_total_points',
                       'target_minutes',
                       'target_goals_scored',
                       'selected',
                       'value',
                       'value_change',
                       'custom_form',
                       'transfers_balance',
                       'transfers_in',
                       'transfers_out',
                       'chance_of_playing_this_round',
                       'chance_of_playing_next_round',
                       'prev_total_points',
                       'prev_minutes',
                       'prev_goals_scored',
                       'prev_bonus',
                       'prev_creativity',
                       'prev_ict_index',
                       'prev_influence',
                       'prev_threat',
                       'team_prev_result_points',
                       'team_prev_mean_points',
                       'team_prev_total_points',
                       'team_prev_unique_scorers',
                       'roll_team_scored',
                       'roll_team_conceded',
                       'roll_team_points',
                       'roll_unique_scorers',
                       'roll_mean_points',
                       'roll_total_points',
                       'roll_minutes',
                       'roll_goals_scored',
                       'kickoff_hour_cos',
                       'kickoff_hour_sin',
                       'kickoff_weekday_cos',
                       'kickoff_weekday_sin',
                       'prev_kickoff_hour_cos',
                       'prev_kickoff_hour_sin',
                       'prev_kickoff_weekday_cos',
                       'prev_kickoff_weekday_sin',
                       ]
    player_output[cols_to_numeric] = player_output[cols_to_numeric].astype(
        float)

    # Those columns which should be treated as categorical - save as str to
    # avoid import problems
    cols_to_categorical = ['player_id',
                           'position',
                           'team_id',
                           'team_short',
                           'team_name',
                           'kickoff_hour',
                           'kickoff_hour_bin',
                           'kickoff_weekday',
                           'event_day',
                           'fixture_id',
                           'opponent_team',
                           'opponent_team_short',
                           'opponent_team_name',
                           'status',
                           'prev_opponent_team',
                           'prev_playergw_id',
                           'prev_kickoff_hour',
                           'prev_kickoff_hour_bin',
                           'prev_kickoff_weekday',
                           ]
    player_output[cols_to_categorical] = player_output[
        cols_to_categorical].astype(str)

    # Final output is our player dataset with the columns ordered. Also take
    # into account the requested start gameweek here (as rolling values may
    # require earlier gameweeks when created in the code above)
    gw_logic = (player_output['gameweek'].astype(int) >= GW_START) &\
               (player_output['gameweek'].astype(int) <= GW_END + 1)

    player_output = player_output.loc[gw_logic, new_col_order]

    out_path = os.path.join('data', SEASON_ID, 'cleaned')
    out_file = 'player_gameweek_data_GW' +\
               str(GW_NOW) +\
               str(get_datetime_string()) +\
               '.csv'
    player_output.to_csv(os.path.join(out_path, out_file), index=False)
