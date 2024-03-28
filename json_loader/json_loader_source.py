import json
import os
import psycopg2
import pandas as pd

def load_competitions_to_db(json_filepath, db_params):
    # Load JSON data
    with open(json_filepath, 'r') as file:
        data = json.load(file)

    # Filter for the specified seasons
    specified_seasons = ['La Liga 2020/2021', 'La Liga 2019/2020', 'La Liga 2018/2019', 'Premier League 2003/2004']
    filtered_data = [d for d in data if (
        (d['competition_name'] == 'La Liga' and d['season_name'] in ['2020/2021', '2019/2020', '2018/2019']) 
        or (d['competition_name'] == 'Premier League' and d['season_name'] == '2003/2004')
    )]

    # Convert to DataFrame for easier processing
    df = pd.DataFrame(filtered_data)

    # Select only the necessary columns
    df = df[['competition_id', 'season_id', 'competition_name', 'competition_gender', 'country_name', 'season_name']]

    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Insert data into the database
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO competitions (competition_id, season_id, competition_name, competition_gender, country_name, season_name) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (competition_id, season_id) DO UPDATE SET
            competition_name = EXCLUDED.competition_name,
            competition_gender = EXCLUDED.competition_gender,
            country_name = EXCLUDED.country_name,
            season_name = EXCLUDED.season_name;
        """, (row.competition_id, row.season_id, row.competition_name, row.competition_gender, row.country_name, row.season_name))

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def load_teams_data(matches_data, cursor):
    teams = set()
    for match in matches_data:
        teams.add((match['home_team']['home_team_id'], match['home_team']['home_team_name']))
        teams.add((match['away_team']['away_team_id'], match['away_team']['away_team_name']))

    for team_id, name in teams:
        cursor.execute("""
            INSERT INTO teams (team_id, name)
            VALUES (%s, %s)
            ON CONFLICT (team_id) DO NOTHING;
        """, (team_id, name))

def load_stadiums_data(matches_data, cursor):
    stadiums = set()
    for match in matches_data:
        stadium = match.get('stadium')
        if stadium:
            stadiums.add((stadium['id'], stadium['name'], stadium['country']['name']))

    for stadium_id, name, country in stadiums:
        cursor.execute("""
            INSERT INTO stadiums (stadium_id, name, country)
            VALUES (%s, %s, %s)
            ON CONFLICT (stadium_id) DO NOTHING;
        """, (stadium_id, name, country))

def load_referees_data(matches_data, cursor):
    referees = set()
    for match in matches_data:
        referee = match.get('referee')
        if referee:
            referees.add((referee['id'], referee['name'], referee['country']['name']))

    for referee_id, name, country in referees:
        cursor.execute("""
            INSERT INTO referees (referee_id, name, country)
            VALUES (%s, %s, %s)
            ON CONFLICT (referee_id) DO NOTHING;
        """, (referee_id, name, country))

def load_competition_stages_data(matches_data, cursor):
    stages = set()
    for match in matches_data:
        stage = match['competition_stage']
        stages.add((stage['id'], stage['name']))

    for stage_id, name in stages:
        cursor.execute("""
            INSERT INTO competition_stages (stage_id, name)
            VALUES (%s, %s)
            ON CONFLICT (stage_id) DO NOTHING;
        """, (stage_id, name))

def load_matches_data(matches_data, cursor):
    for match in matches_data:
        # Extract the stadium_id, handle if missing
        stadium_id = match['stadium']['id'] if 'stadium' in match and match['stadium'] is not None else None

        # Extract the referee_id, handle if missing
        referee_id = match.get('referee', {}).get('id', None)

        # Insert the match data into the database
        cursor.execute("""
            INSERT INTO matches (match_id, competition_id, season_id, match_date, kick_off, home_team_id, away_team_id, home_score, away_score, match_week, competition_stage_id, stadium_id, referee_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (match_id) DO NOTHING;
        """, (
            match['match_id'], match['competition']['competition_id'], match['season']['season_id'], 
            match['match_date'], match['kick_off'], match['home_team']['home_team_id'], 
            match['away_team']['away_team_id'], match['home_score'], match['away_score'], 
            match['match_week'], match['competition_stage']['id'], stadium_id, referee_id
        ))

# get ids & json data for matches
def load_all_match_data(db_params):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Get all competition_id and season_id pairs
    cursor.execute("SELECT competition_id, season_id FROM competitions")
    competition_season_pairs = cursor.fetchall()

    for competition_id, season_id in competition_season_pairs:
        json_filepath = f'data/matches/{competition_id}/{season_id}.json'
        
        # Load JSON data
        try:
            with open(json_filepath, 'r') as file:
                matches_data = json.load(file)

            # Call your data loading functions
            load_teams_data(matches_data, cursor)
            load_stadiums_data(matches_data, cursor)
            load_referees_data(matches_data, cursor)
            load_competition_stages_data(matches_data, cursor)
            load_matches_data(matches_data, cursor)

        except FileNotFoundError:
            print(f"File not found: {json_filepath}")

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# get ids & json data for events
def load_all_events_data(db_params):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Retrieve all match_ids from the matches table
    cursor.execute("SELECT match_id FROM matches;")
    match_ids = cursor.fetchall()

    # Iterate over each match_id and load its events data
    for match_id in match_ids:
        file_path = f'data/events/{match_id[0]}.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                events_data = json.load(file)
                load_events_data(match_id[0], events_data, cursor)
    
    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# Insert data into the events and related tables
def load_events_data(match_id, events_data, cursor):
    for event in events_data:
        # Insert or ignore into players, teams, positions, event_types
        player_id = event.get('player', {}).get('id')
        if player_id:
            cursor.execute("INSERT INTO players (player_id, name) VALUES (%s, %s) ON CONFLICT (player_id) DO NOTHING;",
                           (player_id, event['player'].get('name')))

        team_id = event.get('team', {}).get('id')
        if team_id:
            cursor.execute("INSERT INTO teams (team_id, name) VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING;",
                           (team_id, event['team'].get('name')))

        position_id = event.get('position', {}).get('id')
        if position_id:
            # Check if position_id exists in the positions table
            cursor.execute("SELECT EXISTS(SELECT 1 FROM positions WHERE position_id = %s)", (position_id,))
            if cursor.fetchone()[0]:
                # If position_id exists, use it
                valid_position_id = position_id
            else:
                # If position_id does not exist, use NULL
                valid_position_id = None
        else:
            valid_position_id = None


        type_id = event['type']['id']
        cursor.execute("INSERT INTO event_types (type_id, name) VALUES (%s, %s) ON CONFLICT (type_id) DO NOTHING;",
                       (type_id, event['type']['name']))

        # Insert into events table
        cursor.execute("""
            INSERT INTO events (event_id, match_id, period, timestamp, minute, second, possession, type_id, player_id, position_id, team_id, location, related_events)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """, (
            event['id'], match_id, event['period'], event['timestamp'], event['minute'], event['second'],
            event['possession'], type_id, player_id, valid_position_id, team_id, json.dumps(event.get('location')),
            json.dumps(event.get('related_events'))
        ))

# get ids & json data for lineups
def load_all_lineups_data(db_params):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Get match_ids from matches table
    cursor.execute("SELECT match_id FROM matches;")
    match_ids = cursor.fetchall()

    # Iterate over match_ids and load lineup data
    for match_id in match_ids:
        file_path = f'data/lineups/{match_id[0]}.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                lineup_data = json.load(file)
            load_lineups_data(match_id, lineup_data, cursor)

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

# Insert data into lineups and related tables
def load_lineups_data(match_id, lineup_data, cursor):
    for team in lineup_data:
        team_id = team['team_id']
        # Ensure team is in teams table
        cursor.execute("INSERT INTO teams (team_id, name) VALUES (%s, %s) ON CONFLICT (team_id) DO NOTHING;", 
                       (team_id, team['team_name']))

        for player in team['lineup']:
            player_id = player['player_id']
            country_id = player['country']['id']
            country_name = player['country']['name']
            nickname = player.get('player_nickname')

            # Ensure country is in countries table
            cursor.execute("""INSERT INTO countries (country_id, country_name) 
                              VALUES (%s, %s) ON CONFLICT (country_id) DO NOTHING;""", 
                              (country_id, country_name))

            # Ensure player is in players table
            cursor.execute("""INSERT INTO players (player_id, name, nickname, country_id) 
                           VALUES (%s, %s, %s, %s) ON CONFLICT (player_id) DO NOTHING;""", 
                           (player_id, player['player_name'], nickname, country_id))

            for position in player.get('positions', []):
                position_id = position['position_id']
                # Ensure position is in positions table
                cursor.execute("""INSERT INTO positions (position_id, position_name) 
                               VALUES (%s, %s) ON CONFLICT (position_id) DO NOTHING;""", 
                               (position_id, position['position']))

                # Insert into lineups table
                cursor.execute("""INSERT INTO lineups (match_id, team_id, player_id, jersey_number, position_id) 
                               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (match_id, team_id, player_id) DO NOTHING;""", 
                               (match_id, team_id, player_id, player['jersey_number'], position_id))

# Fill in details
db_parameters = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': ''
}

# USAGE
#load_competitions_to_db('data/competitions.json', db_parameters)
#load_all_match_data(db_parameters)
# load_all_events_data(db_parameters)
# load_all_lineups_data(db_parameters)