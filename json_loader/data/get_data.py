import requests
import os
import psycopg2

# Download match data
def download_matches_data(competitions, base_url):
    for comp in competitions:
        comp_id = comp['competition_id']
        season_id = comp['season_id']

        # The file path and directory path
        file_path = f'data/matches/{comp_id}/{season_id}.json'
        dir_path = os.path.dirname(file_path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # Full URL to the file
        file_url = f'{base_url}{file_path}'

        # Download the file
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f'Downloaded {file_path}')
        else:
            print(f'Failed to download {file_path}: HTTP {response.status_code}')

# Download event data
def download_events_data(db_params, base_url):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Retrieve all match IDs from the matches table
    cursor.execute("SELECT match_id FROM matches;")
    match_ids = cursor.fetchall()

    for match_id in match_ids:
        # Format match_id and construct file path
        match_id_str = str(match_id[0])
        file_path = f'data/events/{match_id_str}.json'
        dir_path = os.path.dirname(file_path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # Full URL to the file
        file_url = f'{base_url}data/events/{match_id_str}.json'

        # Download the file
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f'Downloaded events data for match_id {match_id_str}')
        else:
            print(f'Failed to download data for match_id {match_id_str}: HTTP {response.status_code}')

    # Close the database connection
    cursor.close()
    conn.close()

# download Lineup data
def download_lineups_data(db_params, base_url):
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Retrieve all match IDs from the matches table
    cursor.execute("SELECT match_id FROM matches;")
    match_ids = cursor.fetchall()

    for match_id in match_ids:
        # Format match_id and construct file path
        match_id_str = str(match_id[0])
        file_path = f'data/lineups/{match_id_str}.json'
        dir_path = os.path.dirname(file_path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # Full URL to the file
        file_url = f'{base_url}data/lineups/{match_id_str}.json'

        # Download the file
        response = requests.get(file_url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f'Downloaded lineups data for match_id {match_id_str}')
        else:
            print(f'Failed to download data for match_id {match_id_str}: HTTP {response.status_code}')

    # Close the database connection
    cursor.close()
    conn.close()
# Usage
base_url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/'
competitions = [
    {'competition_id': '2', 'season_id': '44'},
    {'competition_id': '11', 'season_id': '90'},
    {'competition_id': '11', 'season_id': '42'},
    {'competition_id': '11', 'season_id': '4'}
]
db_parameters = {
    'dbname': 'project_database',
    'user': 'postgres',
    'password': 'pejmyv-1zovfy-fuSxid',
    'host': 'localhost'
}
# download_matches_data(competitions, base_url)
# download_events_data(db_parameters, base_url)
download_lineups_data(db_parameters, base_url)