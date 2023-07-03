from espn_api.football import League
import pprint
draftYear = 2022
cursor = None

import json
conn = None
pp = pprint.PrettyPrinter(width=41, compact=True)
league = League(league_id=684556, year=draftYear, espn_s2='AEAfGvrTc9vkVt9%2BGTv6QxAb3nMwxomk8OcZsvEtbUEJbJAvYJHYH3byAdbZprQEFSzqyjphYlS3bQNSZo1a5WVYzgrNsxp7%2Bc5JD7vQzCZP25a%2FIHXUTlTa3RXMA9YnCDTPlu%2FQPWGF51MZyE6wNkesv%2F5RxPuOUcjP%2FqU%2FY3XPAEG3ZidG0E4OIN4KYzucffAPHW7nxoMLcwhwzbY594d7v6GTqqAlsGG0evuj9YA9F2QmrgV5%2Bu2XkKSNorEGwQ1ROgd17S6VW7ia0bh7UwSU', swid='{FD137FF0-20E0-4529-A766-93B377CB9B98}')
import psycopg2
def sql_dict(dict1, dict2):
    dict3 = {}
    for key1A, key1B in dict1.items():
        # Retrieve the corresponding value from dict2
        value1 = dict2.get(key1A)
        # Add the key-value pair to dict3
        dict3[key1B] = value1
    return dict3
def connectPostgres(): # Database connection details
    global cursor
    global conn
    db_host = 'ep-dark-cloud-064315-pooler.us-east-1.postgres.vercel-storage.com'
    db_name = 'verceldb'
    db_user = 'default'
    db_password = 'NnJ8GotB5Wwh'

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
    cursor = conn.cursor()

def closePostgres():
    global cursor
    global conn
    cursor.close()
    conn.close()

def insert_record(data, table_name):
    global cursor
    global conn
    # Specify the table name and the dictionary containing field names and values
    table_name = table_name
    record_data = data

    # Construct the SQL query for insertion
    fields = ', '.join(record_data.keys())
    values = ', '.join(['%s'] * len(record_data.values()))
    query = f"INSERT INTO {table_name} ({fields}) VALUES ({values})"

    # Execute the query with the values from the dictionary
    cursor.execute(query, tuple(record_data.values()))
    conn.commit()

# Database connection details
db_host = 'ep-dark-cloud-064315-pooler.us-east-1.postgres.vercel-storage.com'
db_name = 'verceldb'
db_user = 'default'
db_password = 'NnJ8GotB5Wwh'

# Function to query missing weeks for a playerId
def get_missing_weeks(player_id):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Query the table for missing weeks
    query = f"""
        SELECT DISTINCT week
        FROM player_stats
        WHERE playerId = {player_id}
        ORDER BY week;
    """
    cursor.execute(query)
    existing_weeks = [row[0] for row in cursor.fetchall()]

    # Generate the list of missing weeks
    all_weeks = list(range(1, 18))  # Assuming 17 weeks in a season
    missing_weeks = [week for week in all_weeks if week not in existing_weeks]

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return missing_weeks

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Query for unique playerIds based on the condition in the given query
query = """
    SELECT DISTINCT playerId
    FROM player_stats
    WHERE season = 2022
    GROUP BY playerId
    HAVING COUNT(DISTINCT week) < 17;
"""
cursor.execute(query)
unique_player_ids = [row[0] for row in cursor.fetchall()]

# Close the cursor and connection
cursor.close()
conn.close()
with open('.\\GetData\\dictionary.json', 'r') as file:
    sql_mapping = json.load(file)
# Iterate over each playerId and get missing weeks
for player_id in unique_player_ids:
    missing_weeks = get_missing_weeks(player_id)
    pi = league.player_info(playerId=player_id)
    connectPostgres()
    for mw in missing_weeks: 
        try:
           stats = pi.stats[mw]
           stats['totalPoints'] = pi.stats[mw]['points']
        except KeyError: 
            stats = {}
        stats['playerId'] = pi.playerId
        stats['playerName'] = pi.name
        stats['week'] = mw
        stats['season'] = draftYear
        stats['position'] = pi.position
        newStats = sql_dict(sql_mapping, stats)
        insert_record(newStats, 'player_stats')
        print(f'inseted {pi.name} week {mw}')
    closePostgres()
