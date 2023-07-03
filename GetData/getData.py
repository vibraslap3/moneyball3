from espn_api.football import League
import pprint
import csv
import psycopg2
import json
draftYear = 2022
cursor = None
global conn
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

def sql_dict(dict1, dict2):
    dict3 = {}
    for key1A, key1B in dict1.items():
        # Retrieve the corresponding value from dict2
        value1 = dict2.get(key1A)
        # Add the key-value pair to dict3
        dict3[key1B] = value1
    return dict3

connectPostgres()
with open('dictionary.json', 'r') as file:
    sql_mapping = json.load(file)
league = League(league_id=684556, year=draftYear, espn_s2='AEAfGvrTc9vkVt9%2BGTv6QxAb3nMwxomk8OcZsvEtbUEJbJAvYJHYH3byAdbZprQEFSzqyjphYlS3bQNSZo1a5WVYzgrNsxp7%2Bc5JD7vQzCZP25a%2FIHXUTlTa3RXMA9YnCDTPlu%2FQPWGF51MZyE6wNkesv%2F5RxPuOUcjP%2FqU%2FY3XPAEG3ZidG0E4OIN4KYzucffAPHW7nxoMLcwhwzbY594d7v6GTqqAlsGG0evuj9YA9F2QmrgV5%2Bu2XkKSNorEGwQ1ROgd17S6VW7ia0bh7UwSU', swid='{FD137FF0-20E0-4529-A766-93B377CB9B98}')
pp = pprint.PrettyPrinter(width=41, compact=True)

for w in range(6, 18):
    pp.pprint(league.box_scores(w)) 
    for g in range(0,8):
        homeTeam = league.box_scores(w)[g].home_team.team_id
        for l in league.box_scores(w)[g].home_lineup:
            try:
                stats = l.stats[w]["breakdown"]
            except KeyError:
                stats = {}
            stats['playerId'] = l.playerId
            stats['playerName'] = l.name
            stats['week'] = w
            stats['season'] = draftYear
            stats['totalPoints'] = l.points
            stats['projectedPoints'] = l.projected_points
            stats['position'] = l.position
            newStats = sql_dict(sql_mapping, stats)
            insert_record(newStats, 'player_stats')
            lineup = {}
            lineup['teamId'] = homeTeam
            lineup['playerId'] = l.playerId
            lineup['week'] = w
            lineup['season'] = draftYear
            lineup['playerPosition'] = l.position
            lineup['rosterPosition'] = l.slot_position
            print(lineup)
            insert_record(lineup, 'team_lineups')
        try:
            awayTeam = league.box_scores(w)[g].away_team.team_id
        except AttributeError:
            continue
        for a in league.box_scores(w)[g].away_lineup:
            try:
                stats = a.stats[w]["breakdown"]
            except KeyError:
                stats = {}
            stats['playerId'] = a.playerId
            stats['playerName'] = a.name
            stats['week'] = w
            stats['season'] = draftYear
            stats['totalPoints'] = a.points
            stats['projectedPoints'] = a.projected_points
            stats['position'] = a.position
            newStats = sql_dict(sql_mapping, stats)
            insert_record(newStats, 'player_stats')
            lineup = {}
            lineup['teamId'] = awayTeam
            lineup['playerId'] = a.playerId
            lineup['week'] = w
            lineup['season'] = draftYear
            lineup['playerPosition'] = a.position
            lineup['rosterPosition'] = a.slot_position
            print(lineup)
            insert_record(lineup, 'team_lineups')


closePostgres()