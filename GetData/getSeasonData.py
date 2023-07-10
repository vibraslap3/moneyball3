from espn_api.football import League
import pprint
import datetime
import json
import psycopg2

draftYear = 2021
cursor = None
conn = None
pp = pprint.PrettyPrinter(width=41, compact=True)
league = League(league_id=684556, year=draftYear, espn_s2='AEAfGvrTc9vkVt9%2BGTv6QxAb3nMwxomk8OcZsvEtbUEJbJAvYJHYH3byAdbZprQEFSzqyjphYlS3bQNSZo1a5WVYzgrNsxp7%2Bc5JD7vQzCZP25a%2FIHXUTlTa3RXMA9YnCDTPlu%2FQPWGF51MZyE6wNkesv%2F5RxPuOUcjP%2FqU%2FY3XPAEG3ZidG0E4OIN4KYzucffAPHW7nxoMLcwhwzbY594d7v6GTqqAlsGG0evuj9YA9F2QmrgV5%2Bu2XkKSNorEGwQ1ROgd17S6VW7ia0bh7UwSU', swid='{FD137FF0-20E0-4529-A766-93B377CB9B98}')


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

def dt_to_postgres(epoch_time_ms):
    epoch_time_sec = epoch_time_ms // 1000
    dt = datetime.datetime.fromtimestamp(epoch_time_sec)
    postgres_dt = dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    return postgres_dt

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

def run_query(query):
    global cursor
    global conn
    cursor.execute(query)
    data = [row[0] for row in cursor.fetchall()]
    return data

def sql_dict(dict1, dict2):
    dict3 = {}
    for key1A, key1B in dict1.items():
        # Retrieve the corresponding value from dict2
        value1 = dict2.get(key1A)
        # Add the key-value pair to dict3
        dict3[key1B] = value1
    return dict3


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
    
def getActivities():
    print('getting activities')
    activity = league.recent_activity(size=2000)
    connectPostgres()
    for a in activity:
        for act in a.actions:
            data = {}
            data['teamid'] = act[0].team_id
            type = act[1]
            if 'ADDED' in type:
                type = 'ADDED'
            data['atype'] = type
            data['playerid'] = act[2].playerId
            data['fabcost'] = act[3]
            dt = a.date
            data['adatetime'] = dt_to_postgres(dt)
            insert_record(data, 'activity')
    closePostgres()
    print('done')

#getActivities()

def getDraft():
    print('getting draft')
    draftList = league.draft
    connectPostgres()
    for i, pick in enumerate(draftList):
        pickData = {}
        pickData['year'] = draftYear
        pickData['overallpick'] = i+1
        pickData['round'] = (i // 16)+1
        pickData['pick'] = (i % 16)+1
        pickData['playerid'] = draftList[i].playerId
        pickData['fantasyowner'] = draftList[i].team.team_id
        insert_record(pickData, 'Draft_data')
    closePostgres()
    print('done')

def getPlayerData():
    print('getting player data')
    connectPostgres()
    query = f"""
        SELECT DISTINCT playerId
        FROM player_stats
        WHERE season = {draftYear}
        GROUP BY playerId;
    """
    unique_player_ids = run_query(query)
    for player_id in unique_player_ids:
        player = league.player_info(playerId=player_id)
        playerData = {}
        playerData['playerid'] = player_id
        playerData['PlayerName'] = player.name
        playerData['position'] = player.position
        playerData['proTeam'] = player.proTeam
        insert_record(playerData, 'players')
    closePostgres()
    print('done')
def fillData():
    print('filling data')
    
    connectPostgres()
    query = f"""
        SELECT DISTINCT playerId
        FROM player_stats
        WHERE season = {draftYear}
        GROUP BY playerId
        HAVING COUNT(DISTINCT week) < 17;
    """
    unique_player_ids = run_query(query)
    closePostgres()
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
    print('done')

def getTeams():
    print('getting teams')
    connectPostgres()
    query = """SELECT DISTINCT teamID from teams"""
    existingTeams = run_query(query)
    teams = league.teams
    for t in teams:
        if t.team_id not in existingTeams:
            print(f'inserting {t.team_name}')
            teamData = {}
            teamData['teamid'] = t.team_id
            teamData['teamname'] = t.team_name
            teamData['ownername'] = t.owner
            insert_record(teamData, 'teams')
    closePostgres()
    print('done')

def getPlayerStats():
    print('getting player stats')
    connectPostgres()
    with open('.\\GetData\\dictionary.json', 'r') as file:
        sql_mapping = json.load(file)
    league = League(league_id=684556, year=draftYear, espn_s2='AEAfGvrTc9vkVt9%2BGTv6QxAb3nMwxomk8OcZsvEtbUEJbJAvYJHYH3byAdbZprQEFSzqyjphYlS3bQNSZo1a5WVYzgrNsxp7%2Bc5JD7vQzCZP25a%2FIHXUTlTa3RXMA9YnCDTPlu%2FQPWGF51MZyE6wNkesv%2F5RxPuOUcjP%2FqU%2FY3XPAEG3ZidG0E4OIN4KYzucffAPHW7nxoMLcwhwzbY594d7v6GTqqAlsGG0evuj9YA9F2QmrgV5%2Bu2XkKSNorEGwQ1ROgd17S6VW7ia0bh7UwSU', swid='{FD137FF0-20E0-4529-A766-93B377CB9B98}')
    pp = pprint.PrettyPrinter(width=41, compact=True)

    for w in range(1, 18):
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
    print('done')

def getFaData():
    fa = league.free_agents(size=350)
    for l in fa:
        for s in l.stats:
            try:
                stats = s["breakdown"]
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


#getPlayerStats()
#getFaData()
#fillData()
# getTeams()
# getActivities()
getDraft()
# getPlayerData()