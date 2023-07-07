from espn_api.football import League
import pprint
import datetime
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