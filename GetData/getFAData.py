from espn_api.football import League
import pprint
import csv
import psycopg2
import json
draftYear = 2022
def insert_record(data, table_name):
    # Database connection details
    db_host = 'ep-dark-cloud-064315-pooler.us-east-1.postgres.vercel-storage.com'
    db_name = 'verceldb'
    db_user = 'default'
    db_password = 'NnJ8GotB5Wwh'

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    # Specify the table name and the dictionary containing field names and values
    table_name = table_name
    record_data = data

    # Construct the SQL query for insertion
    fields = ', '.join(record_data.keys())
    values = ', '.join(['%s'] * len(record_data.values()))
    query = f"INSERT INTO {table_name} ({fields}) VALUES ({values})"

    # Execute the query with the values from the dictionary
    cursor.execute(query, tuple(record_data.values()))

    # Commit the transaction and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()

def sql_dict(dict1, dict2):
    dict3 = {}
    for key1A, key1B in dict1.items():
        # Retrieve the corresponding value from dict2
        value1 = dict2.get(key1A)
        # Add the key-value pair to dict3
        dict3[key1B] = value1
    return dict3

with open('dictionary.json', 'r') as file:
    sql_mapping = json.load(file)
league = League(league_id=684556, year=draftYear, espn_s2='AEAfGvrTc9vkVt9%2BGTv6QxAb3nMwxomk8OcZsvEtbUEJbJAvYJHYH3byAdbZprQEFSzqyjphYlS3bQNSZo1a5WVYzgrNsxp7%2Bc5JD7vQzCZP25a%2FIHXUTlTa3RXMA9YnCDTPlu%2FQPWGF51MZyE6wNkesv%2F5RxPuOUcjP%2FqU%2FY3XPAEG3ZidG0E4OIN4KYzucffAPHW7nxoMLcwhwzbY594d7v6GTqqAlsGG0evuj9YA9F2QmrgV5%2Bu2XkKSNorEGwQ1ROgd17S6VW7ia0bh7UwSU', swid='{FD137FF0-20E0-4529-A766-93B377CB9B98}')
pp = pprint.PrettyPrinter(width=41, compact=True)

fa = league.free_agents(week=6, size=50)
print(fa)