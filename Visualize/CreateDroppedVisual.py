import pandas as pd

import psycopg2
import plotly.io as pio
import plotly.express as px
pio.templates.default = "plotly_dark"
import plotly.graph_objects as go
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

def run_query(query):
    global cursor
    global conn
    cursor.execute(query)
    data = [row for row in cursor.fetchall()]
    df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    return df

Query = f"""SELECT Distinct a.PlayerId, t.owner, a.ADateTime, ps.playerName,ps.position,  ps.season, (COALESCE(sum(ps.totalPoints),0)/count(*)) as ppg, a.fabcost, count(*) as games
FROM Activity AS a
JOIN (
    SELECT ps.playerId, ps.playername, ps.week, ps.season, ps.position, gd.startdate, ps.totalpoints
    FROM player_stats AS ps
    JOIN GameDates AS gd ON ps.week = gd.Week AND ps.season = gd.year
) AS ps ON ps.playerId = a.PlayerId AND ps.StartDate > a.ADateTime AND ps.StartDate < a.ADateTime + INTERVAL '18 week'
JOIN teams AS t on a.teamid = t.teamid
WHERE a.AType = 'DROPPED' and ps.startdate > '2023-02-01'
group by a.PlayerId, t.owner, a.ADateTime, ps.playerName, ps.position, ps.season, a.fabcost
HAVING (COALESCE(sum(ps.totalPoints),0)/count(*)) > 9
ORDER BY ppg desc"""


connectPostgres()
data = run_query(Query)
pointslist = list(data['ppg'])
nameslist = list(data['playername'])
ownerslist = list(data['owner'])
fig = go.Figure()
fig = px.bar(x=nameslist, y=pointslist, text=nameslist, color=ownerslist)

fig.show()