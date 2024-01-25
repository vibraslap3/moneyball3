import pandas as pd

import psycopg2
import plotly.io as pio

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

Query = f"""SELECT Distinct a.PlayerId, t.owner, a.ADateTime, ps.playerName,ps.position,  ps.season, sum(ps.totalPoints), count(*) as gameCount
FROM Activity AS a
JOIN (
    SELECT DISTINCT a2.teamid, a2.adatetime
    FROM Activity AS a2
) AS a2 ON  a.adatetime = a2.adatetime AND  a2.teamid <> a.teamid

JOIN (
    SELECT ps.playerId, ps.playername, ps.week, ps.season, ps.position, gd.startdate, ps.totalpoints
    FROM player_stats AS ps
    JOIN GameDates AS gd ON ps.week = gd.Week AND ps.season = gd.year
) AS ps ON ps.playerId = a.PlayerId AND ps.StartDate > a.ADateTime AND ps.StartDate < a.ADateTime + INTERVAL '18 week'

JOIN teams AS t ON a2.teamid = t.teamid
WHERE a.AType = 'TRADED' AND ps.startdate > '2023-02-01'
group by a.PlayerId, t.owner, a.ADateTime, ps.playerName, ps.position, ps.season
ORDER BY a.ADateTime, owner"""

connectPostgres()
data = run_query(Query)
#get unique adatetime from data
#make 8 empty lists, 4 for the data, 4 for the names
list1 = []
list2 = []
list3 = []
list4 = []
namelist1 = []
namelist2 = []
namelist3 = []
namelist4 = []
#loop through the data
lastOwner = ""
lastDate = ""
for index, row in data.iterrows():
    if lastDate != row['adatetime']:
        list1.append(row['sum'])
        namelist1.append(row['playername'])
        lastOwner = row['owner']
        lastDate = row['adatetime']
        try:
            if data.iloc[index+1]['owner'] == lastOwner and data.iloc[index+1]['adatetime'] == lastDate:
                list2.append(data.iloc[index+1]['sum'])
                namelist2.append(data.iloc[index+1]['playername'])
            else:
                list2.append(0)
                namelist2.append("")
            if data.iloc[index+2]['owner'] == lastOwner and data.iloc[index+2]['adatetime'] == lastDate:
                list3.append(data.iloc[index+2]['sum'])
                namelist3.append(data.iloc[index+2]['playername'])
            else:
                list3.append(0)
                namelist3.append("")
            if data.iloc[index+3]['owner'] == lastOwner and data.iloc[index+3]['adatetime'] == lastDate:
                list4.append(data.iloc[index+3]['sum'])
                namelist4.append(data.iloc[index+3]['playername'])
            else:
                list4.append(0)
                namelist4.append("")
        except IndexError:
            list2.append(0)
            namelist2.append("")
    elif lastOwner != row['owner']:
        list1.append(row['sum'])
        namelist1.append(row['playername'])
        lastOwner = row['owner']
        lastDate = row['adatetime']
        try:
            if data.iloc[index+1]['owner'] == lastOwner and data.iloc[index+1]['adatetime'] == lastDate:
                list2.append(data.iloc[index+1]['sum'])
                namelist2.append(data.iloc[index+1]['playername'])
            else:
                list2.append(0)
                namelist2.append("")
            if data.iloc[index+2]['owner'] == lastOwner and data.iloc[index+2]['adatetime'] == lastDate:
                list3.append(data.iloc[index+2]['sum'])
                namelist3.append(data.iloc[index+2]['playername'])
            else:
                list3.append(0)
                namelist3.append("")
            if data.iloc[index+3]['owner'] == lastOwner and data.iloc[index+3]['adatetime'] == lastDate:
                list4.append(data.iloc[index+3]['sum'])
                namelist4.append(data.iloc[index+3]['playername'])
            else:
                list4.append(0)
                namelist4.append("")
        except IndexError:
            list2.append(0)
            namelist2.append("")
    else:
        pass

closePostgres()
owner_adatetime = data[['owner','adatetime']].drop_duplicates()
#get list ofg
adatetime = owner_adatetime['adatetime'].tolist()
x = [
    adatetime,
    owner_adatetime['owner'].tolist(),
]
fig = go.Figure()
fig.add_bar(x=x,y=list1, text=namelist1,textfont={'color':'white'})
fig.add_bar(x=x,y=list2, text=namelist2,textfont={'color':'white'})
fig.add_bar(x=x,y=list3, marker={'color': 'darkGreen' },  text=namelist3, textfont={'color':'white'}, )
fig.add_bar(x=x,y=list4, text=namelist4,textfont={'color':'white'})
fig.update_layout(barmode="relative")
fig.update_layout(
    title={
        'text': "Trades by Total ROS Points",  # The text of the title
        'font': {'size': 28, },  # Font size and color
        'x': 0.5,  # Horizontal alignment: 0.5 means center
    }
)
fig.update_layout(
    yaxis={
        'title': {
            'text': "<b>ROS Points After Trade</b>",  # The text of the y-axis title
            'font': {'size': 24, },  # Font size and color
            'standoff': 20  # Distance between the title and the axis
        }
    }
)

fig.show()