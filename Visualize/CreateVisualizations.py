from string import ascii_letters
import numpy as np
import pandas as pd
import pprint
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
year = 2021

pp = pprint.PrettyPrinter(width=41, compact=True)
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
    return data

picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, COALESCE(sum(s.gamesplayed),0) as gamesplayed, COALESCE((sum(s.totalpoints)/sum(s.gamesplayed)),0)as ppg
FROM draft_data d
JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
join players p on d.playerid = p.playerid
join Teams t on d.fantasyowner = t.teamid

WHERE s.season = 2021
group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
order by round, pick;"""
def createVisual(grid,values,teamnames,title,vmin,vmax,center):
    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            values[i].reverse()
            grid[i].reverse()
    d = pd.DataFrame(values,dtype=float)
    r = pd.DataFrame(grid)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    sns.set(rc={'axes.facecolor':'dimgrey', 'figure.facecolor':'dimgrey'})

    fig, ax = plt.subplots(figsize=(36, 12))
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=vmin, vmax=vmax, center=center, linewidths=.5, linecolor="dimgrey", cbar=False, fmt='')
    hm.set_xticklabels(teamnames, color="white",fontsize="14")
    hm.xaxis.set_label_position('top')
    ytick_labels = [i for i in range(1,15)]
    hm.set_yticklabels(ytick_labels, color="white",fontsize="14", rotation=0)
    hm.set_title(title, fontsize=24, color="white")
    fig = hm.get_figure()
    fig.savefig(f".\\visualize\\visuals\\{title.replace(' ','_')}.png", bbox_inches='tight')

def totalPointsDraftHeatmap():
    pickList = []
    valuesList = []
    roundPicks = []
    roundValues = []
    teamnames = []
    connectPostgres()
    picks = run_query(picksQuery)
    thisRound = 0
    for p in picks:
        if p[0] < 17:
            teamnames.append(p[5])
        if thisRound == p[1]:
            roundPicks.append(p[3]+"\n"+str(round(p[6],2)))
            roundValues.append(p[6])

        else:
            if roundPicks != []:
                pickList.append(roundPicks)
                valuesList.append(roundValues)
            roundPicks = [p[3]+"\n"+str(round(p[6],2))]
            totalPoints = p[6]

            roundValues = [totalPoints]
            thisRound = p[1]
    if roundPicks != []:
        pickList.append(roundPicks)
        valuesList.append(roundValues)
    title = f"Total Points Draft Heatmap {year}"
    createVisual(pickList,valuesList,teamnames,title,1,225,110)


def ppgDraftHeatmap():
    pickList = []
    valuesList = []
    roundPicks = []
    roundValues = []
    teamnames = []
    connectPostgres()
    picks = run_query(picksQuery)
    closePostgres()
    
    thisRound = 0
    for p in picks:
        if p[0] < 17:
            teamnames.append(p[5])
        if thisRound == p[1]:
            cellText = p[3]+"\n"+str(round(p[8],2))+" PPG"
            roundPicks.append(cellText)
            roundValues.append(p[8])

        else:
            if roundPicks != []:
                pickList.append(roundPicks)
                valuesList.append(roundValues)
            roundPicks = [p[3]+"\n"+str(round(p[8],2))]
            totalPoints = p[8]

            roundValues = [totalPoints]
            thisRound = p[1]
    if roundPicks != []:
        pickList.append(roundPicks)
        valuesList.append(roundValues)
    
    title = f"Points Per Game Draft Heatmap {year}"
    createVisual(pickList,valuesList,teamnames,title,0,18,9)


def totalPointsDraftHeatmapRankScale():
    pickList = []
    valuesList = []
    roundPicks = []
    roundValues = []
    teamnames = []
    connectPostgres()
    picks = run_query(picksQuery)
    ranking = {}
    for pos in ['QB','RB','WR','TE','K','D/ST']:
        query = f"""select playerName, sum(totalPoints)
            from player_stats
            where season = {year} AND position = '{pos}' and totalpoints IS NOT NULL
            group by playerName 
            order by sum(totalPoints) desc"""
        r = run_query(query)
        posranks = {}
        for i in range(len(r)):
            if pos == 'WR':
                posranks[r[i][0]] = i+0.5
            else:
                posranks[r[i][0]] = i+1
        ranking[pos] = posranks
    pp.pprint(ranking)
    thisRound = 0
    for p in picks:
        if p[0] < 17:
            teamnames.append(p[5])
        try:
            rank = ranking[p[4]][p[3]]
        except:
            rank = 100
        if rank > 20:
            rating = 1
        else:
            rating = 21 - rank
        if thisRound == p[1]:
            roundPicks.append(p[3]+"\n"+p[4]+"#"+str(rank))
            roundValues.append(rating)
        else:
            if roundPicks != []:
                pickList.append(roundPicks)
                valuesList.append(roundValues)
            roundPicks = [p[3]+"\n"+p[4]+"#"+str(rank)]
            totalPoints = rating
            roundValues = [totalPoints]
            thisRound = p[1]
    if roundPicks != []:
        pickList.append(roundPicks)
        valuesList.append(roundValues)
    
    closePostgres()
    title = f"Points Per Game Ranking Draft Heatmap {year}"
    createVisual(pickList,valuesList,teamnames,title,1,20,10)

def ppgDraftHeatmapRankScale():
    pickList = []
    valuesList = []
    roundPicks = []
    roundValues = []
    teamnames = []
    connectPostgres()
    picks = run_query(picksQuery)
    ranking = {}
    for pos in ['QB','RB','WR','TE','K','D/ST']:
        query = f"""select playerName, (sum(totalPoints)/sum(gamesplayed)) as PPG
            from player_stats
            where season = {year} AND position = '{pos}' and totalpoints IS NOT NULL
            group by playerName 
            HAVING sum(gamesplayed) > 8
            order by PPG desc"""
        r = run_query(query)
        posranks = {}
        for i in range(len(r)):
            if pos == 'WR':
                posranks[r[i][0]] = i+0.5
            else:
                posranks[r[i][0]] = i+1
        ranking[pos] = posranks
    pp.pprint(ranking)
    thisRound = 0
    for p in picks:
        if p[0] < 17:
            teamnames.append(p[5])
        try:
            rank = ranking[p[4]][p[3]]
        except:
            rank = 100
        if rank > 20:
            rating = 1
        else:
            rating = 21 - rank
        if p[4] == 'WR':
            rank = int(rank*2)
        if thisRound == p[1]: 
            roundPicks.append(p[3]+"\n"+p[4]+"#"+str(rank))
            roundValues.append(rating)
        else:
            if roundPicks != []:
                pickList.append(roundPicks)
                valuesList.append(roundValues)
            roundPicks = [p[3]+"\n"+p[4]+"#"+str(rank)]
            totalPoints = rating
            roundValues = [totalPoints]
            thisRound = p[1]

    if roundPicks != []:
        pickList.append(roundPicks)
        valuesList.append(roundValues)
    
    closePostgres()
    
    title = f"Total Points Ranking Draft Heatmap {year}"
    createVisual(pickList,valuesList,teamnames,title,1,20,10)

def DraftGamesPlayedHeatmap():
    pickList = []
    valuesList = []
    roundPicks = []
    roundValues = []
    teamnames = []
    connectPostgres()
    picks = run_query(picksQuery)
    closePostgres()
    thisRound = 0
    for p in picks:
        if thisRound == p[1]:
            roundPicks.append(p[3]+"\n"+str(round(p[7],2)))
            roundValues.append(p[7])

        else:
            if roundPicks != []:
                pickList.append(roundPicks)
                valuesList.append(roundValues)
            roundPicks = [p[3]+"\n"+str(round(p[7],2))]
            totalPoints = p[7]

            roundValues = [totalPoints]
            thisRound = p[1]
    if roundPicks != []:
        pickList.append(roundPicks)
        valuesList.append(roundValues)

    
    title = f"Total Games Played Ranking Draft Heatmap {year}"
    createVisual(pickList,valuesList,teamnames,title,1,17,12)

totalPointsDraftHeatmap()
totalPointsDraftHeatmapRankScale()
ppgDraftHeatmap()
ppgDraftHeatmapRankScale()
DraftGamesPlayedHeatmap()

