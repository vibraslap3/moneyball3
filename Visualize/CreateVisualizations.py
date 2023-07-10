from string import ascii_letters
import numpy as np
import pandas as pd
import pprint
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
year = 2022

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

def totalPointsDraftHeatmap():
    picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed))as ppg
    FROM draft_data d
    JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
    join players p on d.playerid = p.playerid
    join Teams t on d.fantasyowner = t.teamid

    WHERE s.year = {year} 
    group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
    order by round, pick;"""
    connectPostgres()
    picks = run_query(picksQuery)

    pickList = []
    valuesList = []
    thisRound = 0
    roundPicks = []
    roundValues = []
    for p in picks:
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


    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            valuesList[i].reverse()
            pickList[i].reverse()
    d = pd.DataFrame(valuesList,dtype=float)
    r = pd.DataFrame(pickList)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)
    cmap2 = sns.color_palette("RdYlGn")
    
    closePostgres()
    # Draw the heatmap with the mask and correct aspect ratio
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=1, vmax=225, center=110, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
    fig = hm.get_figure()
    fig.savefig(".\\visualize\\visuals\\DraftMatrixTotalPoints.png")

def ppgDraftHeatmap():
    picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed)) as ppg
    FROM draft_data d
    JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
    join players p on d.playerid = p.playerid
    join Teams t on d.fantasyowner = t.teamid

    WHERE s.year = {year} 
    group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
    order by round, pick;"""
    connectPostgres()
    picks = run_query(picksQuery)
    closePostgres()

    pickList = []
    valuesList = []
    thisRound = 0
    roundPicks = []
    roundValues = []
    for p in picks:
        if thisRound == p[1]:
            roundPicks.append(p[3]+"\n"+str(round(p[8],2)))
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


    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            valuesList[i].reverse()
            pickList[i].reverse()
    d = pd.DataFrame(valuesList,dtype=float)
    r = pd.DataFrame(pickList)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=0, vmax=18, center=9, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
    fig = hm.get_figure()
    fig.savefig(".\\visualize\\visuals\\DraftMatrixPPG.png")

def totalPointsDraftHeatmapRankScale():
    picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed))as ppg
    FROM draft_data d
    JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
    join players p on d.playerid = p.playerid
    join Teams t on d.fantasyowner = t.teamid

    WHERE s.year = {year} 
    group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
    order by round, pick;"""
    connectPostgres()
    picks = run_query(picksQuery)

    pickList = []
    valuesList = []
    thisRound = 0
    roundPicks = []
    roundValues = []
    #select unique positions
    #for each position get the top 20 players
    #check for player name in top 20
    # if not found, rank them lowest
    ranking = {}
    for pos in ['QB','RB','WR','TE','K','DEF']:
        query = f"""select playerName, sum(totalPoints)
            from player_stats
            where season = {year} AND position = '{pos}' and totalpoints IS NOT NULL
            group by playerName 
            order by sum(totalPoints) desc
            limit 20"""
        r = run_query(query)
        posranks = {}
        for i in range(len(r)):
            if pos == 'WR':
                posranks[r[i][0]] = i+0.5
            else:
                posranks[r[i][0]] = i+1
        ranking[pos] = posranks
    pp.pprint(ranking)
    for p in picks:
        try:
            rank = ranking[p[4]][p[3]]
            rating = 21 - rank
        except KeyError:
            rank = 20
            rating = 1
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

    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            valuesList[i].reverse()
            pickList[i].reverse()
    d = pd.DataFrame(valuesList,dtype=float)
    r = pd.DataFrame(pickList)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=1, vmax=20, center=10, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
    fig = hm.get_figure()
    fig.savefig(".\\visualize\\visuals\\DraftMatrixTotalPointsRank.png")

def totalPointsDraftHeatmapPPGRankScale():
    picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed))as ppg
    FROM draft_data d
    JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
    join players p on d.playerid = p.playerid
    join Teams t on d.fantasyowner = t.teamid

    WHERE s.year = {year} 
    group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
    order by round, pick;"""
    connectPostgres()
    picks = run_query(picksQuery)

    pickList = []
    valuesList = []
    thisRound = 0
    roundPicks = []
    roundValues = []
    #select unique positions
    #for each position get the top 20 players
    #check for player name in top 20
    # if not found, rank them lowest
    ranking = {}
    for pos in ['QB','RB','WR','TE','K','DEF']:
        query = f"""select playerName, (sum(totalPoints)/sum(gamesplayed)) as PPG
            from player_stats
            where season = {year} AND position = '{pos}' and totalpoints IS NOT NULL
            group by playerName 
            HAVING(sum(gamesplayed) >= 8)
            order by PPG desc
            limit 20"""
        r = run_query(query)
        posranks = {}
        for i in range(len(r)):
            if pos == 'WR':
                posranks[r[i][0]] = i+0.5
            else:
                posranks[r[i][0]] = i+1
        ranking[pos] = posranks
    pp.pprint(ranking)
    for p in picks:
        try:
            rank = ranking[p[4]][p[3]]
            rating = 21 - rank
        except KeyError:
            rank = 20
            rating = 1
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

    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            valuesList[i].reverse()
            pickList[i].reverse()
    d = pd.DataFrame(valuesList,dtype=float)
    r = pd.DataFrame(pickList)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=1, vmax=20, center=10, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
    fig = hm.get_figure()
    fig.savefig(".\\visualize\\visuals\\DraftMatrixPPGRank.png")

def DraftGamesPlayedHeatmap():
    picksQuery = f"""SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed)) as ppg
    FROM draft_data d
    JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
    join players p on d.playerid = p.playerid
    join Teams t on d.fantasyowner = t.teamid

    WHERE s.year = {year} 
    group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
    order by round, pick;"""
    connectPostgres()
    picks = run_query(picksQuery)
    closePostgres()

    pickList = []
    valuesList = []
    thisRound = 0
    roundPicks = []
    roundValues = []
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


    sns.set_theme(style="white")
    for i in range(14):
        if i % 2 == 1:
            valuesList[i].reverse()
            pickList[i].reverse()
    d = pd.DataFrame(valuesList,dtype=float)
    r = pd.DataFrame(pickList)

    # Set up the matplotlib figure
    f, ax = plt.subplots(figsize=(40, 11))

    # Generate a custom diverging colormap
    cmap = sns.diverging_palette(11, 130, s=100, l=60,  as_cmap=True)

    # Draw the heatmap with the mask and correct aspect ratio
    hm = sns.heatmap(d,annot=r, cmap=cmap, vmin=1, vmax=17, center=12, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
    fig = hm.get_figure()
    fig.savefig(".\\visualize\\visuals\\DraftMatrixGamesPlayed.png")
DraftGamesPlayedHeatmap()
"""
For each position
    get top 32 players for each role, (64 for WR))
    heatmap by ranking
    create scale
        0-mid set mid = 0.5
        mid-max set max = 1.0 


"""