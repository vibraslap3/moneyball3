from string import ascii_letters
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from _utils import getCsvData
year = 2022
picks = getCsvData(f'./FantasyData/{year}/Draft/Picks.csv')[1:]
playerData = getCsvData(f'./FantasyData/{year}/All_Positions_Summary.csv')[1:]
playerDict = {}
pickList = []
valuesList = []
for d in playerData:
    playerDict[d[0]] = d[1:]
thisRound = 0
roundPicks = []
roundValues = []
for p in picks:
    if thisRound == p[1]:
        roundPicks.append(p[3])
        totalPoints = playerDict[p[3]][2]
        roundValues.append(totalPoints)

    else:
        if roundPicks != []:
            pickList.append(roundPicks)
            valuesList.append(roundValues)
        roundPicks = [p[3]]
        totalPoints = playerDict[p[3]][2]

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
cmap = sns.diverging_palette(12, 130, 100, 30,  as_cmap=True)

# Draw the heatmap with the mask and correct aspect ratio
hm = sns.heatmap(d,annot=r, cmap=cmap, vmax=90, center=36, linewidths=.5, cbar_kws={"shrink": .5}, fmt='')
fig = hm.get_figure()
fig.savefig("out.png")

"""
For each position
    get max points
    get points for 16th rank
        32nd rank for WR
    create scale
        0-mid set mid = 0.5
        mid-max set max = 1.0 


"""