--get points scored for all added players
SELECT DISTINCT a.PlayerId, t.owner, a.ADateTime, ps.playerName, sum(ps.totalPoints), a.fabcost
FROM Activity AS a
JOIN (
    SELECT tl.playerId, tl.teamId, tl.week
    FROM team_lineups AS tl
    WHERE tl.rosterPosition <> 'BE'
    GROUP BY tl.playerId, tl.teamId, tl.week
) AS tl ON tl.playerId = a.PlayerId AND tl.teamId = a.TeamId
JOIN GameDates AS gd ON tl.week = gd.Week AND gd.StartDate > a.ADateTime
JOIN player_stats AS ps ON ps.playerId = a.PlayerId AND ps.week = tl.week
JOIN teams AS t on a.teamid = t.teamid
WHERE a.AType = 'ADDED' and a.ADateTime > '2022-09-01' and a.adatetime < '2023-01-31'
GROUP BY a.playerid, t.owner, a.adatetime, ps.playername, a.fabcost

ORDER BY sum(ps.totalpoints);

--standard ppg for drafted players
SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, sum(s.totalpoints) as total, COALESCE(sum(s.gamesplayed),0) as gamesplayed, COALESCE((sum(s.totalpoints)/sum(s.gamesplayed)),0)as ppg
FROM draft_data d
JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season
join players p on d.playerid = p.playerid
join Teams t on d.fantasyowner = t.teamid

WHERE s.season = 2021
group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
order by round, pick;

-- takes into account games played on drafting team
SELECT d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName, count(tl.week) , sum(s.totalpoints) as total, sum(s.gamesplayed) as gamesplayed, (sum(s.totalpoints)/sum(s.gamesplayed))as ppg
FROM draft_data d

join players p on d.playerid = p.playerid
join Teams t on d.fantasyowner = t.teamid
JOIN (
    SELECT tl.week, playerid, teamid, tl.season
    FROM team_lineups AS tl
    WHERE tl.rosterPosition <> 'BE'
) AS tl ON tl.playerId = d.PlayerId AND tl.teamId = d.fantasyowner and tl.season = d.year
JOIN player_stats s ON d.playerid = s.playerid and d.year = s.season and s.week = tl.week

WHERE s.year = 2022 
group by d.overallpick, d.round, d.pick, p.playername, p.Position, t.TeamName
order by round, pick;

