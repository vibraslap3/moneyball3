SELECT Distinct a.PlayerId, t.owner, a.ADateTime, ps.playerName,tl.playerposition,  tl.season, sum(ps.totalPoints), a.fabcost, count(*) as gameCount
FROM Activity AS a
JOIN (
    SELECT tl.playerId, tl.teamId, tl.week, tl.season, tl.playerposition, gd.startdate
    FROM team_lineups AS tl
	JOIN GameDates AS gd ON tl.week = gd.Week AND tl.season = gd.year
    WHERE tl.rosterPosition <> 'BE'
) AS tl ON tl.playerId = a.PlayerId AND tl.teamId = a.TeamId AND tl.StartDate > a.ADateTime AND tl.StartDate < a.ADateTime + INTERVAL '18 week'
JOIN player_stats AS ps ON ps.playerId = a.PlayerId AND ps.week = tl.week and ps.season = tl.season
JOIN teams AS t on a.teamid = t.teamid
WHERE a.AType = 'ADDED' and tl.startdate > '2023-02-01'
group by a.PlayerId, t.owner, a.ADateTime, ps.playerName, tl.playerposition, tl.season, a.fabcost
ORDER BY a.adatetime