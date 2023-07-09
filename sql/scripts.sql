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