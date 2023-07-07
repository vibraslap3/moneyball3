SELECT tl.week, t.TeamName, a.PlayerId, ps.playerName, a.ADateTime AS DateAdded, SUM(ps.totalPoints) AS PointsEarned
FROM Activity AS a
JOIN Teams AS t ON t.TeamId = a.TeamId
JOIN team_lineups AS tl ON tl.playerId = a.PlayerId 
JOIN player_stats AS ps ON ps.playerId = a.PlayerId AND ps.week = tl.week
WHERE a.AType = 'ADDED' AND tl.rosterPosition <> 'Bench'
GROUP BY tl.week, t.TeamName, a.PlayerId, ps.playerName, a.ADateTime
ORDER BY tl.week;
