WITH teams_deduped AS (
    SELECT DISTINCT 
        team_id,
        abbreviation
    FROM teams
),
game_details_with_season AS (
    SELECT 
        gd.*,
        g.season,
        t.team_id AS gd_team_id,
        t.abbreviation AS team_abbrev
    FROM game_details gd
    LEFT JOIN games g ON gd.game_id = g.game_id
    LEFT JOIN teams_deduped t ON gd.team_abbreviation = t.abbreviation
),
home_wins AS (
    SELECT 
        home_team_id AS team_id, 
        COUNT(*) AS wins
    FROM games
    WHERE home_team_wins = 1
    GROUP BY home_team_id
),
visitor_wins AS (
    SELECT 
        visitor_team_id AS team_id, 
        COUNT(*) AS wins
    FROM games
    WHERE home_team_wins = 0
    GROUP BY visitor_team_id
),
combined_wins AS (
    SELECT 
        COALESCE(h.team_id, v.team_id) AS team_id,
        COALESCE(h.wins, 0) + COALESCE(v.wins, 0) AS total_wins
    FROM home_wins h
    FULL OUTER JOIN visitor_wins v ON h.team_id = v.team_id
)
SELECT
    CASE WHEN GROUPING(gd.player_name) = 0 THEN gd.player_name ELSE 'ALL PLAYERS' END AS player_name,
    CASE WHEN GROUPING(gd.team_abbrev) = 0 THEN gd.team_abbrev ELSE 'ALL TEAMS' END AS team_abbreviation,
    CASE WHEN GROUPING(gd.season) = 0 THEN gd.season::TEXT ELSE 'ALL SEASONS' END AS season,
    SUM(gd.pts) AS total_points,
    COUNT(*) AS games_played,
    CASE 
        WHEN GROUPING(gd.player_name) = 0 AND GROUPING(gd.team_abbrev) = 0 THEN 'Player by Team'
        WHEN GROUPING(gd.player_name) = 0 AND GROUPING(gd.season) = 0 THEN 'Player by Season'
        WHEN GROUPING(gd.team_abbrev) = 0 AND GROUPING(gd.player_name) = 1 THEN 'Team Totals'
        ELSE 'Other Aggregation'
    END AS aggregation_type,
    tw.total_wins AS team_wins
FROM game_details_with_season gd
LEFT JOIN combined_wins tw ON gd.gd_team_id = tw.team_id
GROUP BY GROUPING SETS (
    (gd.player_name, gd.team_abbrev),  -- Using renamed column
    (gd.player_name, gd.season),
    (gd.team_abbrev)
), tw.total_wins
ORDER BY 
    aggregation_type,
    total_points DESC;