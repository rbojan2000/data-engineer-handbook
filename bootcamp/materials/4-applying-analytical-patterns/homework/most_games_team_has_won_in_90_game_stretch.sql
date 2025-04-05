WITH team_game_results AS (
    SELECT 
        game_id,
        season,
        home_team_id AS team_id,
        CASE WHEN home_team_wins = 1 THEN 1 ELSE 0 END AS is_win
    FROM games
    
    UNION ALL
    
    SELECT 
        game_id,
        season,
        visitor_team_id AS team_id,
        CASE WHEN home_team_wins = 0 THEN 1 ELSE 0 END AS is_win
    FROM games
),
ordered_games AS (
    SELECT 
        team_id,
        game_id,
        season,
        is_win,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY season, game_id) AS game_num
    FROM team_game_results
    ORDER BY team_id, season, game_id
),
valid_windows AS (
    SELECT 
        team_id,
        game_num AS start_game_num,
        game_num + 89 AS end_game_num,
        LEAD(game_num, 89) OVER (PARTITION BY team_id ORDER BY game_num) AS check_end_exists
    FROM ordered_games
),
complete_90_game_stretches AS (
    SELECT 
        v.team_id,
        v.start_game_num,
        v.end_game_num,
        SUM(o.is_win) AS wins_in_90_games
    FROM valid_windows v
    JOIN ordered_games o ON o.team_id = v.team_id 
                        AND o.game_num BETWEEN v.start_game_num AND v.end_game_num
    WHERE v.check_end_exists IS NOT NULL
    GROUP BY v.team_id, v.start_game_num, v.end_game_num
)

SELECT 
    t.abbreviation AS team,
    MAX(c.wins_in_90_games) AS max_wins_in_90_game_stretch
FROM complete_90_game_stretches c
JOIN teams t ON c.team_id = t.team_id
GROUP BY t.abbreviation, c.team_id
ORDER BY max_wins_in_90_game_stretch DESC;