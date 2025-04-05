WITH lebron_games AS (
    SELECT 
        gd.game_id,
        g.season,
        gd.pts,
        gd.player_name,
        ROW_NUMBER() OVER (ORDER BY g.season, gd.game_id) AS game_num
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),

streak_identification AS (
    SELECT 
        *,
        SUM(CASE WHEN pts <= 10 THEN 1 ELSE 0 END) OVER (ORDER BY game_num) AS streak_group
    FROM lebron_games
),

streak_calculation AS (
    SELECT 
        streak_group,
        MIN(season) AS start_season,
        MAX(season) AS end_season,
        COUNT(*) AS streak_length
    FROM streak_identification
    WHERE pts > 10
    GROUP BY streak_group
)

SELECT 
    'Lebron James' as player_name,
    streak_length AS consecutive_10pt_games,
    start_season,
    end_season
FROM streak_calculation
ORDER BY streak_length desc;