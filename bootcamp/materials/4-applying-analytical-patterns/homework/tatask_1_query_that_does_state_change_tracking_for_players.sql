
insert into players_growth_accounting
WITH yesterday AS (
    SELECT * FROM players_growth_accounting
    WHERE season = 1996
),
     today AS (
         SELECT
            CAST(player_name AS TEXT) as player_name,
            season,
            COUNT(1)
         FROM player_seasons
         WHERE season = 1997
         AND player_name IS NOT NULL
         GROUP BY player_name, season
     )
         SELECT COALESCE(t.player_name, y.player_name) as player_name,
                COALESCE(y.first_active_season, t.season) AS first_active_season,
                COALESCE(t.season, y.last_active_season) AS last_active_season,
                CASE
                    WHEN y.player_name IS NULL THEN 'New'
                    WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
                    WHEN y.last_active_season < t.season - 1 THEN 'Returned from Retirement'
                    WHEN t.season IS NULL AND y.last_active_season = y.season THEN 'RETIRED'
                    ELSE 'Stayed Retired'
                    end as state,
                COALESCE(y.seasons_active,
                         ARRAY []::INT[])
                    || CASE
                           WHEN
                               t.player_name IS NOT NULL
                               THEN ARRAY [t.season]
                           ELSE ARRAY []::INT[]
                    END AS seasons_active,
                COALESCE(t.season, y.season+ 1) as season
         FROM today t
                  FULL OUTER JOIN yesterday y
                                  ON t.player_name = y.player_name
