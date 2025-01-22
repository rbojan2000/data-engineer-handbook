CREATE TYPE scd_type AS(
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
);

--------------------------------

WITH last_year_scd AS (
    SELECT * 
    FROM actors_scd
    WHERE current_year = 2002
      AND end_date = 2002
),
historical_scd AS (
    SELECT
        name,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_scd
    WHERE current_year = 2002
      AND end_date < 2002
),
this_year_data AS (
    SELECT * 
    FROM actors
    WHERE current_year = 2003
),
unchanged_records AS (
    SELECT 
        ts.name,
        ts.quality_class,
        ts.is_active,
        ls.start_date,
        ts.current_year AS end_year
    FROM this_year_data ts
    JOIN last_year_scd ls
        ON ls.name = ts.name
    WHERE ts.quality_class = ls.quality_class
      AND ts.is_active = ls.is_active
),
changed_records AS (
    SELECT 
        ts.name,
        UNNEST(ARRAY[
            ROW(
                ls.quality_class,
                ls.is_active,
                ls.start_date,
                ls.end_date
            )::scd_type,
            ROW(
                ts.quality_class,
                ts.is_active,
                ts.current_year,
                ts.current_year
            )::scd_type
        ]) AS records
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls
        ON ls.name = ts.name
    WHERE (
        ts.quality_class <> ls.quality_class OR 
        ts.is_active <> ls.is_active
    )
),
unnested_changed_records AS (
    SELECT 
        name,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
        (records::scd_type).end_date
    FROM changed_records    
),
new_records AS (
    SELECT 
        ts.name,
        ts.quality_class,
        ts.is_active,
        ts.current_year AS start_year,
        ts.current_year AS end_year
    FROM this_year_data ts
    LEFT JOIN last_year_scd ls 
        ON ts.name = ls.name
    WHERE ls.name IS NULL
)
SELECT * 
FROM historical_scd
UNION ALL
SELECT * 
FROM unnested_changed_records
UNION ALL
SELECT * 
FROM new_records;