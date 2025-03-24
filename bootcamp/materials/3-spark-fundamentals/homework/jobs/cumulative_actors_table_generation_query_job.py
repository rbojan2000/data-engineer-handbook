from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

query = """
WITH yesterday AS (
    SELECT * FROM actors
    WHERE current_year = 1999
),
today AS (
    SELECT 
        actor,
        actorid,
        COLLECT_LIST(STRUCT(film, votes, rating, filmid, year)) AS films,
        AVG(rating) AS average_rating,
        2000 AS year
    FROM actor_films
    WHERE year = 2000
    GROUP BY actor, actorid
)
SELECT
    COALESCE(t.actor, y.name) AS name, 
    CASE
        WHEN y.films IS NULL THEN 
            t.films
        WHEN y.films IS NOT NULL THEN
            y.films || t.films
        ELSE
            y.films
    END AS films,
    CASE
        WHEN t.year IS NOT NULL THEN
            CASE
                WHEN t.average_rating > 8 THEN 'star'
                WHEN t.average_rating > 7 THEN 'good'
                WHEN t.average_rating > 6 THEN 'average'
                ELSE 'bad'
            END
        ELSE y.quality_class
    END AS quality_class,
    (t.year IS NOT NULL) AS is_active,
    CASE
        WHEN t.year IS NOT NULL THEN t.year
        ELSE y.current_year + 1
    END AS current_year
FROM today t 
FULL OUTER JOIN yesterday y
    ON t.actor = y.name
"""

def cumulative_actors_actors_table_generation_query(spark: SparkSession, actors_df: DataFrame, actor_films_df: DataFrame) -> DataFrame:
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    result_df = spark.sql(query)

    return result_df
