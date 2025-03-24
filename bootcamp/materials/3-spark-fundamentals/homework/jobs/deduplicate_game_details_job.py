from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

query = """
    WITH deduped AS (
        SELECT 
            gd.*,
            g.game_date_est,
            g.season,
            g.home_team_id,
            g.visitor_team_id,
            ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
        FROM game_details gd
        JOIN games g ON gd.game_id = g.game_id
    )
    SELECT * 
    FROM deduped
    WHERE row_num = 1
"""


def deduplicate_game_details(spark: SparkSession, game_details_df: DataFrame, games_df: DataFrame) -> DataFrame:
    game_details_df.createOrReplaceTempView("game_details")
    games_df.createOrReplaceTempView("games")

    # Execute the query
    deduped_df = spark.sql(query)
    return deduped_df
