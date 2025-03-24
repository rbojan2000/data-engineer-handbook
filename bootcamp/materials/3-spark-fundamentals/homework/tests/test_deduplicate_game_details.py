from chispa.dataframe_comparer import *
from ..jobs.deduplicate_game_details_job import deduplicate_game_details
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

from chispa.dataframe_comparer import assert_df_equality


def test_deduplicate_game_details(spark):
    game_details_data = [
        (1, 101, 201),
        (1, 101, 201),
        (2, 102, 202),
        (3, 103, 203)
    ]
    game_details_schema = ["game_id", "team_id", "player_id"]
    game_details_df = spark.createDataFrame(game_details_data, schema=game_details_schema)

    games_data = [
        (1, "2023-01-01", 2023, 1, 2),
        (2, "2023-01-02", 2023, 3, 4),
        (2, "2023-01-02", 2023, 3, 4),
        (3, "2023-01-03", 2023, 5, 6)
    ]
    games_schema = ["game_id", "game_date_est", "season", "home_team_id", "visitor_team_id"]
    games_df = spark.createDataFrame(games_data, schema=games_schema)

    expected_data = [
        (1, 101, 201, "2023-01-01", 2023, 1, 2, 1),
        (2, 102, 202, "2023-01-02", 2023, 3, 4, 1),
        (3, 103, 203, "2023-01-03", 2023, 5, 6, 1)
    ]
    expected_schema = StructType([
        StructField("game_id", LongType(), True),
        StructField("team_id", LongType(), True),
        StructField("player_id", LongType(), True),
        StructField("game_date_est", StringType(), True),
        StructField("season", LongType(), True),
        StructField("home_team_id", LongType(), True),
        StructField("visitor_team_id", LongType(), True),
        StructField("row_num", IntegerType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)
    result_df = deduplicate_game_details(spark, game_details_df, games_df)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)