from chispa.dataframe_comparer import *
from ..jobs.cumulative_actors_table_generation_query_job import cumulative_actors_actors_table_generation_query
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, BooleanType

from chispa.dataframe_comparer import assert_df_equality


def test_cumulative_actors_table_generation_query(spark):
    actors_data = [
        ("Leonardo DiCaprio", [{"film": "Titanic", "votes": 10, "rating": 8.5, "filmid": 1, "year": 1997}], "star", False, 1999),
        ("Tom Hanks", [{"film": "Forrest Gump", "votes": 8, "rating": 7.8, "filmid": 2, "year": 1994}], "star", False, 1999)
    ]
    actors_schema = StructType([
        StructField("name", StringType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("filmid", IntegerType(), True),
            StructField("year", IntegerType(), True)
        ])), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), True)
    ])
    actors_df = spark.createDataFrame(actors_data, schema=actors_schema)

    actor_films_data = [
        ("Leonardo DiCaprio", 1, "The Beach", 9, 7.5, 3, 2000),
        ("Tom Hanks", 2, "Cast Away", 8, 8.2, 4, 2000),
        ("Brad Pitt", 3, "Fight Club", 10, 8.8, 5, 2000)
    ]
    actor_films_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actorid", IntegerType(), True),
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("filmid", IntegerType(), True),
        StructField("year", IntegerType(), True)
    ])
    actor_films_df = spark.createDataFrame(actor_films_data, schema=actor_films_schema)

    expected_data = [
        ("Leonardo DiCaprio", [{"film": "Titanic", "votes": 10, "rating": 8.5, "filmid": 1, "year": 1997},
                               {"film": "The Beach", "votes": 9, "rating": 7.5, "filmid": 3, "year": 2000}],
         "good", True, 2000),
        ("Tom Hanks", [{"film": "Forrest Gump", "votes": 8, "rating": 7.8, "filmid": 2, "year": 1994},
                       {"film": "Cast Away", "votes": 8, "rating": 8.2, "filmid": 4, "year": 2000}],
         "star", True, 2000),
        ("Brad Pitt", [{"film": "Fight Club", "votes": 10, "rating": 8.8, "filmid": 5, "year": 2000}],
         "star", True, 2000)
    ]
    expected_df = spark.createDataFrame(expected_data, schema=actors_schema)

    # Run the query
    result_df = cumulative_actors_actors_table_generation_query(spark, actors_df, actor_films_df)

    result_df_sorted = result_df.orderBy("name")
    expected_df_sorted = expected_df.orderBy("name")

    assert_df_equality(result_df_sorted, expected_df_sorted, ignore_nullable=True)

