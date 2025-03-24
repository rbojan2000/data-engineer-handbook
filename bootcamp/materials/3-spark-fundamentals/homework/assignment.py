# spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0 assignment.py

MATCHES_CSV_PATH = "../data/matches.csv"
MATCHES_DETAILS_CSV_PATH = "../data/match_details.csv"
MEDALS_MATCHES_PLAYERS_CSV_PATH = "../data/medals_matches_players.csv"
MEDALS_CSV_PATH = "../data/medals.csv"
MAPS_CSV_PATH = "../data/maps.csv"

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def broadcast_join_medals_and_maps(spark):
    medals = spark.table("local.bootcamp.medals")
    maps = spark.table("local.bootcamp.maps")
    
    cross_join_result = medals.crossJoin(F.broadcast(maps))
    cross_join_result.explain()
    cross_join_result.show()

def bucket_join_match_details_matches_and_medal_matches_players_on_match_id_with_16_buckets(spark) -> DataFrame:
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    match_details = spark.read.table("local.bootcamp.match_details")
    matches = spark.read.table("local.bootcamp.matches")
    medals_matches_players = spark.read.table("local.bootcamp.medals_matches_players")
    
    joined_df = match_details \
        .join(matches, "match_id", "inner") \
        .join(medals_matches_players, "match_id", "inner")

    joined_df.show(10)
    joined_df.explain()
    
    return joined_df

def aggregate_the_joined_data_frame_to_figure_out_questions(spark, joined_df):
    maps = spark.table("local.bootcamp.maps")
    medals = spark.table("local.bootcamp.medals")
    joined_df = joined_df \
        .join(maps, "mapid", "inner") \
        .join(medals, "medal_id", "inner")
    
    # Query 4a: Average number of kills per game per player
    most_kills = joined_df.groupBy("local.bootcamp.match_details.player_gamertag", "local.bootcamp.match_details.match_id") \
        .agg(F.avg("player_total_kills").alias("avg_kills")) \
        .groupBy("local.bootcamp.match_details.player_gamertag") \
        .agg(F.avg("avg_kills").alias("avg_kills_per_game")) \
        .orderBy(F.desc("avg_kills_per_game"))

    most_kills_top_player = most_kills.limit(1)
    most_kills_top_player.show()
    
    # Query 4b: Most played playlist
    most_played_playlist = joined_df.groupBy("local.bootcamp.matches.playlist_id") \
        .agg(F.count("*").alias("playlist_count")) \
        .orderBy(F.desc("playlist_count"))
    
    most_played_playlist_top = most_played_playlist.limit(1)
    most_played_playlist_top.show()

    # Query 4c: Most played map
    most_played_map = joined_df.groupBy("local.bootcamp.maps.name") \
        .agg(F.count("local.bootcamp.maps.name").alias("map_count")) \
        .orderBy(F.desc("map_count"))
    most_played_map_top = most_played_map.limit(1)
    most_played_map_top.show()
    
    # Query 4d: Most Killing Spree medals by map
    most_killing_spree_map = joined_df.filter(joined_df["local.bootcamp.medals.name"] == "Killing Spree") \
        .groupBy("local.bootcamp.maps.name") \
        .agg(F.count("match_id").alias("killing_spree_count")) \
        .orderBy(F.desc("killing_spree_count"))

    most_killing_spree_map_top = most_killing_spree_map.limit(1)
    most_killing_spree_map_top.show()

def insert(spark: SparkSession):
    
    matchesBucketed = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv(MATCHES_CSV_PATH) \

    matchDetailsBucketed = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(MATCHES_DETAILS_CSV_PATH) \

    medals_matches_players = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(MEDALS_MATCHES_PLAYERS_CSV_PATH) \

    medals = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(MEDALS_CSV_PATH) \

    maps = spark.read.option("header", "true") \
                            .option("inferSchema", "true") \
                            .csv(MAPS_CSV_PATH) \

    matchesBucketed.select("match_id", "mapid", "is_team_game", "playlist_id", "completion_date") \
        .write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("local.bootcamp.matches") 

    matchDetailsBucketed.select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
        .write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("local.bootcamp.match_details")

    medals_matches_players.select("match_id", "player_gamertag", "medal_id", "count") \
        .write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("local.bootcamp.medals_matches_players")

    medals.select("medal_id", "description", "name", "difficulty") \
        .write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("local.bootcamp.medals")

    maps.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save("local.bootcamp.maps")

def ddls(spark: SparkSession):
    spark.sql("CREATE SCHEMA IF NOT EXISTS local.bootcamp")

    matches_ddl = """
        CREATE TABLE IF NOT EXISTS local.bootcamp.matches (
            match_id STRING,
            mapid STRING,
            is_team_game BOOLEAN,
            playlist_id STRING,
            completion_date TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (completion_date)
        CLUSTERED BY (match_id) INTO 16 BUCKETS;
    """
    spark.sql(matches_ddl)
    
    match_details_ddl = """
        CREATE TABLE IF NOT EXISTS local.bootcamp.match_details (
            match_id STRING,
            player_gamertag STRING,
            player_total_kills INTEGER,
            player_total_deaths INTEGER
        )
        USING iceberg
        PARTITIONED BY (match_id)
        CLUSTERED BY (match_id) INTO 16 BUCKETS;
    """
    spark.sql(match_details_ddl)

    medals_matches_players_ddl = """
        CREATE TABLE IF NOT EXISTS local.bootcamp.medals_matches_players (
            match_id STRING,
            player_gamertag STRING,
            medal_id STRING,
            count INTEGER
        )
        USING iceberg
        PARTITIONED BY (match_id)
        CLUSTERED BY (match_id) INTO 16 BUCKETS;
    """
    spark.sql(medals_matches_players_ddl)

    medals_ddl = """
        CREATE TABLE IF NOT EXISTS local.bootcamp.medals (
            medal_id STRING,
            description STRING,
            name STRING,
            difficulty STRING
        )
        USING iceberg
        PARTITIONED BY (medal_id)
    """
    spark.sql(medals_ddl)

    maps_ddl = """
        CREATE TABLE IF NOT EXISTS local.bootcamp.maps (
            mapid STRING,
            name STRING,
            description STRING
        )
        USING iceberg
    """
    spark.sql(maps_ddl)

def main():

    spark = SparkSession.builder \
        .master("local") \
        .appName("assignment") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "7g") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "spark-warehouse") \
        .getOrCreate()
        
    ddls(spark)
    insert(spark)

    broadcast_join_medals_and_maps(spark)
    dataset = bucket_join_match_details_matches_and_medal_matches_players_on_match_id_with_16_buckets(spark)
    aggregate_the_joined_data_frame_to_figure_out_questions(spark, dataset)
    
if __name__ == "__main__":
    main()
