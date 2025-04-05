import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session
import psycopg2

def create_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name


def create_results_sink_postgres(t_env):
    table_name = 'session_analysis'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            host VARCHAR,
            ip VARCHAR,
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_raw_events_sink_postgres(t_env):
    table_name = 'raw_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    
    return table_name

def cmp_hosts():
    conn = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "host.docker.internal"),
        port=os.environ.get("POSTGRES_PORT", 5432),
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT 
            host,
            COUNT(*) AS total_sessions,
            SUM(num_events) AS total_events,
            ROUND(AVG(num_events), 2) AS avg_events_per_session
        FROM 
            session_analysis
        WHERE 
            host LIKE '%techcreator%'
        GROUP BY 
            host
        ORDER BY 
            avg_events_per_session DESC;
    """)
    
    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()

def average_number_of_web_events_of_session_from_user_on_tech_creator():
    conn = psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST", "host.docker.internal"),
        port=os.environ.get("POSTGRES_PORT", 5432),
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            ip, 
            AVG(num_events)
        FROM 
            session_analysis
        WHERE 
            host like '%techcreator%'
        GROUP BY 
            ip
    """)

    for row in cur.fetchall():
        print(row)

    cur.close()
    conn.close()        
    
def sessionize_web_events():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    session_gap_minutes = 5
    
    source_table = create_source_kafka(t_env)
    raw_events_sink = create_raw_events_sink_postgres(t_env)
    
    result_table = create_results_sink_postgres(t_env)

    events = t_env.from_path(source_table)
    events.execute_insert(raw_events_sink)
    
    
    sessionized = events \
        .window(Session.with_gap(lit(session_gap_minutes).minutes).on(col("event_timestamp")).alias("session_window")) \
        .group_by(col("session_window"), col("ip"), col("host"), col("url")) \
        .select(
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            col("host"),
            col("ip"),
            col("url").count.alias("num_events")
        ) \
        .execute_insert(result_table)
    
    average_number_of_web_events_of_session_from_user_on_tech_creator()
    cmp_hosts()
    sessionized.wait()

if __name__ == '__main__':
    sessionize_web_events()
