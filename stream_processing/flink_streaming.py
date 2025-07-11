from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
import os

env= StreamExecutionEnvironment.get_execution_environment()
t_env= StreamTableEnvironment.create(env)

base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'flink_jars'))

env.add_jars(
    f"file:///{base_dir}/flink-connector-kafka_2.12-1.14.4.jar".replace('\\', '/'),
    f"file:///{base_dir}/flink-sql-connector-kafka_2.12-1.14.4.jar".replace('\\', '/')
)


t_env.execute_sql("""
  CREATE TABLE user_clicks (
    user_id STRING,
    product_id STRING,
    click_time TIMESTAMP(3),
    session_id STRING,
    WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_clicks',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'click_consumer',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
)

    
          """)


t_env.execute_sql("""
CREATE TABLE fact_clicks_sink (
    user_id STRING,
    product_id STRING,
    click_time TIMESTAMP(3),
    session_id STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/ariflow',
    'table-name' = 'fact_clicks',
    'username' = 'ariflow',
    'password' = 'ariflow',
    'sink.buffer-flush.interval' = '1s',
    'sink.buffer-flush.max-rows' = '100',
    'sink.max-retries' = '3'
)

        

                  """)


t_env.execute_sql("""
INSERT INTO fact_clicks_sink
SELECT user_id,product_id,click_time,session_id
                  FROM user_clicks            
                        """).wait()