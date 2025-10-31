import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


import logging
import uuid

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName("KafkaSparkStreaming") \
            .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,'
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .master("local[*]") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
    except Exception as e:
        logging.error(f"Error creating Spark connection: {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    df = None
    try:
        df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "streaming_data") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Connected to Kafka successfully")
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
    return df


def parse_kafka_message(spark_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        # field name aligned with producer DAG 'postcode'
        StructField("postcode", StringType(), True),
        StructField("email", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])
    #chuyển message dạng JSON (nhận từ Kafka) → thành DataFrame
    user_df = (
        spark_df.selectExpr("CAST(value AS STRING) as value")
        .select(from_json(col('value'), schema).alias('data'))
        .select('data.*')
    )
    logging.info("Parsed Kafka DataFrame")
    return user_df


def create_cassandra_connection():
    cass_session = None
    try:
        cluster = Cluster(['cassandra_db'])
        cass_session = cluster.connect()
        logging.info("Cassandra connection created successfully")
    except Exception as e:
        logging.error(f"Error creating Cassandra connection: {e}")
    return cass_session


def create_keyspace(session):
    session.execute("""
                   CREATE KEYSPACE IF NOT EXISTS spark_streaming
                   WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
                   """)
    logging.info("Keyspace created successfully")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streaming.streaming_data (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        user_name TEXT,
        dob TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    logging.info("Table created successfully")


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        selected_df = parse_kafka_message(spark_df)
        selected_df.printSchema()

        # create cassandra connection and ensure keyspace/table
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Setup completed successfully")

            streaming_query = (selected_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/spark_checkpoint')
                               .option('keyspace', 'spark_streaming')
                               .option('table', 'streaming_data')
                               .start())
            streaming_query.awaitTermination()
        
