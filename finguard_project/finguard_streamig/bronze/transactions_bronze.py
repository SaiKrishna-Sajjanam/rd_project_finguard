# *****importing the libries*****

# from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import from_json, col, unbase64, base64, split, current_timestamp

# *****defining the table name and comment for the table*****
@dp.table(
    name='finguard.bronze.transactions',
    comment='Raw stream transactions data ingested from Kafka'
)

# *****defining function to read the data from kafka stream*****

def transactions() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(scope = "finguard-scope", key = "kafka_connection_details")
    kafka_config = json.loads(kafka_connection_json)
    topic = kafka_config['topic']
    apiSecret = kafka_config['api_secret']
    apikey = kafka_config['api_key']
    bootstrapservers = kafka_config['bootstrap_servers']

    # *****jass config to access kafka from databricks*****
    jasas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{apikey}" password="{apiSecret}";'

    # *****reading the data from kafka to dataframe for further processing*****
    streaming_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapservers) \
        .option("subscribe", topic) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", jasas_config) \
        .option("startingOffsets", "earliest") \
        .load()
    # *****parsing the data from kafka to json*****
    parsed_streaming_df = streaming_df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("timestampType"),
        current_timestamp().alias("ingestion_time")
    )
    # *****returning the dataframe*****
    return parsed_streaming_df