# *****importing the libries*****
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import from_json, col, unbase64, base64, split, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from pyspark.sql import functions as F

#  *****defining the table name, comment and expectation*****
@dp.table(
    name='finguard.silver.transactions',
    comment='parsed and cleaned  transactions data'
)

@dp.expect_or_drop("Valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("Valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("Valid_card_number", "card_number IS NOT NULL")
@dp.expect_or_drop("Valid_merchant_id", "merchant_id IS NOT NULL")
@dp.expect_or_drop("Valid_amount", "amount > 0")

# *****defining the function to return the dataframe*****
def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")

    # *****Define schema for the incoming data*****
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", StringType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True),
    ])
    transformed_df = bronze_df.select(
        F.from_json(col("value"),schema).alias("data")
        ,F.col("topic").alias("Kafka_topic")
        ,F.col("partition").alias("Kafka_partition")
        ,F.col("offset").alias("Kafka_offset")
        ,F.col("timestamp").alias("Kafka_timestamp")
        ,F.current_timestamp().alias("bronze_ingestion_timestamp")
        
    ).select(
        F.col("data.*")
        ,F.col("Kafka_topic")
        ,F.col("Kafka_partition")
        ,F.col("Kafka_offset")
        ,F.col("Kafka_timestamp")
        ,F.col("bronze_ingestion_timestamp")
        ,F.current_timestamp().alias("silver_ingestion_timestamp")
    )
    
    return transformed_df
















