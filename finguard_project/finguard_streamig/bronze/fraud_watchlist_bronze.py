# *****importing the libries*****

# from pyspark.sql.functions import *
# from pyspark.sql.types import *
from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


# *****defining the table name and comment for the table*****
@dp.table(
    name='finguard.bronze.fraud_watchlist',
    comment='fraud_watchlist Raw stream  data ingested by autoloader'
)

# *****defining function to read the data from kafka stream*****

def fraud_watchlist_bronze() -> DataFrame:

    # *****reading the data from kafka to dataframe for further processing*****
    streaming_df = (spark.readStream
  .format('cloudFiles')
  .option('cloudFiles.format', 'json')
  .option('cloudfiles.inferSchema', 'true')
  .load('/Volumes/finguard/source/fraud_watchlist/source_data/')
)
    # *****parsing the data from kafka to json*****
    parsed_streaming_df = streaming_df.select(
        F.col("watchlist_id"),
        F.col("watch_type"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.col("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("city"),
        F.col("country"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("ingestion_time")
    )
    # *****returning the dataframe*****
    return parsed_streaming_df