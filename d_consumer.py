from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CryptoAssetAnalyzer") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("name", StringType()) \
    .add("symbol", StringType()) \
    .add("price", DoubleType())

# Read data from Kafka topic into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto_assets") \
    .load()

# Convert value column to string and parse JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Define a window of 5 minutes
windowed_df = parsed_df \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes")) \
    .agg({"price": "avg"})

# Write the aggregated data to an output sink (e.g., console, file, database)
query = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Start the streaming query
query.awaitTermination()
