from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BitcoinAnalyzer") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = StructType() \
    .add("id", StringType()) \
    .add("rank", StringType()) \
    .add("symbol", StringType()) \
    .add("name", StringType()) \
    .add("supply", StringType()) \
    .add("maxSupply", StringType()) \
    .add("marketCapUsd", StringType()) \
    .add("volumeUsd24Hr", StringType()) \
    .add("priceUsd", StringType()) \
    .add("changePercent24Hr", StringType()) \
    .add("vwap24Hr", StringType())

# Read data from Kafka topic into a DataFrame
bitcoin_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin") \
    .load()

# Convert value column to string and parse JSON
parsed_df = bitcoin_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Convert price to DoubleType
parsed_df = parsed_df.withColumn("priceUsd", col("priceUsd").cast(DoubleType()))

# Define a window of 5 data points
windowed_df = parsed_df \
    .groupBy(window("id", "20 seconds")) \
    .agg({"priceUsd": "avg", "priceUsd": "max"})

# Start the streaming query
query = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

# Stop the SparkSession
spark.stop()
