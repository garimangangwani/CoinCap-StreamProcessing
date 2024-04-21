from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BitcoinPriceAnalyzer") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = StructType() \
    .add("id", StringType()) \
    .add("symbol", StringType()) \
    .add("name", StringType()) \
    .add("supply", DoubleType()) \
    .add("maxSupply", DoubleType()) \
    .add("marketCapUsd", DoubleType()) \
    .add("volumeUsd24Hr", DoubleType()) \
    .add("priceUsd", DoubleType()) \
    .add("changePercent24Hr", DoubleType()) \
    .add("vwap24Hr", DoubleType()) \
    .add("explorer", StringType()) \
    .add("timestamp", LongType())

# Read data from Kafka topic into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin") \
    .load()

# Convert value column to string and parse JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Print the value column
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()

