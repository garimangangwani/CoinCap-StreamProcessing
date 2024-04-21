from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, col
from pyspark.sql.types import StructType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BitcoinAveragePrice") \
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType() \
    .add("id", "string") \
    .add("rank", "string") \
    .add("symbol", "string") \
    .add("name", "string") \
    .add("supply", "double") \
    .add("maxSupply", "double") \
    .add("marketCapUsd", "double") \
    .add("volumeUsd24Hr", "double") \
    .add("priceUsd", "double") \
    .add("changePercent24Hr", "double") \
    .add("vwap24Hr", "double")

# Read data from Kafka topic into a DataFrame
bitcoin_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin") \
    .load()

# Convert the value column from binary to string and parse JSON
bitcoin_data = bitcoin_df \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'id STRING, rank STRING, symbol STRING, name STRING, supply DOUBLE, maxSupply DOUBLE, marketCapUsd DOUBLE, volumeUsd24Hr DOUBLE, priceUsd DOUBLE, changePercent24Hr DOUBLE, vwap24Hr DOUBLE') AS data") \
    .select("data.*")

# Define a window of time (e.g., 1 hour)
windowed_data = bitcoin_data \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window("timestamp", "1 hour")) \
    .agg(avg(col("priceUsd")).alias("average_price"))

# Start the streaming query to calculate the average price of Bitcoin
query = windowed_data \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
