from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, max
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ethereumPriceAnalyzer") \
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
    .option("subscribe", "ethereum") \
    .load()

# Convert value column to string and parse JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType())) # Convert timestamp column to TIMESTAMP type

# Apply window operation to calculate average price, total volume traded, and maximum price over 5-minute window
windowed_df = parsed_df \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window("timestamp", "2 minutes")) \
    .agg(avg("priceUsd").alias("avg_price_USD"), 
         sum("volumeUsd24Hr").alias("total_volume_USD"), 
         max("priceUsd").alias("max_price_USD"))

# Write the aggregated data to the console with truncate=False
query = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()

