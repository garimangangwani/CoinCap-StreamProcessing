#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0     coincap_consumer.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BitcoinConsumer") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = "timestamp STRING, id STRING, rank STRING, symbol STRING, name STRING, supply STRING, maxSupply STRING, marketCapUsd STRING, volumeUsd24Hr STRING, priceUsd STRING, changePercent24Hr STRING, vwap24Hr STRING"

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

# Transformation 1: Filtering out records with empty symbol
filtered_df = parsed_df.filter(col("symbol").isNotNull())

# Transformation 2: Convert priceUsd column to DoubleType
price_usd_df = filtered_df.withColumn("priceUsd", col("priceUsd").cast("double"))

# Transformation 3: Calculate the total market cap
total_market_cap = price_usd_df.selectExpr("sum(marketCapUsd) as total_market_cap")

# Transformation 4: Calculate the average price of Bitcoin over a window of time
average_price = price_usd_df \
    .groupBy(window("timestamp", "5 minutes")) \
    .agg({"priceUsd": "avg"})

# Transformation 5: Compute the maximum price variation for Bitcoin over a window of time
max_price_variation = price_usd_df \
    .groupBy(window("timestamp", "1 hour")) \
    .agg({"priceUsd": "max"}) \
    .withColumnRenamed("max(priceUsd)", "max_price") \
    .withColumnRenamed("window", "time_window")

# Start the streaming query
query1 = max_price_variation \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Start the streaming query
query2 = price_usd_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
    
# Wait for the streaming query to terminate
query1.awaitTermination()

# Wait for the streaming query to terminate
query2.awaitTermination()
