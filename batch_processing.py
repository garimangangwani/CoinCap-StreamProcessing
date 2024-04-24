from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from datetime import datetime, timedelta

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BatchProcessingWithTimeWindow") \
    .getOrCreate()

# Define MySQL connection properties
mysql_props = {
    "url": "jdbc:mysql://localhost:3306/coincap",
    "user": "gariman",
    "password": "",
}

# Define schema for the DataFrame
schema = StructType([
    StructField("id", StringType()),
    StructField("average_price", DoubleType()),
    StructField("max_price", DoubleType()),
    StructField("total_volume", DoubleType())
])

# Read data from MySQL table into a DataFrame
crypto_assets_df = spark.read \
    .format("jdbc") \
    .option("url", mysql_props["url"]) \
    .option("dbtable", "crypto_assets") \
    .option("user", mysql_props["user"]) \
    .option("password", mysql_props["password"]) \
    .load()

# Filter data for the past 15 minutes
current_time = int(datetime.now().timestamp() * 1000)  # Convert to milliseconds
start_time = int((datetime.now() - timedelta(minutes=15)).timestamp() * 1000)  # Convert to milliseconds
filtered_df = crypto_assets_df.filter(col("timestamp").between(start_time, current_time))

# Calculate average price, max price, and total volume for each id
aggregated_df = filtered_df.groupBy("id").agg(
    avg("priceUsd").alias("average_price"),
    max("priceUsd").alias("max_price"),
    sum("volumeUsd24Hr").alias("total_volume")
)

# Insert the aggregated data into a new MySQL table
aggregated_df.write \
    .format("jdbc") \
    .option("url", mysql_props["url"]) \
    .option("dbtable", "price_analysis_results") \
    .option("user", mysql_props["user"]) \
    .option("password", mysql_props["password"]) \
    .mode("overwrite") \
    .save()

# Stop the SparkSession
spark.stop()

