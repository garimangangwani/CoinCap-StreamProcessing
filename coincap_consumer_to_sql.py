from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, DoubleType , LongType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CryptoAssetAnalyzer") \
    .getOrCreate()

# Define the schema for the Kafka messages
schema = StructType() \
    .add("timestamp", LongType()) \
    .add("id", StringType()) \
    .add("asset_rank", StringType()) \
    .add("symbol", StringType()) \
    .add("name", StringType()) \
    .add("supply", DoubleType()) \
    .add("maxSupply", DoubleType()) \
    .add("marketCapUsd", DoubleType()) \
    .add("volumeUsd24Hr", DoubleType()) \
    .add("priceUsd", DoubleType()) \
    .add("changePercent24Hr", DoubleType()) \
    .add("vwap24Hr", DoubleType()) \
    .add("explorer", StringType())

# Read data from Kafka topics into DataFrames
kafka_df1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin") \
    .load()

kafka_df2 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ethereum") \
    .load()

kafka_df3 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dogecoin") \
    .load()

# Convert value column to string and parse JSON for all DataFrames
parsed_df1 = kafka_df1 \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

parsed_df2 = kafka_df2 \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

parsed_df3 = kafka_df3 \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Union all three DataFrames
union_df = parsed_df1.union(parsed_df2).union(parsed_df3)

# Define function to insert DataFrame into MySQL table
def insert_to_mysql(df, epoch_id):
    # Define MySQL connection properties
    mysql_props = {
        "url": "jdbc:mysql://localhost:3306/coincap",
        "user": "hemanth",
        "password": "",
        "dbtable": "crypto_assets"
    }
    # Write DataFrame to MySQL table
    df.write.jdbc(url=mysql_props["url"],
                  table=mysql_props["dbtable"],
                  mode="append",
                  properties={"user": mysql_props["user"], "password": mysql_props["password"]})

# Write the parsed data to the MySQL table
query = union_df \
    .writeStream \
    .foreachBatch(insert_to_mysql) \
    .outputMode("append") \
    .start()

# Start the streaming query
query.awaitTermination()

