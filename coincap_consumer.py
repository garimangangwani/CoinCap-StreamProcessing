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
    .add("price", StringType())

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
query = parsed_df \
    .writeStream \
    .foreachBatch(insert_to_mysql) \
    .outputMode("append") \
    .start()

# Start the streaming query
query.awaitTermination()

