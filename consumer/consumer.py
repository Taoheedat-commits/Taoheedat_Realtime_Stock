from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import from_json, col
import os

# Directory where Spark will store checkpoint data
checkpoint_dir = "/tmp/checkpoint/kafka_to_postgres"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)

# Schema matching Kafka data
kafka_data_schema = StructType([
    StructField("date", StringType()),
    StructField("high", StringType()),
    StructField("low", StringType()),
    StructField("open", StringType()),
    StructField("close", StringType()),
    StructField("symbol", StringType())
])

# Create Spark session
spark = (SparkSession.builder
         .appName("KafkaSparkStreaming")
         .getOrCreate()
)

# Read stream from Kafka
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "stock_analysis")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()
)

# Convert Kafka value (JSON string) to structured columns
parsed_df = (df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .select(from_json(col("value"), kafka_data_schema).alias("data"))
    .select("data.*")
)

# Processed dataframe
processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol")
)

# Write stream to console
query = (processed_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", checkpoint_dir)
    .start()
)

# Keep stream running
query.awaitTermination()