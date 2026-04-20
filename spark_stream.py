from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName("TrafficAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("location", StringType()),
    StructField("speed", IntegerType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic") \
    .option("startingOffsets", "latest") \
    .load()
# Convert binary → string
json_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Convert timestamp string → actual timestamp
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# Add traffic classification
traffic_df = parsed_df.withColumn(
    "traffic_status",
    when(col("speed") < 20, "HEAVY")
    .when((col("speed") >= 20) & (col("speed") <= 40), "MODERATE")
    .otherwise("SMOOTH")
)

# 🔥 FIX: small window for fast output
agg_df = traffic_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("location"),
        col("traffic_status")
    ) \
    .count()

# Write output to JSON files
query = agg_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "output") \
    .option("checkpointLocation", "checkpoint") \
    .start()

# Keep streaming
query.awaitTermination()