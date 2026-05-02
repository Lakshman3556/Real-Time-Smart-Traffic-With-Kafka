"""
spark_consumer.py — Fixed Spark Streaming Consumer
Schema matches producer exactly. Reads traffic topic,
windows 10s, writes to ./output/ as JSON.
Run: spark-submit spark_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("TrafficAnalysis") \
    .config("spark.sql.shuffle.partitions","2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Schema matches producer exactly
schema = StructType([
    StructField("vehicle_id",     StringType()),
    StructField("location",       StringType()),
    StructField("speed",          IntegerType()),
    StructField("traffic_status", StringType()),   # use producer's value
    StructField("count",          IntegerType()),   # use producer's count
    StructField("timestamp",      StringType()),
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","traffic") \
    .option("startingOffsets","latest") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("d")).select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"),"yyyy-MM-dd HH:mm:ss"))

# Window aggregation — keeps producer's status & count
agg_df = parsed_df \
    .withWatermark("event_time","15 seconds") \
    .groupBy(
        window(col("event_time"),"10 seconds","5 seconds"),
        col("location"),
        col("traffic_status")
    ).agg(
        spark_max("count").alias("count"),
        spark_max("speed").alias("speed")
    )

output_df = agg_df.select(
    col("window.start").cast("string").alias("window_start"),
    col("window.end").cast("string").alias("window_end"),
    col("location"),
    col("traffic_status").alias("status"),
    col("count"),
    col("speed")
)

query = output_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path","output") \
    .option("checkpointLocation","checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

print("✅ Spark Streaming started → writing to ./output/")
query.awaitTermination()