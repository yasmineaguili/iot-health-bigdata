from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    avg,
    window,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# =========================
# Schéma des messages Kafka
# =========================
schema = StructType([
    StructField("patientId", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True)
])

# =========================
# Spark Session
# =========================
spark = SparkSession.builder \
    .appName("IoT-Health-Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# Lecture du flux Kafka
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "normal") \
    .option("startingOffsets", "latest") \
    .load()

# =========================
# Parsing + Processing Time
# =========================
parsed = df.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select(
        current_timestamp().alias("timestamp"),  # processing-time
        col("data.metric").alias("metric"),
        col("data.value").alias("value")
    )

# =========================
# Agrégation temps réel
# =========================
aggregated = parsed.groupBy(
        window(col("timestamp"), "5 seconds"),
        col("metric")
    ).agg(
        avg("value").alias("avg_value")
    )

# =========================
# Sortie console (streaming)
# =========================
query = aggregated.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/streaming-metrics") \
    .start()

query.awaitTermination()
