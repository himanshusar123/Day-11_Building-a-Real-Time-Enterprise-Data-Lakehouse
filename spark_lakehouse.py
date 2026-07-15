import os
import sys
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument("--timeout", type=int, default=None, help="Streaming timeout in seconds")
args, unknown = parser.parse_known_args()
timeout = args.timeout

# Setup JDK and Hadoop environment variables for PySpark
script_dir = os.path.dirname(os.path.abspath(__file__))
local_jdk_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "jdk"))
if not os.environ.get("JAVA_HOME") and os.path.exists(local_jdk_path):
    subdirs = [os.path.join(local_jdk_path, d) for d in os.listdir(local_jdk_path) if os.path.isdir(os.path.join(local_jdk_path, d))]
    if subdirs:
        os.environ["JAVA_HOME"] = subdirs[0]
        os.environ["PATH"] = os.path.join(subdirs[0], "bin") + os.path.pathsep + os.environ.get("PATH", "")
        print(f"Dynamically set JAVA_HOME to: {os.environ['JAVA_HOME']}")

local_hadoop_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "hadoop"))
if not os.environ.get("HADOOP_HOME") and os.path.exists(local_hadoop_path):
    os.environ["HADOOP_HOME"] = local_hadoop_path
    os.environ["PATH"] = os.path.join(local_hadoop_path, "bin") + os.path.pathsep + os.environ.get("PATH", "")
    print(f"Dynamically set HADOOP_HOME to: {os.environ['HADOOP_HOME']}")

# Configure PySpark to use the active virtual environment's Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
print(f"Configured PYSPARK_PYTHON to: {sys.executable}")


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("Initializing Spark Session with Apache Iceberg catalog...")
spark = SparkSession.builder \
    .appName("SmartLogix-Lakehouse-Ingest") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", os.path.join(script_dir, "warehouse")) \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching the Kafka stream events
schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("weight", DoubleType(), True),
    StructField("priority", StringType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Create Iceberg Database and Table if not exists
print("Ensuring target Iceberg database and table exist...")
spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.shipments (
        shipment_id STRING,
        origin STRING,
        destination STRING,
        vehicle_id STRING,
        weight DOUBLE,
        priority STRING,
        revenue DOUBLE,
        status STRING,
        timestamp STRING
    ) USING iceberg
""")
print("Iceberg table schema verified.")

# Start reading from Kafka topic
print("Subscribing to Kafka topic: shipment-events...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "shipment-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payloads
parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# Write stream to local Apache Iceberg table
print("Starting streaming ingestion to Iceberg table...")
query = parsed_stream.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", os.path.join(script_dir, "checkpoints", "shipments")) \
    .toTable("local.db.shipments")

# Await query termination
try:
    if timeout:
        print(f"Running ingestion with a timeout of {timeout} seconds...")
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if not query.isActive:
                break
            time.sleep(1)
        print("Timeout reached. Stopping ingestion stream...")
        query.stop()
    else:
        query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping Spark Structured Streaming to Iceberg...")
    query.stop()
finally:
    spark.stop()
    print("Spark Session stopped.")
