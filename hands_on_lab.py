import os
import sys
import datetime

# Setup JDK and Hadoop environment variables for PySpark
script_dir = os.path.dirname(os.path.abspath(__file__))
local_jdk_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "jdk"))
if not os.environ.get("JAVA_HOME") and os.path.exists(local_jdk_path):
    subdirs = [os.path.join(local_jdk_path, d) for d in os.listdir(local_jdk_path) if os.path.isdir(os.path.join(local_jdk_path, d))]
    if subdirs:
        os.environ["JAVA_HOME"] = subdirs[0]
        os.environ["PATH"] = os.path.join(subdirs[0], "bin") + os.path.pathsep + os.environ.get("PATH", "")

local_hadoop_path = os.path.abspath(os.path.join(script_dir, "..", "smartlogix-analytics", "hadoop"))
if not os.environ.get("HADOOP_HOME") and os.path.exists(local_hadoop_path):
    os.environ["HADOOP_HOME"] = local_hadoop_path
    os.environ["PATH"] = os.path.join(local_hadoop_path, "bin") + os.path.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession

def main():
    print("==================================================================")
    print("      Day 11: Apache Iceberg & Modern Lakehouse Hands-on Lab      ")
    print("==================================================================")

    # Initialize Spark Session with Apache Iceberg Hadoop catalog
    spark = SparkSession.builder \
        .appName("SmartLogix-Iceberg-Lab") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", os.path.join(script_dir, "warehouse")) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Create Database & Iceberg Table
    print("\n--- Step 1: Creating database and Iceberg table shipments ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.shipments")
    spark.sql("""
        CREATE TABLE local.db.shipments (
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
    print("Table local.db.shipments created successfully.")

    # Step 2: Ingest Seed Data (ACID Inserts)
    print("\n--- Step 2: Ingesting seed data (ACID Transactions) ---")
    spark.sql("""
        INSERT INTO local.db.shipments VALUES
        ('SH-1001', 'Delhi', 'Mumbai', 'V-101', 500.0, 'High', 7500.0, 'In Transit', '2026-07-15 09:00:00'),
        ('SH-1002', 'Mumbai', 'Pune', 'V-102', 250.0, 'Medium', 2500.0, 'Vehicle Assigned', '2026-07-15 09:10:00'),
        ('SH-1003', 'Bengaluru', 'Chennai', 'V-103', 1200.0, 'High', 18000.0, 'Delivered', '2026-07-15 09:15:00')
    """)
    print("Seed records successfully inserted.")
    spark.sql("SELECT * FROM local.db.shipments").show()

    # Step 3: ACID Update & Merge Into
    print("\n--- Step 3: Performing DML Updates & MERGE INTO ---")
    
    # Simple Update
    spark.sql("""
        UPDATE local.db.shipments 
        SET status = 'Out for Delivery' 
        WHERE shipment_id = 'SH-1001'
    """)
    print("Updated shipment SH-1001 status.")
    
    # Merge (Upsert) simulating fresh events feed
    # Creating a temp view of incoming batch
    incoming_data = [
        ('SH-1002', 'Mumbai', 'Pune', 'V-102', 250.0, 'Medium', 2500.0, 'In Transit', '2026-07-15 10:00:00'),
        ('SH-1004', 'Hyderabad', 'Delhi', 'V-104', 800.0, 'Low', 8000.0, 'Shipment Created', '2026-07-15 10:05:00')
    ]
    columns = ["shipment_id", "origin", "destination", "vehicle_id", "weight", "priority", "revenue", "status", "timestamp"]
    spark.createDataFrame(incoming_data, schema=columns).createOrReplaceTempView("incoming_batch")

    print("Merging incoming batch (Updates SH-1002, Inserts new SH-1004)...")
    spark.sql("""
        MERGE INTO local.db.shipments target
        USING incoming_batch source
        ON target.shipment_id = source.shipment_id
        WHEN MATCHED THEN
          UPDATE SET target.status = source.status, target.timestamp = source.timestamp
        WHEN NOT MATCHED THEN
          INSERT (shipment_id, origin, destination, vehicle_id, weight, priority, revenue, status, timestamp)
          VALUES (source.shipment_id, source.origin, source.destination, source.vehicle_id, source.weight, source.priority, source.revenue, source.status, source.timestamp)
    """)
    print("Merge statement executed successfully.")
    spark.sql("SELECT * FROM local.db.shipments").show()

    # Step 4: Examine Metadata & Snapshots (Audit Trail)
    print("\n--- Step 4: Querying Iceberg Table History & Snapshots ---")
    print("Table Commit History:")
    spark.sql("SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor FROM local.db.shipments.history").show(truncate=False)

    print("Table Snapshots Audit:")
    snapshots_df = spark.sql("SELECT committed_at, snapshot_id, operation, summary FROM local.db.shipments.snapshots")
    snapshots_df.show(truncate=False)

    # Collect snapshot IDs for Time Travel testing
    snapshots = [row.snapshot_id for row in snapshots_df.collect()]
    print(f"Discovered {len(snapshots)} snapshots in the audit log.")

    # Step 5: Time Travel Queries
    print("\n--- Step 5: Demonstrating Time Travel capabilities ---")
    if len(snapshots) >= 2:
        v1_snapshot = snapshots[0]
        print(f"Querying Table state as of the very first commit (Snapshot ID: {v1_snapshot})...")
        
        # SQL syntax
        print("Using SQL syntax (FOR SYSTEM_VERSION AS OF):")
        spark.sql(f"SELECT * FROM local.db.shipments FOR SYSTEM_VERSION AS OF {v1_snapshot}").show()

        # DataFrame reader syntax
        print("Using Spark DataFrame options syntax:")
        spark.read.option("snapshot-id", v1_snapshot).table("local.db.shipments").show()
    else:
        print("Not enough snapshots found to travel back in time.")

    # Step 6: Schema Evolution (Instant columns addition without rewrite)
    print("\n--- Step 6: Demonstrating Schema Evolution ---")
    print("Adding 'delivery_rating' column to shipments...")
    spark.sql("ALTER TABLE local.db.shipments ADD COLUMNS (delivery_rating INT)")
    
    print("Updated Schema:")
    spark.sql("SELECT * FROM local.db.shipments").printSchema()

    print("Updating the new column value for delivered shipments...")
    spark.sql("UPDATE local.db.shipments SET delivery_rating = 5 WHERE status = 'Delivered'")
    spark.sql("SELECT shipment_id, status, delivery_rating FROM local.db.shipments").show()

    # Step 7: Partition Evolution Concepts
    print("\n--- Step 7: Demonstrating Partition Evolution ---")
    print("By default, the shipments table is unpartitioned.")
    print("Iceberg allows us to add partitioning on 'origin' or 'destination' without rewriting the table data:")
    print("SQL: ALTER TABLE local.db.shipments ADD PARTITION FIELD origin")
    
    # Adding partition field
    spark.sql("ALTER TABLE local.db.shipments ADD PARTITION FIELD origin")
    print("Partition evolution succeeded! New writes will be partitioned by 'origin' automatically, while historical data remains untouched.")

    print("\n==================================================================")
    print("      Day 11 Lab Completed Successfully!                          ")
    print("==================================================================")

    spark.stop()

if __name__ == "__main__":
    main()
