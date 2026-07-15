# Day 11: Modern Data Lakehouse using Apache Iceberg 🧊

Welcome to **Day 11** of the Enterprise Data Engineering curriculum! Today we evolve our streaming analytics pipeline from a lightweight SQL database (SQLite) to a robust, production-ready **Data Lakehouse** using **Apache Iceberg** and **PySpark**.

---

## 🏗️ Architecture: SQL vs Lakehouse

```text
Traditional Streaming:
[Producer] ➔ [Kafka Topic] ➔ [Spark Structured Streaming] ➔ [SQLite (No ACID scale/history)]

Modern Lakehouse (Day 11):
[Producer] ➔ [Kafka Topic] ➔ [Spark Structured Streaming] ➔ [Apache Iceberg (Hadoop Catalog)]
                                                                    │
                                                                    ├── (ACID Transactions)
                                                                    ├── (Time Travel Queries)
                                                                    ├── (Schema Evolution)
                                                                    └── (Partition Evolution)
```

---

## 📂 Project Structure

```text
Day-11_Building-a-Real-Time-Enterprise-Data-Lakehouse/
├── docker-compose.yml   # Kafka Broker infrastructure
├── requirements.txt     # Project dependencies
├── producer.py          # Simulated telemetry event generator
├── spark_lakehouse.py   # Streaming ingestion pipeline from Kafka into Iceberg
├── hands_on_lab.py      # Interactive lab scripts (DML, Time Travel, Evolution)
└── README.md            # Lab instructions (This file)
```

---

## 🚀 Setup Guide

### 1. Configure the Environment
Ensure your terminal environment has access to the python dependencies:
```bash
pip install -r requirements.txt
```

### 2. Start Kafka Infrastructure
Launch the local single-node Kafka broker:
```bash
docker compose up -d
```
Verify the container is healthy:
```bash
docker ps
```

---

## ⚡ Exercise 1: Ingest Live Streams into the Lakehouse

In this exercise, you run the streaming ingestion pipeline where Kafka shipment events are written directly into an Iceberg table in real-time.

1. **Terminal 1: Start the Producer**
   Generate mock shipment events:
   ```bash
   python producer.py
   ```

2. **Terminal 2: Run the Ingestion Engine**
   Start the PySpark ingestion job which loads Spark Iceberg runtime package and streams Kafka events to `local.db.shipments`:
   ```bash
   python spark_lakehouse.py --timeout 60
   ```

3. Notice how Apache Iceberg manages metadata inside a auto-created `warehouse/db/shipments/metadata` folder. Every batch commit writes new JSON metadata, manifest lists, and manifest files, ensuring ACID consistency.

---

## 🔬 Exercise 2: Interactive Lakehouse Capabilities Lab

Run the interactive script to perform advanced transactional operations supported by Iceberg:

```bash
python hands_on_lab.py
```

### What happens inside the Lab:

#### 1. ACID Transactions
We insert initial records and execute updates/deletes. Unlike standard data lakes (Parquet/ORC files), Iceberg enables row-level updates and deletes safely using snapshot isolation.

#### 2. DML Merges
We perform a `MERGE INTO` (upsert) to synchronize a batch of shipment data, updating existing shipments and inserting new ones in a single atomic transaction.

#### 3. Time Travel
We inspect the table's commit history (`local.db.shipments.history`) and snapshots (`local.db.shipments.snapshots`). We query historical data versions directly using their snapshot IDs:
```sql
SELECT * FROM local.db.shipments FOR SYSTEM_VERSION AS OF <snapshot_id>
```

#### 4. Schema Evolution
We add a new column `delivery_rating` dynamically without rewriting the table:
```sql
ALTER TABLE local.db.shipments ADD COLUMNS (delivery_rating INT)
```
Historical data automatically populates as `NULL`, and new data can populate it immediately.

#### 5. Partition Evolution
We evolve the partition layout dynamically:
```sql
ALTER TABLE local.db.shipments ADD PARTITION FIELD origin
```
Iceberg handles this on the fly without table reconstruction, unlike traditional warehouses.

---

## 💡 Key Discussion Questions
1. How does Apache Iceberg differ from traditional Hive partition layouts?
2. What are the benefits of *Hidden Partitioning*?
3. How does the metadata layer prevent "partial writes" during streaming failures?
