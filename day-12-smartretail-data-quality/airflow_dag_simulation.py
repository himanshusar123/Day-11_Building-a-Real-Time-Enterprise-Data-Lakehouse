import os
import sys
import time

# Add current path
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_DIR)

from run_data_quality import run_validation

def simulate_airflow_dag():
    print("="*60)
    print("[AIRFLOW] DAG SIMULATION: SmartRetail_Data_Ingestion_Pipeline")
    print("="*60)
    
    # Task 1: Extract
    print("[Task: Extract_Retail_Data] RUNNING...")
    time.sleep(1)
    print("[Task: Extract_Retail_Data] SUCCESS: Loaded daily dirty transaction file.")
    print("-" * 50)
    
    # Task 2: Validate
    print("[Task: Run_Great_Expectations_Validation] RUNNING...")
    time.sleep(1)
    
    # Trigger Great Expectations
    stats = run_validation()
    
    if not stats:
        print("[Task: Run_Great_Expectations_Validation] FAILED: Missing dataset.")
        return
        
    success_rate = stats["success_rate"]
    print(f"[Task: Run_Great_Expectations_Validation] SUCCESS: Completed validation. Batch success rate: {success_rate:.2f}%")
    print("-" * 50)
    
    # Task 3: Decision Gate (Branching)
    threshold = 85.0
    print(f"[Airflow Decision Gate] Checking validation threshold (required: {threshold}%, actual: {success_rate:.2f}%)")
    
    if success_rate >= threshold:
        # Task 4a: Load to Lakehouse
        print(f"[Task: Ingest_Clean_Data_To_Lakehouse] RUNNING (Threshold met!)")
        time.sleep(1)
        print(f"[Task: Ingest_Clean_Data_To_Lakehouse] SUCCESS: Ingested {stats['clean']} clean records into Apache Iceberg table 'lakehouse.smartretail.orders'.")
        print(f"[Task: Ingest_Clean_Data_To_Lakehouse] INFO: Quarantined {stats['quarantine']} failed records in storage for audit.")
    else:
        # Task 4b: Fail & Alert
        print(f"[Task: Quarantine_Alert_Stakeholders] RUNNING (Threshold NOT met! Quality score: {success_rate:.2f}%)")
        time.sleep(1)
        print(f"[Task: Quarantine_Alert_Stakeholders] ALERT: Sent Slack notification to #data-ops: 'Validation failed for SmartRetail orders. {stats['quarantine']} rows quarantined. Ingestion stopped!'")
        print(f"[Task: Ingest_Clean_Data_To_Lakehouse] SKIPPED: Pipeline stopped to prevent data corruption in Iceberg.")
        
    print("="*60)
    print("[AIRFLOW] DAG RUN: COMPLETED")
    print("="*60)

if __name__ == "__main__":
    simulate_airflow_dag()
