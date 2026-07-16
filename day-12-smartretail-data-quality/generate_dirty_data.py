import os
import csv
import random
from datetime import datetime, timedelta

# Create directory if it doesn't exist
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "smartretail_orders_dirty.csv")

# Set random seed for reproducibility
random.seed(42)

# Candidate values for generation
domains = ["gmail.com", "yahoo.com", "outlook.com", "retailcorp.in"]
first_names = ["Aarav", "Aditi", "Vivaan", "Ananya", "Vihaan", "Diya", "Sai", "Pari", "Arjun", "Kavya"]
last_names = ["Sharma", "Verma", "Gupta", "Mehta", "Patel", "Singh", "Reddy", "Nair", "Joshi", "Rao"]

def generate_dirty_dataset():
    records = []
    
    # 1. Generate clean base records (150 rows)
    for i in range(1, 151):
        order_id = f"ORD_{1000 + i}"
        customer_id = f"CUST_{random.randint(500, 999)}"
        product_id = f"PROD_{random.randint(100, 199)}"
        quantity = random.randint(1, 10)
        price = round(random.uniform(49.99, 9999.99), 2)
        
        # Safe transaction dates: past 30 days
        days_ago = random.randint(1, 30)
        tx_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        
        # Valid email
        f_name = random.choice(first_names).lower()
        l_name = random.choice(last_names).lower()
        email = f"{f_name}.{l_name}@{random.choice(domains)}"
        
        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "price": price,
            "transaction_date": tx_date,
            "customer_email": email
        })

    # 2. Inject intentional data quality issues (30 rows)
    
    # - Missing Product ID (5 rows)
    for i in range(5):
        records[random.randint(0, len(records)-1)]["product_id"] = ""
        
    # - Missing Customer ID (3 rows)
    for i in range(3):
        records[random.randint(0, len(records)-1)]["customer_id"] = ""

    # - Negative Price (5 rows)
    for i in range(5):
        idx = random.randint(0, len(records)-1)
        records[idx]["price"] = -1 * abs(records[idx]["price"])

    # - Price as 0 (2 rows)
    for i in range(2):
        records[random.randint(0, len(records)-1)]["price"] = 0.0

    # - Quantity <= 0 (5 rows)
    for i in range(5):
        records[random.randint(0, len(records)-1)]["quantity"] = random.choice([0, -2, -5])

    # - Invalid Customer Emails (5 rows)
    invalid_emails = ["missing_at_sign.com", "john.doe@", "@domain.com", "plainaddress", "test.com@test"]
    for i in range(5):
        idx = random.randint(0, len(records)-1)
        records[idx]["customer_email"] = invalid_emails[i]

    # - Future Transaction Dates (3 rows)
    for i in range(3):
        idx = random.randint(0, len(records)-1)
        future_days = random.randint(5, 50)
        records[idx]["transaction_date"] = (datetime.now() + timedelta(days=future_days)).strftime("%Y-%m-%d")

    # - Duplicate Order IDs (3 duplicates)
    for i in range(3):
        idx_to_dup = random.randint(0, 50)
        idx_to_overwrite = random.randint(100, len(records)-1)
        records[idx_to_overwrite]["order_id"] = records[idx_to_dup]["order_id"]

    # Write to CSV
    headers = ["order_id", "customer_id", "product_id", "quantity", "price", "transaction_date", "customer_email"]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {len(records)} orders with intentional quality issues at: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_dirty_dataset()
