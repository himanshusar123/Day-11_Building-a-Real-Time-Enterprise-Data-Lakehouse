import os
import json
import pandas as pd
import great_expectations as gx
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
INPUT_FILE = os.path.join(DATA_DIR, "smartretail_orders_dirty.csv")
CLEAN_FILE = os.path.join(DATA_DIR, "smartretail_orders_clean.csv")
QUARANTINE_FILE = os.path.join(DATA_DIR, "smartretail_orders_quarantine.csv")
RESULTS_JSON = os.path.join(DATA_DIR, "validation_results.json")

def run_validation():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file {INPUT_FILE} not found. Run generate_dirty_data.py first.")
        return

    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE)
    
    # 1. Initialize GX Ephemeral Data Context (GX 1.0+ Fluent API)
    context = gx.get_context(mode="ephemeral")
    
    # 2. Configure Datasource & Data Asset
    data_source = context.data_sources.add_pandas(name="smartretail_datasource")
    data_asset = data_source.add_dataframe_asset(name="smartretail_orders_asset")
    
    # 3. Configure Batch Definition
    batch_def = data_asset.add_batch_definition_whole_dataframe(name="smartretail_orders_batch_def")
    
    # 4. Create Expectation Suite
    suite = context.suites.add(gx.ExpectationSuite(name="smartretail_expectation_suite"))
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Applying expectations. Current validation date threshold: {current_date}")

    # Add expectations to the suite
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="quantity", min_value=1))
    
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column="customer_email", regex=email_regex))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="transaction_date", max_value=current_date))

    # 5. Create Validation Definition and Run
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="smartretail_orders_val_def",
            data=batch_def,
            suite=suite
        )
    )
    
    print("Running Great Expectations validation definition...")
    validation_results = val_def.run(batch_parameters={"dataframe": df})

    # Process results and find unexpected indexes
    unexpected_rows = {} # maps row_idx -> list of failure reasons
    results_summary = []
    
    for result in validation_results.results:
        success = result.success
        expectation_type = result.expectation_config.type
        kwargs = result.expectation_config.kwargs
        column = kwargs.get("column")
        
        # Log to summary list
        results_summary.append({
            "column": column,
            "expectation": expectation_type,
            "success": success,
            "unexpected_count": result.result.get("unexpected_count", 0),
            "unexpected_percent": result.result.get("unexpected_percent", 0.0)
        })
        
        if not success:
            unexpected_indices = result.result.get("partial_unexpected_index_list")
            unexpected_values = result.result.get("partial_unexpected_list")
            
            # Identify which rows failed this specific check
            if unexpected_indices:
                for idx, val in zip(unexpected_indices, unexpected_values):
                    reason = f"{column} failed {expectation_type} (value: {val})"
                    if idx not in unexpected_rows:
                        unexpected_rows[idx] = []
                    unexpected_rows[idx].append(reason)

    # Save to JSON
    with open(RESULTS_JSON, "w") as f:
        json.dump(validation_results.to_json_dict(), f, indent=4)

    # Separate into Clean and Quarantined dataframes
    failed_indices = list(unexpected_rows.keys())
    
    clean_df = df.drop(index=failed_indices).copy()
    quarantine_df = df.loc[failed_indices].copy()
    
    # Add failure reasons column to quarantine dataset
    reasons_col = [", ".join(unexpected_rows[idx]) for idx in quarantine_df.index]
    quarantine_df["quarantine_reasons"] = reasons_col
    
    # Save datasets
    clean_df.to_csv(CLEAN_FILE, index=False)
    quarantine_df.to_csv(QUARANTINE_FILE, index=False)
    
    # Statistics
    total_records = len(df)
    clean_records = len(clean_df)
    quarantine_records = len(quarantine_df)
    success_rate = (clean_records / total_records) * 100 if total_records > 0 else 0
    
    print("\n" + "="*40)
    print("DATA QUALITY VALIDATION SUMMARY")
    print("="*40)
    print(f"Total Rows Processed:   {total_records}")
    print(f"Passed Data Checks:     {clean_records} ({success_rate:.2f}%) -> Saved to {os.path.basename(CLEAN_FILE)}")
    print(f"Quarantined Rows:      {quarantine_records} ({100-success_rate:.2f}%) -> Saved to {os.path.basename(QUARANTINE_FILE)}")
    print("="*40)
    
    for summary in results_summary:
        status = "[PASSED]" if summary["success"] else "[FAILED]"
        col_prefix = f"Column [{summary['column']}]:" if summary["column"] else "Dataset:"
        print(f"{status} - {col_prefix} {summary['expectation']} (Failed rows: {summary['unexpected_count']})")
    print("="*40)
    
    return {
        "total": total_records,
        "clean": clean_records,
        "quarantine": quarantine_records,
        "success_rate": success_rate
    }

if __name__ == "__main__":
    run_validation()
