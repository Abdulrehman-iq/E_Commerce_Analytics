import great_expectations as ge
import pandas as pd
from sqlalchemy import create_engine

# Set up connection to PostgreSQL
engine = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")

# Load data
pdf=pd.read_sql("SELECT * FROM fact_orders", engine)

df=ge.dataset.PandasDataset(pdf)
# Define expectations
results = df.validate(
    expectation_suite={
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "order_id"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "revenue",
                    "min_value": 0,
                    "max_value": 10000
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "quantity",
                    "value_set": [1, 2, 3, 4, 5]
                    
                }
            }
        ]
    }
)

if not results["success"]:
    print("Data quality checks failed!")
    print(results)
    raise ValueError("Data quality issues detected")
else:
    print("All data quality checks passed!")