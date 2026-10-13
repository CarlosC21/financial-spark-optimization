# Optimizing Large-Scale Financial Transaction Analysis with Apache Spark

## Project Overview

This project investigates Apache Spark performance optimization using a simulated financial transaction dataset. The goal is to establish a baseline, apply optimization techniques, and measure their impact on execution time and resource utilization.

## Technology Stack

- Python 3.13
- Apache Spark / PySpark 4.2.0
- Java 17
- Ubuntu 24.04 running on WSL2
- Git and GitHub
- AWS EMR and Amazon S3 — pending mentor guidance

## Dataset

The project uses the [Financial Transactions Dataset: Analytics](https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets) from Kaggle.

Relevant files:

- `transactions_data.csv` — transaction records and monetary amounts
- `users_data.csv` — customer demographics and account information
- `train_fraud_labels.json` — transaction fraud labels
- `cards_dat.csv` — card information
- `mcc_codes.json` — merchant category code reference data

Raw datasets are stored locally and excluded from Git.

### Dataset Schema Note

The downloaded files differ from the column names described in the assignment:

- Transactions use `client_id`, `id`, `date`, and `amount`.
- Users use `id` and `current_age`.
- The users file does not contain `AccountID` or `CustomerOccupation`.

The current implementation therefore joins `transactions.client_id` to `users.id` and aggregates transaction amounts by customer age. Currency-formatted amounts are converted to decimal values before aggregation.

## Work Completed

### Part 0: Local Environment Setup

- Installed and verified Java 17, Python, and PySpark.
- Set up Ubuntu 24.04 through WSL2.
- Created a Python virtual environment.
- Verified Spark using a local test job.
- Confirmed access to the transaction and user CSV files.

### Part 1: Naive Data Loading and Join

Implemented `src/naive_join.py` to:

1. Load the transaction and user CSV files with headers and schema inference.
2. Print both schemas and the first five records.
3. Drop records with null join keys.
4. Perform an INNER JOIN on `transactions.client_id = users.id`.
5. Select relevant transaction and customer attributes.
6. Aggregate transaction amounts by customer age.
7. Write the results to Parquet in overwrite mode.
8. Measure elapsed execution time.

The script accepts command-line arguments for input and output paths, supporting local execution and future S3-based execution.

## Baseline Results

| Measurement | Result |
|---|---|
| Initial successful local run | 15.88 seconds |
| Portable-script local run | 15.24 seconds |
| Spark version | 4.2.0 |
| Execution environment | Ubuntu on WSL2 |
| Output format | Snappy-compressed Parquet |
| Aggregated customer-age groups | 74 |
| Parquet validation | Successful |

These are initial local measurements. Execution times may vary between runs and environments.

## Current Project Status

- Local environment setup: **Complete**
- Naive Spark join and aggregation: **Complete**
- Parquet output validation: **Complete**
- AWS EMR baseline: **Pending mentor guidance**
- Spark optimization experiments: **Not yet started**

No AWS cluster has been created, and no dataset files have been uploaded to AWS. The AWS portion is on hold pending clarification about assignment requirements, approved resources, and potential costs.

## Remaining Work

1. Confirm with the mentor whether AWS EMR execution is mandatory and whether an approved sandbox or credits are available.
2. Complete the baseline on EMR and record its runtime, if approved.
3. Compare Spark join strategies:
   - Sort-Merge Join
   - Shuffle Hash Join
   - Broadcast Hash Join
4. Evaluate Adaptive Query Execution (AQE).
5. Investigate partitioning and shuffle behavior.
6. Evaluate executor configuration where supported.
7. Use Spark UI metrics and execution plans to diagnose bottlenecks.
8. Record benchmark results and explain the observed performance differences.

## Project Structure

```text
financial-spark-optimization/
├── .gitignore
├── README.md
├── src/
│   └── naive_join.py
└── output/                 # Generated output; excluded from Git
```

## Run the Local Baseline

Activate the Python virtual environment in Ubuntu and run:

```bash
source .venv/bin/activate

spark-submit \
  --master "local[*]" \
  --driver-memory 4g \
  --conf spark.executor.memory=2g \
  ./src/naive_join.py \
  --transactions-path /mnt/c/Users/Trader/downloads/transactions_data.csv \
  --users-path /mnt/c/Users/Trader/downloads/users_data.csv \
  --output-path "$PWD/output/naive_join_by_age"
```

Adjust the input paths if the datasets are stored elsewhere.

## Project Objective Going Forward

Build on the validated naive baseline by changing one optimization variable at a time, collecting execution metrics, and comparing the results. The final analysis will explain which techniques improve performance for this workload and why.
