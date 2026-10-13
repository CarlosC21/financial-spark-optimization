from pathlib import Path
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    project_root = Path(__file__).resolve().parent.parent

    # Default paths for the current WSL2 environment.
    data_dir = Path("/mnt/c/Users/Trader/downloads")

    parser = argparse.ArgumentParser(
        description="Naive financial transaction join baseline"
    )
    parser.add_argument(
        "--transactions-path",
        default=str(data_dir / "transactions_data.csv"),
        help="Local path or S3 URI for the transactions CSV"
    )
    parser.add_argument(
        "--users-path",
        default=str(data_dir / "users_data.csv"),
        help="Local path or S3 URI for the users CSV"
    )
    parser.add_argument(
        "--output-path",
        default=str(
            project_root / "output" / "naive_join_by_age"
        ),
        help="Local path or S3 URI for Parquet output"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Validate local inputs. S3 paths are resolved by Spark.
    for path in (args.transactions_path, args.users_path):
        if path.startswith(("s3://", "s3a://", "s3n://")):
            continue

        if not Path(path).is_file():
            raise FileNotFoundError(
                f"Dataset not found: {path}"
            )

    spark = (
        SparkSession.builder
        .appName("FinancialTransactions-NaiveJoin")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Measure data loading, inspection, processing, and writing.
        start_time = time.time()

        # 1. Load transactions.
        print("\n=== Loading transactions ===")
        transactions_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(args.transactions_path)
        )

        print("\n=== Transactions schema ===")
        transactions_df.printSchema()

        print("\n=== First 5 transactions ===")
        transactions_df.show(5)

        # 2. Load users.
        print("\n=== Loading users ===")
        users_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(args.users_path)
        )

        print("\n=== Users schema ===")
        users_df.printSchema()

        print("\n=== First 5 users ===")
        users_df.show(5)

        # 3. Remove records with null join keys.
        transactions_clean = transactions_df.dropna(
            subset=["client_id"]
        )
        users_clean = users_df.dropna(subset=["id"])

        # 4. Naive INNER JOIN using the actual dataset keys.
        print("\n=== Performing INNER JOIN ===")
        joined_df = (
            transactions_clean.alias("t")
            .join(
                users_clean.alias("u"),
                F.col("t.client_id") == F.col("u.id"),
                "inner"
            )
        )

        # 5. Select relevant columns and parse transaction amounts.
        selected_df = joined_df.select(
            F.col("t.id").alias("TransactionID"),
            F.col("t.date").alias("TransactionDate"),
            F.regexp_replace(
                F.trim(F.col("t.amount")),
                r"[$,]",
                ""
            ).cast("decimal(18,2)").alias("TransactionAmount"),
            F.col("u.current_age").alias("CustomerAge")
        )

        # 6. Aggregate transaction amounts by customer age.
        result_df = (
            selected_df
            .groupBy("CustomerAge")
            .agg(
                F.sum("TransactionAmount").alias(
                    "TotalTransactionAmount"
                )
            )
        )

        # 7. Write Parquet and trigger Spark computation.
        print("\n=== Writing Parquet output ===")
        result_df.write.mode("overwrite").parquet(
            args.output_path
        )

        elapsed_time = time.time() - start_time

        print("\n=== Baseline execution completed ===")
        print(f"Output directory: {args.output_path}")
        print(f"Elapsed time: {elapsed_time:.2f} seconds")
        print(f"Elapsed time: {elapsed_time / 60:.2f} minutes")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
