
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("ValidateLondonDataset") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Dynamic project directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Input and output paths
file_path = "file://" + os.path.join(
    BASE_DIR, "Dataset", "london_house_price_data.csv"
)

output_path = os.path.join(
    BASE_DIR, "Dataset", "cleaned_london_house_price_data"
)

try:
    # 1. Load dataset
    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True
    )

    # 2. Dataset schema
    print("\n--- DATASET SCHEMA ---")
    df.printSchema()

    # 3. Total rows
    print("\n--- TOTAL ROWS ---")
    total_rows = df.count()
    print("Total rows:", total_rows)

    # 4. Total columns
    print("\n--- TOTAL COLUMNS ---")
    print("Total columns:", len(df.columns))

    # 5. Sample data
    print("\n--- SAMPLE DATA ---")
    df.show(5, truncate=False)

    # 6. Missing values before cleaning
    print("\n--- MISSING VALUES BEFORE CLEANING ---")

    missing_values = df.select([
        sum(
            when(col("`" + c + "`").isNull(), 1)
            .otherwise(0)
        ).alias(c)
        for c in df.columns
    ])

    missing_values.show(truncate=False)

    # 7. Exact duplicate count
    print("\n--- EXACT DUPLICATE ROWS ---")

    distinct_rows = df.distinct().count()
    duplicate_rows = total_rows - distinct_rows

    print("Total rows:", total_rows)
    print("Distinct rows:", distinct_rows)
    print("Exact duplicate rows:", duplicate_rows)

    # 8. Display duplicate records
    print("\n--- DUPLICATE RECORD DETAILS ---")

    columns = [
        col("`" + c + "`")
        for c in df.columns
    ]

    duplicate_groups = df.groupBy(*columns) \
        .count() \
        .filter(col("count") > 1)

    duplicate_groups.show(30, truncate=False)

    # 9. Remove exact duplicates
    print("\n--- CLEANING EXACT DUPLICATES ---")

    cleaned_df = df.dropDuplicates()
    cleaned_rows = cleaned_df.count()

    print("Original rows:", total_rows)
    print("Rows after removing duplicates:", cleaned_rows)
    print("Rows removed:", total_rows - cleaned_rows)

    # 10. Display cleaned sample
    print("\n--- CLEANED DATA SAMPLE ---")
    cleaned_df.show(5, truncate=False)

    # 11. Validate cleaned dataset
    print("\n--- CLEANED DATA VALIDATION ---")

    print("Original row count:", total_rows)
    print("Cleaned row count:", cleaned_rows)
    print("Cleaned column count:", len(cleaned_df.columns))

    remaining_duplicates = (
        cleaned_rows - cleaned_df.distinct().count()
    )

    print("Remaining exact duplicates:", remaining_duplicates)

    # Missing values after cleaning
    print("\n--- MISSING VALUES AFTER CLEANING ---")

    cleaned_missing_values = cleaned_df.select([
        sum(
            when(col("`" + c + "`").isNull(), 1)
            .otherwise(0)
        ).alias(c)
        for c in cleaned_df.columns
    ])

    cleaned_missing_values.show(truncate=False)

    # 12. Save cleaned dataset
    print("\n--- SAVING CLEANED DATASET ---")
    print("Output path:", output_path)

    # Ensure the parent directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    cleaned_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv("file://" + output_path)

    # 13. Verify that Spark actually created the output
    print("\n--- VERIFYING SAVED DATASET ---")

    if not os.path.isdir(output_path):
        raise RuntimeError(
            "Spark write completed, but output directory was not found: "
            + output_path
        )

    output_files = os.listdir(output_path)

    print("Directory exists:", os.path.isdir(output_path))
    print("Files created:", output_files)

    csv_files = [
        f for f in output_files
        if f.startswith("part-") and f.endswith(".csv")
    ]

    success_file = os.path.isfile(
        os.path.join(output_path, "_SUCCESS")
    )

    if not csv_files:
        raise RuntimeError("No CSV part files found in the output directory.")

    if not success_file:
        raise RuntimeError("Spark _SUCCESS marker was not found.")

    print("CSV part files found:", len(csv_files))
    print("_SUCCESS marker found:", success_file)
    print("Cleaned dataset saved and verified successfully!")

    # 14. Final summary
    print("\n--- FINAL CLEANING SUMMARY ---")
    print("Original rows:", total_rows)
    print("Cleaned rows:", cleaned_rows)
    print("Duplicates removed:", duplicate_rows)
    print("Remaining exact duplicates:", remaining_duplicates)
    print("Columns retained:", len(cleaned_df.columns))
    print("Missing values were preserved.")

finally:
    spark.stop()

