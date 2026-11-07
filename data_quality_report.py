
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count, round
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("London House Price Data Quality Report") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Dynamic project directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Cleaned dataset path
file_path = "file://" + os.path.join(
    BASE_DIR,
    "Dataset",
    "cleaned_london_house_price_data"
)

try:
    # Load cleaned dataset
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(file_path)

    print("\n========== DATASET OVERVIEW ==========")
    print("Total rows:", df.count())
    print("Total columns:", len(df.columns))

    print("\n========== DATA TYPES ==========")
    df.printSchema()

    print("\n========== MISSING VALUES REPORT ==========")

    total_rows = df.count()

    missing_report = df.select([
        sum(
            when(col("`" + c + "`").isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ])

    missing_report.show(truncate=False)

    print("\n========== MISSING VALUE PERCENTAGES ==========")

    missing_percentage = missing_report.select([
        round(
            (col("`" + c + "`") / total_rows) * 100, 2
        ).alias(c)
        for c in df.columns
    ])

    missing_percentage.show(truncate=False)

    print("\n========== INVALID NUMERIC VALUES ==========")

    numeric_columns = [
        "saleEstimate_currentPrice",
        "rentEstimate_currentPrice",
        "bedrooms",
        "bathrooms"
    ]

    for c in numeric_columns:
        print("\nChecking column:", c)

        df.filter(
            col("`" + c + "`") < 0
        ).select(c).show(10, truncate=False)

    print("\n========== ZERO BEDROOMS / BATHROOMS ==========")

    for c in ["bedrooms", "bathrooms"]:
        print("\nChecking column:", c)

        df.filter(
            col("`" + c + "`") == 0
        ).select(c).show(10, truncate=False)

    print("\n========== SALE PRICE CONSISTENCY ==========")

    invalid_sale_estimates = df.filter(
        (col("saleEstimate_lowerPrice") >
         col("saleEstimate_currentPrice")) |
        (col("saleEstimate_currentPrice") >
         col("saleEstimate_upperPrice"))
    )

    print(
        "Records with inconsistent sale estimates:",
        invalid_sale_estimates.count()
    )

    invalid_sale_estimates.select(
        "postcode",
        "saleEstimate_lowerPrice",
        "saleEstimate_currentPrice",
        "saleEstimate_upperPrice"
    ).show(10, truncate=False)

    print("\n========== RENT ESTIMATE CONSISTENCY ==========")

    invalid_rent_estimates = df.filter(
        (col("rentEstimate_lowerPrice") >
         col("rentEstimate_currentPrice")) |
        (col("rentEstimate_currentPrice") >
         col("rentEstimate_upperPrice"))
    )

    print(
        "Records with inconsistent rent estimates:",
        invalid_rent_estimates.count()
    )

    invalid_rent_estimates.select(
        "postcode",
        "rentEstimate_lowerPrice",
        "rentEstimate_currentPrice",
        "rentEstimate_upperPrice"
    ).show(10, truncate=False)

    print("\n========== DATA QUALITY REPORT COMPLETED ==========")

    print("\n========== DATA QUALITY SUMMARY ==========")

    print("Total records          :", df.count())
    print("Total columns          :", len(df.columns))
    print("Missing bedrooms       :", df.filter(col("bedrooms").isNull()).count())
    print("Missing bathrooms      :", df.filter(col("bathrooms").isNull()).count())

    print("Sale estimate issues   :", invalid_sale_estimates.count())
    print("Rent estimate issues   :", invalid_rent_estimates.count())

    print("\nData quality report completed successfully.")

finally:
    spark.stop()

