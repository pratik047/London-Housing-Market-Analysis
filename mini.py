
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================
# 1. Configure logging
# =========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =========================================
# 2. Save analytical results
# =========================================

def save_result(result, output_path, query_name):

    try:
        logger.info("Saving %s result to %s", query_name, output_path)

        # Explicitly use local filesystem
        spark_output_path = "file://" + output_path

        # Coalesce(1) creates one output partition
        result.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(spark_output_path)

        # Verify output directory
        if not os.path.isdir(output_path):
            raise FileNotFoundError(
                "Output directory not found: " + output_path
            )

        # Verify CSV part file
        csv_files = [
            file for file in os.listdir(output_path)
            if file.endswith(".csv")
        ]

        if len(csv_files) != 1:
            raise FileNotFoundError(
                "Expected one CSV part file, found "
                + str(len(csv_files))
                + " in: "
                + output_path
            )

        logger.info("%s result saved successfully", query_name)

    except Exception:
        logger.exception("Failed to save %s result", query_name)
        raise


# =========================================
# Q1 - Highest average sale price by postcode
# =========================================

def find_postcode_max_avg_sale_price(df):

    try:
        logger.info("Starting Q1: Highest average sale price by postcode")

        result = (
            df.filter(F.col("postcode").isNotNull())
            .groupBy("postcode")
            .agg(F.avg("saleEstimate_currentPrice").alias("avgSales"))
            .filter(F.col("avgSales").isNotNull())
            .orderBy(F.col("avgSales").desc())
            .limit(1)
        )

        result.show(truncate=False)
        logger.info("Q1 completed successfully")

        return result

    except Exception:
        logger.exception("Q1 failed")
        raise


# =========================================
# Q2 - Property type with highest average bathrooms
# =========================================

def find_property_type_max_bathrooms(df):

    try:
        logger.info("Starting Q2: Highest average bathrooms by property type")

        result = (
            df.filter(F.col("propertyType").isNotNull())
            .groupBy("propertyType")
            .agg(F.avg("bathrooms").alias("AvgBathRoom"))
            .filter(F.col("AvgBathRoom").isNotNull())
            .orderBy(F.col("AvgBathRoom").desc())
            .limit(1)
        )

        result.show(truncate=False)
        logger.info("Q2 completed successfully")

        return result

    except Exception:
        logger.exception("Q2 failed")
        raise


# =========================================
# Q3 - Total properties by country
# =========================================

def count_properties_by_country(df):

    try:
        logger.info("Starting Q3: Total properties by country")

        result = (
            df.filter(F.col("country").isNotNull())
            .groupBy("country")
            .agg(F.count("*").alias("TotalNoProperty"))
            .orderBy(F.col("TotalNoProperty").desc())
        )

        result.show(truncate=False)
        logger.info("Q3 completed successfully")

        return result

    except Exception:
        logger.exception("Q3 failed")
        raise


# =========================================
# Q4 - Average sale-price percentage change by tenure
# =========================================

def avg_sales_price_change_by_tenure(df):

    try:
        logger.info("Starting Q4: Average sale-price percentage change by tenure")

        result = (
            df.filter(F.col("tenure").isNotNull())
            .groupBy("tenure")
            .agg(
                F.avg(
                    F.col("`saleEstimate_valueChange.percentageChange`")
                ).alias("AvgForEachTenure")
            )
            .filter(F.col("AvgForEachTenure").isNotNull())
            .orderBy(F.col("AvgForEachTenure").desc())
        )

        result.show(truncate=False)
        logger.info("Q4 completed successfully")

        return result

    except Exception:
        logger.exception("Q4 failed")
        raise


# =========================================
# Q5 - Country with highest average rent
# =========================================

def find_highest_avg_rent(df):

    try:
        logger.info("Starting Q5: Country with highest average rent")

        result = (
            df.filter(F.col("country").isNotNull())
            .groupBy("country")
            .agg(F.avg("rentEstimate_currentPrice").alias("avg_rent"))
            .filter(F.col("avg_rent").isNotNull())
            .orderBy(F.col("avg_rent").desc())
            .limit(1)
        )

        result.show(truncate=False)
        logger.info("Q5 completed successfully")

        return result

    except Exception:
        logger.exception("Q5 failed")
        raise


# =========================================
# Q6 - Property type with highest average bedrooms
# =========================================

def find_property_type_max_bedrooms(df):

    try:
        logger.info("Starting Q6: Highest average bedrooms by property type")

        result = (
            df.filter(F.col("propertyType").isNotNull())
            .groupBy("propertyType")
            .agg(F.avg("bedrooms").alias("AvgBedrooms"))
            .filter(F.col("AvgBedrooms").isNotNull())
            .orderBy(F.col("AvgBedrooms").desc())
            .limit(1)
        )

        result.show(truncate=False)
        logger.info("Q6 completed successfully")

        return result

    except Exception:
        logger.exception("Q6 failed")
        raise


# =========================================
# Q7 - Median sale price by tenure
# =========================================

def find_median_sales_price_per_tenure(df):

    try:
        logger.info("Starting Q7: Median sale price by tenure")

        result = (
            df.filter(F.col("tenure").isNotNull())
            .groupBy("tenure")
            .agg(
                F.percentile_approx(
                    "saleEstimate_currentPrice", 0.5
                ).alias("MedianSP")
            )
            .filter(F.col("MedianSP").isNotNull())
            .orderBy(F.col("MedianSP").desc())
        )

        result.show(truncate=False)
        logger.info("Q7 completed successfully")

        return result

    except Exception:
        logger.exception("Q7 failed")
        raise


# =========================================
# Main application
# =========================================

def main():

    spark = None

    try:
        logger.info("PySpark application started")

        # Create Spark session
        spark = (
            SparkSession.builder
            .appName("PropertyAnalysis")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Dynamic project directory
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        # Input dataset path
        file_path = "file://" + os.path.join(
            BASE_DIR,
            "Dataset",
            "cleaned_london_house_price_data"
        )

        logger.info("Loading London House Price dataset")

        df = spark.read.csv(
            file_path,
            header=True,
            inferSchema=True
        )

        logger.info("Dataset loaded successfully")
        logger.info("Dataset columns: %s", df.columns)

        # Output directory
        output_dir = os.path.join(BASE_DIR, "Output")

        os.makedirs(output_dir, exist_ok=True)

        logger.info("Output directory: %s", output_dir)

        # =================================
        # Execute Q1 - Q7 and save results
        # =================================

        result = find_postcode_max_avg_sale_price(df)
        save_result(
            result,
            os.path.join(output_dir, "Q1_max_avg_sale_price"),
            "Q1"
        )

        result = find_property_type_max_bathrooms(df)
        save_result(
            result,
            os.path.join(output_dir, "Q2_max_avg_bathrooms"),
            "Q2"
        )

        result = count_properties_by_country(df)
        save_result(
            result,
            os.path.join(output_dir, "Q3_properties_by_country"),
            "Q3"
        )

        result = avg_sales_price_change_by_tenure(df)
        save_result(
            result,
            os.path.join(output_dir, "Q4_price_change_by_tenure"),
            "Q4"
        )

        result = find_highest_avg_rent(df)
        save_result(
            result,
            os.path.join(output_dir, "Q5_highest_avg_rent"),
            "Q5"
        )

        result = find_property_type_max_bedrooms(df)
        save_result(
            result,
            os.path.join(output_dir, "Q6_max_avg_bedrooms"),
            "Q6"
        )

        result = find_median_sales_price_per_tenure(df)
        save_result(
            result,
            os.path.join(output_dir, "Q7_median_sale_price"),
            "Q7"
        )

        logger.info("All seven analytical results saved successfully")
        logger.info("PySpark application completed successfully")

    except Exception:
        logger.exception("PySpark application failed")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


# =========================================
# Application entry point
# =========================================

if __name__ == "__main__":
    main()

