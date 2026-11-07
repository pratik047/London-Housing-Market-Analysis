
from pyspark.sql import SparkSession

from mini import (
    find_postcode_max_avg_sale_price,
    find_property_type_max_bathrooms,
    count_properties_by_country,
    avg_sales_price_change_by_tenure,
    find_highest_avg_rent,
    find_property_type_max_bedrooms,
    find_median_sales_price_per_tenure
)

spark = (
    SparkSession.builder
    .appName("LondonHousePriceQueryTests")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

path = (
    "/home/talentum/Lab_practice/mini/"
    "Dataset/cleaned_london_house_price_data"
)

try:
    df = spark.read.csv(
        path,
        header=True,
        inferSchema=True
    )

    # Q1: Highest average sale price by postcode
    q1 = find_postcode_max_avg_sale_price(df).first()
    assert q1 is not None
    assert q1["postcode"] == "SW1X 8HB"
    print("PASS: Q1 highest average sale price by postcode")

    # Q2: Property type with highest average bathrooms
    q2 = find_property_type_max_bathrooms(df).first()
    assert q2 is not None
    assert q2["propertyType"] == "Detached Property"
    print("PASS: Q2 highest average bathrooms")

    # Q3: Property count by country
    q3 = count_properties_by_country(df).first()
    assert q3 is not None
    assert q3["country"] == "England"
    assert q3["TotalNoProperty"] == 282853
    print("PASS: Q3 property count by country")

    # Q4: Average sale-price percentage change by tenure
    q4 = avg_sales_price_change_by_tenure(df).first()
    assert q4 is not None
    assert q4["tenure"] == "Feudal"
    assert abs(q4["AvgForEachTenure"] - 41.318188811870755) < 0.01
    print("PASS: Q4 average percentage change by tenure")

    # Q5: Country with highest average rent
    q5 = find_highest_avg_rent(df).first()
    assert q5 is not None
    assert q5["country"] == "England"
    assert abs(q5["avg_rent"] - 3589.377057213162) < 0.01
    print("PASS: Q5 highest average rent")

    # Q6: Property type with highest average bedrooms
    q6 = find_property_type_max_bedrooms(df).first()
    assert q6 is not None
    assert q6["propertyType"] == "Detached House"
    assert abs(q6["AvgBedrooms"] - 4.12483745123537) < 0.01
    print("PASS: Q6 highest average bedrooms")

    # Q7: Median sale price by tenure
    q7 = find_median_sales_price_per_tenure(df).first()
    assert q7 is not None
    assert q7["tenure"] == "Freehold"
    assert q7["MedianSP"] == 869000.0
    print("PASS: Q7 median sale price by tenure")

    print("ALL 7 QUERY TESTS PASSED")

finally:
    spark.stop()

