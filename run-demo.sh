
#!/bin/bash

# =========================================
# Project configuration
# =========================================

# Automatically detect project directory
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Spark installation path
SPARK_HOME="${SPARK_HOME:-/opt/spark3}"

SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

cd "$PROJECT_DIR" || exit 1

# Verify Spark installation
if [ ! -x "$SPARK_SUBMIT" ]; then
    echo "ERROR: spark-submit not found at $SPARK_SUBMIT"
    echo "Please configure SPARK_HOME correctly."
    exit 1
fi

echo "======================================"
echo " London House Price Analysis Pipeline"
echo "======================================"

# Step 1: Clean and validate the dataset
echo "[1/4] Running data cleaning..."

"$SPARK_SUBMIT" validate_data.py > validation.log 2>&1

if [ $? -ne 0 ]; then
    echo "FAILED: Data cleaning. Check validation.log"
    exit 1
fi

echo "PASSED: Data cleaning"

# Step 2: Generate the data quality report
echo "[2/4] Running data quality report..."

"$SPARK_SUBMIT" data_quality_report.py > data_quality.log 2>&1

if [ $? -ne 0 ]; then
    echo "FAILED: Data quality report. Check data_quality.log"
    exit 1
fi

echo "PASSED: Data quality report"

# Step 3: Run the seven analytical queries
echo "[3/4] Running PySpark analysis..."

"$SPARK_SUBMIT" mini.py > output.log 2>&1

if [ $? -ne 0 ]; then
    echo "FAILED: Analysis. Check output.log"
    exit 1
fi

echo "PASSED: Analysis"

# Step 4: Run automated tests
echo "[4/4] Running automated tests..."

"$SPARK_SUBMIT" test_project.py > test_output.log 2>&1

if [ $? -ne 0 ]; then
    echo "FAILED: Automated tests. Check test_output.log"
    exit 1
fi

if ! grep -q "ALL 7 QUERY TESTS PASSED" test_output.log; then
    echo "FAILED: Query test confirmation missing."
    echo "Check test_output.log"
    exit 1
fi

echo "PASSED: Automated tests"

echo "======================================"
echo "PROJECT PIPELINE COMPLETED SUCCESSFULLY"
echo "Logs: validation.log, data_quality.log,"
echo "      output.log, test_output.log"
echo "======================================"

