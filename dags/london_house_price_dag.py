
import os
import sys

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


# Project directory: parent of the dags folder
PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

# Configurable Spark and Python paths
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark3")
PYTHON_PATH = os.environ.get(
    "PYSPARK_PYTHON",
    sys.executable
)

SPARK_SUBMIT = os.path.join(SPARK_HOME, "bin", "spark-submit")

default_args = {
    "owner": "talentum",
    "start_date": days_ago(1)
}

with DAG(
    dag_id="london_house_price_pipeline",
    default_args=default_args,
    description="London House Price PySpark Pipeline",
    schedule_interval=None,
    catchup=False
) as dag:

    start_pipeline = BashOperator(
        task_id="start_pipeline",
        bash_command="echo 'London House Price Pipeline Started'"
    )

    run_pyspark_queries = SparkSubmitOperator(
        task_id="run_pyspark_queries",
        application=os.path.join(PROJECT_DIR, "mini.py"),
        conn_id="spark_default",
        spark_binary=SPARK_SUBMIT,
        env_vars={
            "SPARK_HOME": SPARK_HOME,
            "PYTHONPATH": (
                os.path.join(SPARK_HOME, "python")
                + ":"
                + os.path.join(
                    SPARK_HOME,
                    "python",
                    "lib",
                    "py4j-0.10.9-src.zip"
                )
            ),
            "PYSPARK_PYTHON": PYTHON_PATH,
            "PYSPARK_DRIVER_PYTHON": PYTHON_PATH
        }
    )

    verify_output = BashOperator(
        task_id="verify_output",
        bash_command=(
            "test $(find "
            + os.path.join(PROJECT_DIR, "Output")
            + " -mindepth 2 -maxdepth 2 "
            + "-name '*.csv' | wc -l) -eq 7 "
            "&& echo 'All 7 CSV outputs generated successfully'"
        )
    )

    start_pipeline >> run_pyspark_queries >> verify_output

