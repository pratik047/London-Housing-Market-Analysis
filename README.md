# London Housing Market Analysis Using PySpark, Apache Airflow & Tableau

## Project Overview

London Housing Market Analysis is a Data Engineering and Analytics project developed as part of the PGCP – Big Data Analytics program at C-DAC IACSD.

The project processes approximately 282,853 London property records using Python and PySpark. It performs data cleaning, data quality validation and seven analytical queries to explore property prices, rental estimates, property characteristics and tenure-wise trends.

Apache Airflow is used to orchestrate the data processing workflow through a DAG, while Tableau Public is used to build an interactive dashboard for visualizing the analytical results.

The project demonstrates an end-to-end batch data processing workflow, from raw dataset preparation to analytical reporting.

## Project Objectives

- Process a large London housing dataset using PySpark.
- Remove exact duplicate records.
- Identify missing and invalid values.
- Validate sale-price and rental estimate consistency.
- Perform seven analytical queries using Spark DataFrames.
- Automate and orchestrate pipeline execution using Apache Airflow.
- Store analytical outputs as CSV files.
- Build an interactive Tableau dashboard.
- Understand practical Data Engineering concepts and workflow management.

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Application development |
| Apache Spark 3.1.1 | Distributed data processing |
| PySpark | Data cleaning and analytical transformations |
| Apache Airflow 1.10.10 | Workflow orchestration |
| Spark SQL DataFrames | Data transformation and aggregation |
| Tableau Public | Dashboard and data visualization |
| CSV | Input dataset and analytical outputs |
| Linux / Ubuntu | Development environment |
| YARN | Execution in the configured Spark environment |
| Python Logging | Execution logs and error reporting |
| Git and GitHub | Version control and project publishing |

## Project Architecture

```text
              London Housing Dataset
                         |
                         v
                 Raw CSV Dataset
                         |
                         v
                PySpark Data Cleaning
                  validate_data.py
                         |
                         v
                  Cleaned Dataset
                         |
             +-----------+-----------+
             |                       |
             v                       v
      Data Quality Checks      PySpark Analysis
   data_quality_report.py          mini.py
             |                       |
             v                       v
      Quality Report          Seven Query Results
                                     |
                                     v
                                CSV Outputs
                                     |
                                     v
                              Tableau Public
                                     |
                                     v
                            Interactive Dashboard

              Apache Airflow Orchestration
          start_pipeline -> run_pyspark_queries
                         -> verify_output
```

Apache Airflow manages the execution sequence of the PySpark processing pipeline. The DAG was manually triggered and successfully executed all three tasks.

## Dataset Description

The project uses a London housing market dataset containing property information and estimated market values.

**Dataset details:**

- Original records: 282,872
- Records after duplicate removal: 282,853
- Exact duplicate records removed: 19
- Total columns: 28
- Original dataset size: approximately 76 MB

The dataset contains information such as:

- Property postcode and country
- Property type
- Bedrooms and bathrooms
- Living rooms and floor area
- Property tenure
- Current sale-price estimates
- Historical sale-price changes
- Rental estimates

**Dataset location:**

`Dataset/london_house_price_data.csv`

The original and cleaned datasets are excluded from the GitHub repository because of their size. Obtain the dataset separately and place it in the expected Dataset directory before running the pipeline.

## Data Cleaning

The `validate_data.py` script performs the initial data preparation.

Main operations:

- Reads the original CSV dataset using PySpark.
- Identifies and removes exact duplicate rows.
- Writes the cleaned dataset to a Spark output directory.
- Prepares the cleaned dataset for further analysis.

### Data Cleaning Results

| Metric | Result |
|---|---:|
| Original records | 282,872 |
| Duplicate records removed | 19 |
| Cleaned records | 282,853 |
| Total columns | 28 |

Cleaned dataset location:

`Dataset/cleaned_london_house_price_data/`

## Data Quality Validation

The `data_quality_report.py` script performs data quality checks on the cleaned dataset.

### Checks performed

- Total record and column count.
- Missing-value count and percentage for each column.
- Negative values in selected numerical columns.
- Zero values in bedrooms and bathrooms.
- Sale-price estimate consistency.
- Rental estimate consistency.
- Summary of data quality findings.

### Missing-value findings

| Column | Missing Records | Missing % |
|---|---:|---:|
| bedrooms | 26,215 | 9.27% |
| bathrooms | 51,100 | 18.07% |
| floorAreaSqM | 15,801 | 5.59% |
| livingRooms | 39,131 | 13.83% |
| currentEnergyRating | 58,297 | 20.61% |
| tenure | 6,310 | 2.23% |
| propertyType | 675 | 0.24% |

Other columns also contain missing values, including rental estimates, sale estimates and historical price-change fields.

### Estimate Consistency Validation

The project checks:

- Sale estimates: lower price <= current price <= upper price.
- Rent estimates: lower price <= current price <= upper price.

| Validation | Inconsistent Records |
|---|---:|
| Sale estimate consistency | 0 |
| Rent estimate consistency | 0 |

Missing values are reported but are not automatically imputed or removed by the validation script.

## PySpark Analytical Queries

The project implements seven analytical queries using Spark DataFrames, grouping, aggregation, sorting and approximate percentiles.

| Query | Analysis | Main Operations |
|---|---|---|
| Q1 | Postcode with the highest average sale price | groupBy, avg, orderBy, limit |
| Q2 | Property type with the highest average bathrooms | groupBy, avg, orderBy, limit |
| Q3 | Property count by country | groupBy, count |
| Q4 | Average sale-price percentage change by tenure | groupBy, avg |
| Q5 | Country with the highest average rent estimate | groupBy, avg, orderBy, limit |
| Q6 | Property type with the highest average bedrooms | groupBy, avg, orderBy, limit |
| Q7 | Median sale price by tenure | groupBy, percentile_approx |

### Key Analytical Findings

| Query | Result |
|---|---|
| Q1 | SW1X 8HB – Average sale price approximately £25.68 million |
| Q2 | Detached Property – Average bathrooms: 3.16 |
| Q3 | England – 282,853 property records |
| Q4 | Feudal tenure – Highest average percentage change: approximately 41.32% |
| Q5 | England – Average rent estimate: approximately £3,589.38 |
| Q6 | Detached House – Average bedrooms: 4.12 |
| Q7 | Freehold – Highest median sale price: £869,000 |

Monetary values are displayed in GBP. The rental period is not specified in the dataset, so the average rent figure is not labelled as monthly or yearly.

## Apache Airflow Workflow Orchestration

Apache Airflow was integrated to automate the execution and verification of the PySpark pipeline.

**Airflow environment:**

- Apache Airflow: 1.10.10
- Python: 3.7.11
- Apache Spark: 3.1.1
- Operating System: Ubuntu Linux

### DAG Workflow

DAG name: `london_house_price_dag`

The workflow contains three tasks:

**1. start_pipeline**
- Starts the pipeline execution.
- Prepares the workflow for PySpark processing.

**2. run_pyspark_queries**
- Executes the PySpark application.
- Runs the seven analytical queries.
- Generates the CSV output files.

**3. verify_output**
- Checks the generated output.
- Verifies that the expected analytical results are available.

### DAG Execution Flow

```text
          start_pipeline
                 |
                 v
       run_pyspark_queries
                 |
                 v
          verify_output
                 |
                 v
              SUCCESS
```

The DAG was manually triggered from the Airflow web interface. All three tasks completed successfully, and the seven query output folders were generated.

### Airflow DAG Location

Original development location:

`~/Desktop/airflow-tutorial/dags/london_house_price_dag.py`

For the published repository, the DAG should also be included in the project's `dags/` directory.

### Running Airflow

Activate the configured Airflow environment and set the Airflow home directory according to your local setup.

Start the Airflow webserver and scheduler in separate terminals:

```bash
airflow webserver -p 8080
```

```bash
airflow scheduler
```

Open the Airflow interface:

`http://localhost:8080`

Enable the `london_house_price_dag` DAG and trigger it manually.

Check the task status and logs through the Airflow interface.

**Note:** The Airflow environment and Spark configuration used in this project are based on the original development machine. Paths and dependencies may need adjustment on another system.

## Analytical Output Files

The seven analytical query results are saved in separate Spark output directories.

```text
Output/
├── Q1_max_avg_sale_price/
├── Q2_max_avg_bathrooms/
├── Q3_properties_by_country/
├── Q4_price_change_by_tenure/
├── Q5_highest_avg_rent/
├── Q6_max_avg_bedrooms/
└── Q7_median_sale_price/
```

Each folder contains a Spark-generated CSV part file.

The output directories are retained in the repository so that the analytical results can be reviewed without rerunning the complete pipeline.

Spark-generated `_SUCCESS` and `.crc` files are not required for reviewing the CSV results.

## Tableau Public Dashboard

Tableau Public was used to create an interactive dashboard based on the seven PySpark analytical outputs.

**Dashboard link:**

https://public.tableau.com/views/miniPro/Dashboard1?:language=en-US&publish=yes

### Dashboard Components

The dashboard includes five KPI cards:

- Highest average sale price and postcode.
- Highest average bathrooms and property type.
- Total property records and country.
- Highest average rent estimate and country.
- Highest average bedrooms and property type.

It also includes two analytical charts:

**1. Average Percentage Change in Sale Price by Tenure Type**
- Compares the average percentage change across property tenure categories.

**2. Median Sale Price by Tenure Type**
- Compares median sale prices across different tenure categories.

### Dashboard Workflow

```text
PySpark Query Results
         |
         v
   Output CSV Files
         |
         v
   Tableau Public
         |
         v
  Data Connections
         |
         v
 KPI Cards and Charts
         |
         v
 Interactive Dashboard
```

The dashboard connects the analytical outputs generated by PySpark with visual reporting in Tableau Public.

## Project Execution

### Prerequisites

- Python 3
- Java compatible with the installed Spark version
- Apache Spark 3.1.1
- PySpark
- Apache Airflow 1.10.10 for DAG orchestration
- Original London housing dataset
- Tableau Public account for publishing the dashboard

### Step 1: Navigate to the project

```bash
cd ~/Lab_practice/mini
```

### Step 2: Clean the dataset

```bash
/opt/spark3/bin/spark-submit validate_data.py
```

### Step 3: Generate the data quality report

```bash
/opt/spark3/bin/spark-submit data_quality_report.py > data_quality.log 2>&1
```

### Step 4: Execute PySpark queries

```bash
/opt/spark3/bin/spark-submit mini.py
```

### Step 5: Execute using the shell script

```bash
chmod +x run-demo.sh
./run-demo.sh
```

To save execution output:

```bash
./run-demo.sh > output.log 2>&1
```

### Step 6: Execute through Apache Airflow

Start the Airflow webserver and scheduler, open the Airflow UI and manually trigger the `london_house_price_dag`.

### Step 7: Visualize using Tableau Public

Connect the seven generated CSV result files to Tableau and use them to build the KPI cards and analytical charts.

## Logging and Error Handling

The project uses Python logging and exception handling to monitor execution.

Implemented features:

- Logs query execution start and completion.
- Captures query execution errors.
- Uses exception handling to prevent silent failures.
- Stops the Spark session in the `finally` block.
- Maintains data cleaning, data quality and application logs.
- Uses Airflow task logs to monitor individual workflow execution.

## Testing and Validation

The project includes supporting scripts:

- `test_project.py` – Project testing.
- `test_project.py` – Output validation.
- `validate_data.py` – Dataset cleaning.
- `data_quality_report.py` – Data quality checks.

The Airflow workflow also contains an output verification task.

## Challenges and Learning Outcomes

During development, the project involved working with:

- Processing a large CSV dataset using PySpark.
- Removing duplicate records without losing valid data.
- Identifying missing and invalid values.
- Validating nested sale-price and rental estimate fields.
- Understanding Spark-generated output directories and CSV part files.
- Managing execution logs and pipeline errors.
- Configuring and executing a multi-task Airflow DAG.
- Connecting PySpark analytical results to Tableau Public.
- Presenting analytical findings through KPI cards and charts.

The project strengthened practical understanding of batch processing, distributed data transformations, workflow orchestration and analytical visualization.

## Limitations and Future Enhancements

- Implement appropriate missing-value treatment strategies.
- Store cleaned data in Parquet format.
- Make dataset paths configurable.
- Add command-line parameters for pipeline execution.
- Add more automated unit tests.
- Extend the pipeline to cloud storage.
- Explore incremental data processing.
- Introduce streaming processing using Kafka as a future enhancement.
- Improve pipeline monitoring and alerting.

These are possible future improvements and are not claimed as currently implemented features.

## Author

**Name:** Pratik Dabade  
**Course:** PGCP – Big Data Analytics (BDA)  
**Institute:** C-DAC IACSD  

**GitHub Repository:** Add your repository URL here.

**Tableau Public Dashboard:**  
https://public.tableau.com/views/miniPro/Dashboard1?:language=en-US&publish=yes

## Conclusion

The London Housing Market Analysis project demonstrates an end-to-end batch data processing and analytics workflow.

PySpark is used for data cleaning, quality validation and analytical processing. Apache Airflow orchestrates the pipeline execution, while Tableau Public presents the results through an interactive dashboard.

The project provided practical experience with distributed data processing, data quality, workflow orchestration, logging, validation and data visualization.
