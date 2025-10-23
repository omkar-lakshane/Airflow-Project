import argparse
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"    
)

logger = logging.getLogger(__name__)

def processed_data(env, bq_project, bq_dataset, tables):
    try:

        spark = SparkSession.builder.appName("GCPDataProcJob").getOrCreate()

        # Define input path
        emp_data = f'gs://airflow_assignment_part1/source_{env}/raw_data/employee/employee.csv'
        dept_data = f'gs://airflow_assignment_part1/source_{env}/raw_data/department/department.csv'

        # Define bucket and data set variables
        # bucket = 'airflow_assignment_part1'
        # emp_data = f'gs://{bucket}/raw_data/employee.csv'
        # dept_data = f'gs://{bucket}/raw_data/department.csv'
        # final_data = f'gs://{bucket}/final_data'

        # Read datasets
        employee = spark.read.csv(emp_data, header=True, inferSchema=True)
        department = spark.read.csv(dept_data, header=True, inferSchema=True)

        # Filter employee data
        filtered_employee = employee.filter(employee.salary > 50000) # Adjust the salary threshold as needed

        # Join datasets
        joined_data = filtered_employee.join(department, "dept_id", "inner")

        # Write output
        # joined_data.write.csv(final_data, mode="overwrite", header=True)

        logger.info(f"Writing final data to BigQuery table: {bq_project}:{bq_dataset}.{tables}")
        joined_data.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{tables}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

    except Exception as e:
        logger.error(f"An error occured : {e}")
        sys.exit(1)

    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Process employee data and write to BigQuery.")
    parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
    parser.add_argument("--bq_project", required=True, help="BigQuery project ID")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--tables", required=True, help="BigQuery table for final data")

    args = parser.parse_args()


    processed_data(
        env=args.env,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        tables=args.tables
    )