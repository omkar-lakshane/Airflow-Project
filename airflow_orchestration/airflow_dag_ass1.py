from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
import uuid

# Define arguments
default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'retries' : 1 ,
    'restart_delay' : timedelta(minutes=2),
    'start_date' : datetime(2025,10,19),       
}


# Define DAG

with DAG (
    'airflow_assignment1_dataproc',
    default_args = default_args,
    schedule_interval = '0 12 * * *',
    catchup = False
) as dag:

    # Fetch environment variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow_assignment_part1")
    bq_project = Variable.get("bq_project", default_var="glassy-totality-475309-u4")
    bq_dataset = Variable.get("bq_dataset", default_var=f"airflow_asgn_1_{env}")
    tables = Variable.get("tables")

    # Generate a unique batch ID using UUID
    batch_id = f"airflow-project1-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # --- 1️⃣ Sensors: Wait for files in both folders ---
    wait_for_folder1 = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_folder1",
        bucket=gcs_bucket,
        prefix=f"source_{env}/raw_data/employee/employee",   # waits until any file appears here
        poke_interval=30,
        timeout=300,
    )

    wait_for_folder2 = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_folder2",
        bucket=gcs_bucket,
        prefix=f"source_{env}/raw_data/department/department",   # waits until any file appears here
        poke_interval=30,
        timeout=300,
    )

    # --- 2️⃣ Combine dependencies ---
    ready_to_run = EmptyOperator(task_id="ready_to_run")

        # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/spark_job/spark_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--tables={tables}"
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "38357290145-compute@developer.gserviceaccount.com",
                "network_uri": "projects/glassy-totality-475309-u4/global/networks/default",
                "subnetwork_uri": "projects/glassy-totality-475309-u4/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=batch_id,
        project_id=bq_project,
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )


    [wait_for_folder1, wait_for_folder2] >> ready_to_run
    ready_to_run >> pyspark_task







#====================================================    
    # Define cluster config
    # CLUSTER_NAME = 'dataproc-spark-airflow-assignment1'
    # PROJECT_ID = 'glassy-totality-475309-u4'
    # REGION = 'us-central1'

        # CLUSTER_CONFIG = {
    #     'master_config': {
    #         'num_instances': 1, # Master node
    #         'machine_type_uri': 'n1-standard-2',  # Machine type
    #         'disk_config': {
    #             'boot_disk_type': 'pd-standard',
    #             'boot_disk_size_gb': 30
    #         }
    #     },
    #     'worker_config': {
    #         'num_instances': 2,  # Worker nodes
    #         'machine_type_uri': 'n1-standard-2',  # Machine type
    #         'disk_config': {
    #             'boot_disk_type': 'pd-standard',
    #             'boot_disk_size_gb': 30
    #         }
    #     },
    #     'software_config': {
    #         'image_version': '2.2.26-debian12'  # Image version
    #     }
    # }


    # cluster_details = Variable.get("cluster_details", deserialize_json=True)
    # CLUSTER_NAME = cluster_details['CLUSTER_NAME']
    # PROJECT_ID = cluster_details['PROJECT_ID']
    # REGION = cluster_details['REGION']

    # config = Variable.get("cluster_config", deserialize_json=True)
    
    # CLUSTER_CONFIG = {
    #     'master_config': config.get('master_config',{}),
    #     'worker_config': config.get('worker_config',{}),
    #     'software_config': config.get('software_config',{})
    # }
        

    # create_cluster = DataprocCreateClusterOperator(
    #     task_id='create_dataproc_cluster',
    #     cluster_name=CLUSTER_NAME,
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     cluster_config=CLUSTER_CONFIG,
    #     dag=dag,
    # )


    # submit_pyspark_job = DataprocSubmitJobOperator(
    #     task_id='submit_pyspark_job_on_dataproc',
    #     region = REGION,
    #     project_id = PROJECT_ID,
    #     job={
    #         'placement' : {'cluster_name' : CLUSTER_NAME},
    #         'pyspark_job' : {'main_python_file_uri' : 'gs://airflow_assignment_part1/spark_job/spark_job_ass1.py'},
    #     },
    #     # dataproc_pyspark_properties=spark_job_resources_parm,
    #     dag=dag,
    # )


    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id='delete_dataproc_cluster',
    #     project_id=PROJECT_ID,
    #     cluster_name=CLUSTER_NAME,
    #     region=REGION,
    #     trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    #     dag=dag,
    # )

    # create_cluster >> submit_pyspark_job >> delete_cluster
