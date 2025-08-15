

from airflow import models
import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



CLUSTER_NAME = 'dataproc-airflow-cluster'
REGION='us-central1' # region
PROJECT_ID='affable-operand-468818-f9' #project name
PYSPARK_URI='gs://us-central1-composer-airflo-f3b665a4-bucket/codes/main_pyspark_job.py' # spark job location in cloud storage


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

default_args = {
    'owner': 'Fer Flores',                   # The owner of the task.
    'depends_on_past': False,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2025, 8, 5),
    'retries': 1,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=1),  # Time between retries
}

with models.DAG(
    "DAG-poc01-data-pipeline",
    default_args = default_args,
    schedule_interval=None,  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:

    t_begin = DummyOperator(task_id="begin")

    ''' Create dataproc '''
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    gsc_to_gbq = GCSToBigQueryOperator(
        task_id="transfer_data_to_bigquery",
        bucket="us-central1-composer-airflo-f3b665a4-bucket",
        source_objects =["output_files/*.snappy.parquet"],
        destination_project_dataset_table ="affable-operand-468818-f9.airflow_test.test_data_pyspk", # bigquery table
        source_format = "PARQUET"
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    t_end = DummyOperator(task_id="end")

    t_begin >> create_cluster >> submit_job >> [delete_cluster,gsc_to_gbq] >> t_end
