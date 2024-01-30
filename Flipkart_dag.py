import datetime

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow import models
from airflow.operators.dummy_operator import DummyOperator

from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

file_name="flipkart_mobiles.csv"
#variables
gcp_sql_path ="gs://landing12/sql_script"

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'project': 'cloudfunction-12',
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with models.DAG(
        "flipkart-mobile-prices-etl-dag",
        default_args=default_args,
        start_date=datetime.datetime(2023, 12, 30),
        # Not scheduled, trigger only
        #schedule_interval=datetime.timedelta(days=1) removed for trigger from cloud function
        schedule_interval=None,
) as dag:
    
    start=DummyOperator(
      task_id="start_task_id",
      dag=dag
    )



    
    dataflow_job_file_to_bq = DataflowCreatePythonJobOperator(
        task_id="dataflow_jobs_file_to_bq",
        py_file="gs://us-central1-flipkart01-dag--ad03eb8e-bucket/Flipkart_dataflow.py",
        job_name="etlflipkart12",
        options = {
                'project': 'cloudfunction-12'
                },
        dataflow_default_options = {
            "inputfile": "gs://flipkart_input_files_12/flipkart_mobiles.csv",
            "temp_location": "gs://tmp-12/",
            "staging_location": "gs://stagging-12/",
            "region": "us-west1"
            }

    )

    insert_mobile_price_into_trst_table = BashOperator(
        task_id='insert_mobile_price_into_trst_table',
        bash_command= 'bq query --nouse_legacy_sql "`gsutil cat {}/Insert_data_dml.sql`"'.foramt(gcp_sql_path),
        dag=dag,
    )

    archive_file = GCSToGCSOperator(
        task_id="archive_file",
        source_bucket="flipkart_input_files_12",
        source_object="flipkart_mobiles.csv",
        destination_bucket="archive-12",
        destination_object="archive-12"+str(datetime.date.today())+"-"+file_name,
        move_object=True,
    )

    end=DummyOperator(
        task_id="end_task_id",
        dag=dag
    )  

start>>dataflow_job_file_to_bq  >> archive_file>>end

