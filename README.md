# ETL_Ecommerce
When the user uploads a file to the GCS bucket it will automatically trigger the Airflow DAG from the cloud function. The DAG contains mainly three tasks.
The first task will run a dataflow job (python) that will load file data into BigQuery Table as raw data.
A second task will insert raw BQ tables data into another table as trusted data.
The third task will move the file from the current bucket to an archive bucket/folder
