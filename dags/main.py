from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime,timedelta

from MoviesExtract import ingest_data
from TransformData import transform_data

default_args={
    'owner': 'airflow',
    'depends_on_past' : False,
    'start_date':datetime(2023,12,8),
    'email' : ['airflow@example.com']
}

ingestion_dag=DAG(
    'ott-details-ingestion',
    default_args =default_args,
    description ="ott-dag"
)

ingestion=PythonOperator(
    task_id='ott_ingestion',
    python_callable=ingest_data,
    dag=ingestion_dag
)

transform=PythonOperator(
    task_id="ott_transformation",
    python_callable=transform_data,
    dag=ingestion_dag
)

ingestion >> transform