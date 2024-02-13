from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from fonctions import extraction,transform,loading_db,loading_csv 

default_args = {
    'owner': 'Ayoub',
    'start_date': days_ago(0),
    'email' : ['fraija100@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries': 0,
    'retry delay':timedelta(minutes=5)
}

dag=DAG(
    'JUMIA_SCAPING',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Scraping from Jumia'
)

Extraction_Donnée=PythonOperator(
    task_id='extract',
    python_callable=extraction,
    dag=dag
)
Transformation_Donnée=PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)
Chargement_Donnée_Bd=PythonOperator(
    task_id='load_db',
    python_callable=loading_db,
    dag=dag
)
Chargement_Donnée_csv=PythonOperator(
    task_id='load_csv',
    python_callable=loading_csv,
    dag=dag
)
Extraction_Donnée >> Transformation_Donnée >> [Chargement_Donnée_Bd,Chargement_Donnée_csv]


