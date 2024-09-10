from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Running my scheduled task")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('my_cron_dag',
         default_args=default_args,
         schedule='0 6 1 * *',  # Run at 6 AM on the 1st day of every month
         catchup=False) as dag:

    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task,
    )