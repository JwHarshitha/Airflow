from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_1():
    print("Task 1")

def task_2():
    print("Task 2")

def task_3():
    print("Task 3")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('trigger_rule_dag',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1,
    )

    t2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2,
    )

    t3 = PythonOperator(
        task_id='task_3',
        python_callable=task_3,
        trigger_rule='one_success',  # Triggered if either task_1 or task_2 succeeds
    )

    t1 >> t3
    t2 >> t3
