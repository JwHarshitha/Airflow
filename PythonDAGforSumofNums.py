from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def sum_two_numbers(**kwargs):
    num1 = kwargs.get('num1', 0)
    num2 = kwargs.get('num2', 0)
    result = num1 + num2
    print(f"The sum of {num1} and {num2} is {result}")
    return result

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('sum_numbers_dag',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    sum_task = PythonOperator(
        task_id='sum_task',
        python_callable=sum_two_numbers,
        op_kwargs={'num1': 10, 'num2': 20},  # Pass the numbers here
    )

sum_task
