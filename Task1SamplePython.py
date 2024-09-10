from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def helloworld():
    print("========= Welcome to Airflow ================")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}


with DAG('WelcomeToAirflow',
         default_args=default_args,
         schedule='@once',
         catchup=False) as dag:

    hello_task = PythonOperator(
        task_id='HelloWorld',
        python_callable=helloworld,
    )

    hello_task
