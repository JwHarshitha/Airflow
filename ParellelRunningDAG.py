from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# from temporary import temp

def helloworld():
    print("========= Welcome to Airflow ================")

def helloworld2():
    print("========= Welcome back to Airflow ================")

def helloworld3():
    print("========= See you soon, Airflow ================")

def helloworld4():
    print("========= Goodbye, from Airflow ================")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}


with DAG('WelcomeToAirflowParallel',
         default_args=default_args,
         schedule='@once',
         catchup=False,
         max_active_tasks=10,  # Override dag_concurrency for this DAG
        max_active_runs=2    # Limit concurrent DAG runs
        ) as dag:

    hello_task = PythonOperator(
        task_id='HelloWorld',
        python_callable=helloworld,
    )

    hello_task2 = PythonOperator(
        task_id='HelloWorld2',
        python_callable=helloworld2,
    )

    hello_task3 = PythonOperator(
        task_id='HelloWorld3',
        python_callable=helloworld3,
    )

    hello_task4 = PythonOperator(
        task_id='HelloWorld4',
        python_callable=helloworld4,
    )

    [hello_task4 ,hello_task, hello_task2 ,hello_task3]