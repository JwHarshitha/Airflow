from airflow import DAG
from airflow.operators.python import PythonOperator
from DataTransformations import DataTransformations
from datetime import datetime


def latest_data_transformation(**kwargs):
    url = kwargs.get('url')
    # Check if the URL is provided
    if url is None:
        raise ValueError("URL not provided.")
    data_object=DataTransformations()
    df=data_object.create_df(url)
    data_object.load_to_csv(df,"raw_data")
    df_cleaned=data_object.data_cleaning(df)
    data_object.load_to_csv(df_cleaned,"cleaned_data")
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('DataTransformationAPIDAG',
         default_args=default_args,
         schedule='@once',
         catchup=False) as dag:
    
    # Task 1: Get Request
    task1 = PythonOperator(
        task_id='GetRequestTask',
        python_callable=latest_data_transformation,
        op_kwargs={'url': "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"}
    )

    task1
