from airflow import DAG
from airflow.operators.python import PythonOperator
from DataExtractor import DataExtractor
from DataTransformer import DataTransformer
from DataLoader import DataLoader
from datetime import datetime

def etl_data_transformation(**kwargs):
    url = kwargs.get('url')
    # Check if the URL is provided
    if url is None:
        raise ValueError("URL not provided.")
    extract_object=DataExtractor()
    df=extract_object.create_df(url)
    load_object=DataLoader()
    load_object.load_to_csv(df,"ETL_raw_data")
    transform_object=DataTransformer()
    df_cleaned=transform_object.data_cleaning(df)
    load_object.load_to_csv(df_cleaned,"ETL_cleaned_data")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('ETLDataTransformationAPIDAG',
         default_args=default_args,
         schedule='0 * * * *',
         catchup=False) as dag:
    
    # Task 1: Get Request
    task1 = PythonOperator(
        task_id='GetRequestTask',
        python_callable=etl_data_transformation,
        op_kwargs={'url': "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"}
    )

    task1