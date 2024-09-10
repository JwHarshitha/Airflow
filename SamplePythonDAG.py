from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from random import randint
from DataEngineering import DataEngineeringBase
import os
import configparser

config_path = 'dags/DataEngineering/Config/dev.ini'
config_path = config_path
config = configparser.ConfigParser()
config.read(config_path)
if len(config.sections()) != 0:
        print(config.sections())
        pass
else:
        print(config.sections())
        raise ValueError(f"Config File is not loaded properly please check the path of the file : {config_path}")
input_path = 'dags/Data/Input/'
output_path = 'dags/Data/Output/'

base_obj = DataEngineeringBase.DataEngineeringBase(config)
        

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="DataEngineeringBase", start_date=datetime(2024, 9, 1),schedule="@once",catchup=False) as dag:
    # Tasks are represented as operators
        @task
        def path_to_path():
                base_obj.migrate_data_from_path_to_path(in_file_path=input_path,
                                                in_file_name='Employee.csv',
                                                out_file_type='json',
                                                out_file_name="Employee",
                                                out_file_path=output_path)
                
        @task
        def path_to_mysqldb():
                base_obj.migrate_data_from_path_to_db(in_file_path=input_path,
                                          in_file_name='Employee.csv',
                                          db_type='mysql',
                                          table_name='EmployeeCheckAirflow')
        
        @task
        def path_to_sqlserverdb():
                base_obj.migrate_data_from_path_to_db(in_file_path=input_path,
                                          in_file_name='Employee.csv',
                                          db_type='sqlserver',
                                          table_name='EmployeeCheckAirflow')
                
        # Define task dependencies
        t1 = path_to_path()
        t2 = path_to_mysqldb()
        t3 = path_to_sqlserverdb()
        # t4 = task4()

        t1 >> [t2,t3]