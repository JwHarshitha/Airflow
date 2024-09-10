import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import os


class ReadWriteOps:
    """
    This class is created for maintaining the read and write operations across the project.
    """
    def __init__(self,config) -> None:
        """
        To initialize this class we need a config parameter which holds all the project related parameters in the form of a json.
        """
        self.config = config
        self.input_path = config['FilePathParams']['input_path']
        self.output_path = config['FilePathParams']['output_path']

    def write_data_to_path(self,df, out_file_type, out_file_name=None, out_file_path=None):
        """
        Save a pandas DataFrame to a file with the specified format.

        Parameters:
        df (pandas.DataFrame): The DataFrame to save.
        out_file_type (str): The type of file to save the DataFrame as. Supported types are:
                        'csv', 'xlsx', 'json', 'parquet', 'txt'.
        out_file_name (str): The name of the file to be saved (without extension).
        out_file_path (str): The path where the file will be saved.

        Returns:
        None
        """
        print(out_file_name)
        out_file_type = out_file_type.lower()
        # Get the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Add the timestamp to the file name
        if out_file_name is None:
            full_file_name = f"{out_file_type}_{out_file_name}_{timestamp}"
        else:
            full_file_name = f"{out_file_name}_{timestamp}"

        if out_file_path is None:
            out_file_path = self.output_path
        
        full_path = os.path.join(out_file_path, f"{full_file_name}.{out_file_type}")
        print(full_path)

        if out_file_type == 'csv':
            df.to_csv(full_path, index=False)
        elif out_file_type == 'xlsx':
            df.to_excel(full_path, index=False, engine='openpyxl')
        elif out_file_type == 'json':
            df.to_json(full_path, orient='records', lines=True)
        elif out_file_type == 'parquet':
            df.to_parquet(full_path, index=False)
        elif out_file_type == 'txt':
            df.to_csv(full_path, index=False, sep='|', header=True)
        else:
            raise ValueError(f"Unsupported file type: {out_file_type}. Supported types are: 'csv', 'xlsx', 'json', 'parquet', 'txt'.")

        return True

    def read_data_from_path(self,in_file_path, in_file_name):
        """
        Read a file and return a pandas DataFrame.

        Parameters:
        in_file_path (str): The path where the file is located.
        file_name (str): The name of the file to read (without extension).
        file_type (str): The type of file to read. Supported types are:
                        'csv', 'excel', 'json', 'parquet', 'txt'.

        Returns:
        pandas.DataFrame: The DataFrame containing the file data.
        """
        # in_file_type = in_file_type.lower()
        in_file_type = os.path.splitext(in_file_name)[1][1:].lower()
        full_path = os.path.join(in_file_path, f"{in_file_name}")

        # Read the file based on the file type and return a DataFrame
        if in_file_type == 'csv':
            df = pd.read_csv(full_path)
        elif in_file_type == 'excel':
            df = pd.read_excel(full_path, engine='openpyxl')
        elif in_file_type == 'json':
            df = pd.read_json(full_path, orient='records', lines=True)
        elif in_file_type == 'parquet':
            df = pd.read_parquet(full_path)
        elif in_file_type == 'txt':
            df = pd.read_csv(full_path, sep='|')

        return df

    def read_data_from_db(self,query, engine):
        """
        Read data from a MySQL table and return it as a pandas DataFrame.

        Parameters:
        table_name (str): The name of the table to read from.
        engine (Engine): SQLAlchemy engine object connected to the MySQL database or SQL Server.

        Returns:
        pandas.DataFrame: The DataFrame containing the table data.

        # Example usage:
        # df_mysql = read_from_mysql_table('your_table_name', mysql_engine)
        """
        df = pd.read_sql(query, con=engine)

        return df
        # write_data_to_db(df,table_name,engine,'replace')÷ 
    def write_data_to_db(self,df,table_name,engine,if_exists='append'):
        """
        Write a pandas DataFrame to a MySQL table.

        Parameters:
        df (pandas.DataFrame): The DataFrame to write to the table.
        table_name (str): The name of the table to write to.
        engine (Engine): SQLAlchemy engine object connected to the MySQL database.
        if_exists (str): What to do if the table already exists. Options: 'fail', 'replace', 'append'. Default is 'replace'.

        Returns:
        None

        # Example usage:
        # write_to_mysql_table(df_mysql, 'your_table_name', mysql_engine)
        """
        try:
            df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
        except Exception as e:
            raise ValueError(f"Issue while writing to DB : {e}")

        return True
    

    def execute_non_returning_query(self,engine, query):
        """
        Execute a SQL query that does not return any results, such as DELETE, TRUNCATE, UPDATE, INSERT INTO.

        Parameters:
        engine (Engine): SQLAlchemy engine object connected to the database.
        query (str): The SQL query to execute.

        Returns:
        None

        # Example usage:
        # execute_non_returning_query(mysql_engine, "DELETE FROM your_table_name WHERE condition")
        # execute_non_returning_query(sqlserver_engine, "TRUNCATE TABLE your_table_name")
        """
        try:
            with engine.connect() as connection:
                connection.execute(text(query))
                connection.commit()
            print("Query executed successfully.")
        except Exception as e:
            raise ValueError(f"Issue while Executing : {e}")

        return True


