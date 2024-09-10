from sqlalchemy import create_engine
import urllib.parse


class DBConnectors:
    """
    This class is created for maintaining the database connections across the project.
    """
    def __init__(self,config) -> None:
        """
        To initialize this class we need a config parameter which holds all the database related parameters in the form of a json.
        """
        self.config = config

    def create_mysql_engine(self):
        """
        This function helps us in creating a MySQL Engine
        """
        host = self.config['MySQLConnection']['host']
        username = self.config['MySQLConnection']['username']
        password = self.config['MySQLConnection']['password']
        database_name = self.config['MySQLConnection']['database_name']
        encoded_password = urllib.parse.quote_plus(password)
        connection_string = f'mysql+mysqlconnector://{username}:{encoded_password}@{host}/{database_name}'
        engine = create_engine(connection_string)

        return engine

    def create_sqlserver_engine(self):
        """
        This function helps us in creating a SQL Server Engine
        """
        host = self.config['SQLServerConnection']['host']
        port = self.config['SQLServerConnection']['port']
        username = self.config['SQLServerConnection']['username']
        password = self.config['SQLServerConnection']['password']
        database_name = self.config['SQLServerConnection']['database_name']
        encoded_password = urllib.parse.quote_plus(password)
        connection_string = (
            f"mssql+pyodbc://{username}:{encoded_password}@{host}:{port}/{database_name}?driver=ODBC+Driver+18+for+SQL+Server&encrypt=no"
        )
        engine = create_engine(connection_string)

        return engine
    
    