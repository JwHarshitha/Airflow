import configparser
import os
from DataEngineering.DEUtilities import DBConnectors, ReadWriteOps


class DataEngineeringBase:
    """
    This class is created for developing a standard frame work to migrate the data between different sources
    majorly we are targeting the below types of source to target movement:
    1. File Path to File Path
    2. File Path to Database
    3. Database to File Path
    4. Database to Database
    """
    def __init__(self,config) -> None:
        self.config = config
        
        self.db_obj = DBConnectors.DBConnectors(config=self.config)
        self.rw_obj = ReadWriteOps.ReadWriteOps(config=self.config)
        self.mysql_engine = self.db_obj.create_mysql_engine()
        self.sqlserver_engine = self.db_obj.create_sqlserver_engine()

    def migrate_data_from_path_to_path(self,**kwargs):
        """
        This function is created form migration of data from one location to the other
        for this scenario we have considered the local mac path and moving between input and output
        """
        try:
            df = self.rw_obj.read_data_from_path(in_file_path=kwargs['in_file_path'],
                                                in_file_name=kwargs['in_file_name']
                                                )
            self.rw_obj.write_data_to_path(df=df,
                                        out_file_type=kwargs['out_file_type'],
                                        out_file_name=kwargs['out_file_name'],
                                        out_file_path=kwargs['out_file_path']
                                        )
        except Exception as e:
            raise ValueError(f"Some issue while writing the data to path : {e}")
        
    def migrate_data_from_path_to_db(self,**kwargs):
        """
        This function is created form migration of data from one location to the other
        for this scenario we have considered the local mac path and moving between input and output
        """
        try:
            db_type = kwargs['db_type']
            table_name = kwargs['table_name']
            if db_type.lower() == 'mysql':
                engine = self.mysql_engine
            elif db_type.lower() == 'sqlserver':
                engine = self.sqlserver_engine
            df = self.rw_obj.read_data_from_path(in_file_path=kwargs['in_file_path'],
                                                in_file_name=kwargs['in_file_name']
                                                )
            self.rw_obj.write_data_to_db(df,table_name,engine,if_exists='replace')
        except Exception as e:
            raise ValueError(f"Some issue while writing the data to path : {e}")
        

if __name__ == '__main__':
    exec_type = 'local'
    if exec_type == 'local':
        config_path = 'DataEngineering/Config/dev.ini'
        from DEUtilities import DBConnectors, ReadWriteOps
    else:
        config_path = 'dags/DataEngineering/Config/dev.ini'
        from DataEngineering.DEUtilities import DBConnectors, ReadWriteOps

    config_path = config_path
    config = configparser.ConfigParser()
    config.read(config_path)
    if len(config.sections()) != 0:
        print(config.sections())
        pass
    else:
        print(config.sections())
        raise ValueError(f"Config File is not loaded properly please check the path of the file : {config_path}")
    input_path = config["FilePathParams"]["input_path"]
    output_path = config["FilePathParams"]["output_path"]

    base_obj = DataEngineeringBase(config)
    base_obj.migrate_data_from_path_to_path(in_file_path=input_path,
                                            in_file_name='Employee.csv',
                                            out_file_type='json',
                                            out_file_name="Employee",
                                            out_file_path=output_path)
    base_obj.migrate_data_from_path_to_db(in_file_path=input_path,
                                          in_file_name='Employee.csv',
                                          db_type='sqlserver',
                                          table_name='EmployeeCheckAirflow')