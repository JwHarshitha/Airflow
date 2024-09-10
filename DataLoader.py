class DataLoader:
    """
    This class is for loading any dataframe to the csv file.
    """
    def __init__(self) -> None:
        pass

    def load_to_csv(self, df_cleaned, table_name):
        """
        Applies pandas functions and transforms the given DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame containing the file data.
        table_name (str): The name that needs to be given to the csv file

        Returns:
        None
        """
        try:
            df_cleaned.to_csv(f'dags/Data/{table_name}.csv', index=False)
            print("Data loaded successfully")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")