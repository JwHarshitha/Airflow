from pandas import json_normalize
import requests
import pandas as pd

class DataTransformations:
    """
    This class is created for performing the transformations to a dataframe and load it to the csv file.
    """
    def __init__(self) -> None:
        pass

    def get_request(self, url):
        """
        Reads a URL using requests library  and returns json data.

        Parameters:
        url (str): The url of the api, from which the data needs to be fetched.

        Returns:
        data (json): The json object containing the api data.
        """

        response = requests.get(url)
        # Check the status code (200 indicates success)
        if response.status_code == 200:
            # Convert the response to JSON format
            data = response.json()
        else:
            # print(f'Failed to retrieve data: {response.status_code}')
            data=f'Failed to retrieve data: {response.status_code}'

        return data

    def create_df(self,url):
        """
        Creates a dataframe using the json data and returns a pandas DataFrame.

        Parameters:
        url (str): The url of the api, from which the data needs to be fetched.

        Returns:
        pandas.DataFrame: The DataFrame containing the file data.
        """

        try:
            data= self.get_request(url)
            df= json_normalize(data, sep='_')
            # df.to_csv('Dags/Data/raw_data.csv', index=False)
            return df
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

    def data_cleaning(self,df):
        """
        Applies pandas functions and transforms the given DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame containing the file data.

        Returns:
        pandas.DataFrame: The DataFrame after the transformations are done.
        """
        try:
            df.fillna(value={'roi_times':0,'roi_currency':'N/A','roi_percentage':0},inplace=True)
            df[['current_price', 'market_cap', 'circulating_supply', 'total_supply']] = df[['current_price', 'market_cap', 'circulating_supply', 'total_supply']].apply(lambda x: x.astype(float).round(2))
            df[['roi_times', 'roi_percentage']] = df[['roi_times', 'roi_percentage']].apply(lambda x: x.astype(float))
            # Standardize string formats (e.g., converting all strings to lowercase)
            df['name'] = df['name'].str.title()
            df['symbol'] = df['symbol'].str.upper()
            # Drop duplicate rows if there are any
            df.drop_duplicates(inplace=True)
            date_columns = ['ath_date', 'atl_date', 'last_updated']
            df[date_columns] = df[date_columns].apply(lambda x: pd.to_datetime(x))
            df_cleaned = df.dropna(axis=1, how='all')

            return df_cleaned
        
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

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
            print("Transformed data loaded successfully")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")

# if __name__=='__main__':
#     obj=DataTransformations()
#     df=obj.create_df(url='https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false')
#     obj.load_to_csv(df,"raw_data")
#     df_cleaned=obj.data_cleaning(df)
#     obj.load_to_csv(df_cleaned,"cleaned_data")
