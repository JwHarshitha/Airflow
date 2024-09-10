from pandas import json_normalize
import requests

class DataExtractor:
    """
    This class is created for extracting the data into a dataframe.
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