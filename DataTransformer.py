import pandas as pd

class DataTransformer:
    """
    This class is created for performing the transformations to a dataframe and load it to the csv file.
    """
    def __init__(self) -> None:
        pass

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