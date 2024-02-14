import pandas as pd
from utils.pandas_helpers import format_dataframe, check_for_null_values



def add_relevant_columns(dataframe, symbol):
    relevant_columns = ['symbol', 'open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 
                        'close_time', 'quote_asset_volume', 'number_of_trades', 'extraction_date', 'extraction_params']  
    dataframe['symbol'] = symbol
    df = dataframe[relevant_columns]
    return df


def transform_data(input_file, symbol, outut_file):
    df = pd.read_csv(input_file)
    n_df = add_relevant_columns(df, symbol=symbol)
    transformed_df = format_dataframe(dataframe=n_df, time_unit='ms')
    check_for_null_values(transformed_df)
    transformed_df.to_csv(outut_file, index=False)