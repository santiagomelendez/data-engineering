import pandas as pd


def save_data(data, columns, filepath):
    '''
    Save data into csv using a DataFrame
    '''
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(filepath, index=False)
    return df


def format_dataframe(dataframe, time_unit=None):
    new_df = dataframe
    float64_columns = dataframe.select_dtypes(include='float64').columns.tolist()
    datetime_columns = ['open_time', 'close_time']
    new_df.loc[:, float64_columns] = new_df[float64_columns].round(2)
    new_df.loc[:, 'symbol'] = new_df['symbol'].str.lower()
    for colum in datetime_columns:
        new_df.loc[:, colum] = pd.to_datetime(new_df[colum], unit=time_unit) if time_unit else pd.to_datetime(new_df[colum]) 
    return new_df


def check_for_null_values(df):
    if df.isnull().values.any():
        raise ValueError("DataFrame contains one or more null values.")
