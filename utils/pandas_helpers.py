import pandas as pd


def save_data(data, columns, filepath):
    '''
    Save data into csv using a DataFrame
    '''
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(filepath, index=False)
    return df