import pandas as pd
from utils.pandas_helpers import format_dataframe
from utils.persistence import engine, execute_query


def upload_data(input_file):
    df = pd.read_csv(input_file)
    new_data_df = df[df['should_be_updated'] == False]
    updated_df = df[df['should_be_updated'] == True]
    upload_new_data(dataframe=new_data_df)
    if len(updated_df) > 0:
        update_existing_data(dataframe=updated_df)


def upload_new_data(dataframe):
    print('UPLOAD: Formatting new data before upload')
    new_data_df = format_dataframe(dataframe=dataframe)
    new_data_df = new_data_df[new_data_df.columns[:-2]]
    print(f'UPLOAD: Will be upload {len(new_data_df)} new registers')
    new_data_df.to_sql(name='klines', con=engine, if_exists='append', index=False, method='multi')
    print(f'UPLOAD: Upload successful: {len(new_data_df)} new registers')


def update_existing_data(dataframe):
    print('UPLOAD: Formatting existing data before upload')
    update_data_df = format_dataframe(dataframe=dataframe)
    print(f'UPLOAD: Will be updated {len(update_data_df)} registers')
    update_data_df.to_sql(name='klines_tmp', con=engine, if_exists='replace')
    update_klines()
    print(f'UPLOAD: Update successful: {len(update_data_df)} existing registers')


def update_klines():
    query = """UPDATE klines as k
               SET updated_at = now(),
                    open_time = temp.open_time,
                    open_price = temp.open_price,
                    high_price = temp.high_price,
                    low_price = temp.low_price,
                    close_price = temp.close_price,
                    volume = temp.volume,
                    close_time = temp.close_time,
                    quote_asset_volume = temp.quote_asset_volume,
                    symbol = temp.symbol
                FROM klines_tmp as temp
                WHERE concat(k.open_time, '_', lower(k.symbol), '_', k.close_time) = temp.pk;
            """
    execute_query(query=query)