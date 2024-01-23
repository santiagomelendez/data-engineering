import pandas as pd
from utils.persistence import engine

def validate_duplicates(input_file, output_file):
    '''
    This function receive a file with tickers and check if the
    pk (open_time_symbol_close_time) already exists in db. 
    If exists add the field "should_be_update" to the output_file
    '''
    input_df = pd.read_csv(input_file)
    input_df['pk'] = input_df['open_time'] + '_' + input_df['symbol'].str.lower() + '_' + input_df['close_time']
    with engine.begin() as conn:
        db_pks_df= pd.read_sql_query("""
                                    select concat(open_time, '_', symbol, '_', close_time) as pk 
                                    from klines
                                    """, conn)
    input_df['should_be_updated'] = input_df['pk'].isin(db_pks_df['pk'])
    print(f'VALIDATIONS: There are {(input_df["should_be_updated"] == True).count()} duplicated registers')
    input_df.to_csv(output_file, index=False)

