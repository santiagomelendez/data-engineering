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
    print(f'VALIDATIONS: There are {(input_df["should_be_updated"] == True).sum()} duplicated registers')
    input_df.to_csv(output_file, index=False)


def filter_by_threshold(input_file, threshold):
    '''
    This function receive a file with tickers and check if threshold is between high price and low price
    per each time.
    It returns a list of times where the condition is matched
    '''
    input_df = pd.read_csv(input_file)
    output = input_df.loc[(input_df['low_price'].astype(float) <= threshold) & 
                     (threshold <= input_df['high_price'].astype(float))]
    if not output.empty:
        verb = 'is' if len(output) == 1 else 'are'
        print(f'VALIDATIONS_THRESHOLD: There {verb} {len(output)} candlestick whose price reached {threshold} ')
    return output

