import pandas as pd
import logging
import ETL.config as config
import ETL.utils as utils
import ETL.load_data as load
import ETL.extract_data as extract


logger = logging.getLogger(__name__)

def transform_fact_transactions(df: pd.DataFrame, data_name: str)->pd.DataFrame: 
    '''
    Function to transform TRANSACTIONS fact data

    Parameters:
        df (pd.DataFrame): The DataFrame to transform.
        data_name (str): data name of the dim
    Returns:
        pd.DataFrame: The transformed DataFrame.
    
    '''
    columns_to_rename = {'id': 'transaction_id', 'use_chip': 'card_entry_method', 'errors':'transaction_error'
                         , 'date':'transaction_date', 'client_id':'user_id', 'zip':'merchant_zip'}
    #query to merge data and create merchant_key
    query = 'SELECT merchant_key, merchant_id, merchant_city, merchant_state, merchant_zip, mcc FROM financial.dim_merchant'
    merge_on = ['merchant_id', 'merchant_city', 'merchant_state', 'merchant_zip','mcc']
    columns_to_remove = ['merchant_id', 'merchant_city', 'merchant_state','merchant_zip', 'mcc','minute_block']

    #variables use them to create ['minute_block'] column
    bins   = [-1, 15, 30, 45, 59] 
    labels = ['0015', '1630', '3145', '4659']

    try:
        logger.info(f'Starting transform {data_name} fact data')

        #df = df.copy()
        df_cleaned = df.rename(columns=columns_to_rename)
        df_cleaned['amount'] = df_cleaned['amount'].fillna('0').str.replace(r'[/$]', '', regex=True).astype(float)
        df_cleaned.loc[(df_cleaned['merchant_state'].isna()) & (df_cleaned['merchant_city'] == 'ONLINE'), ['merchant_state']] = 'ONLINE'
        df_cleaned.loc[(df_cleaned['merchant_state'] == 'ONLINE') & (df_cleaned['merchant_city'] == 'ONLINE'), 'merchant_zip'] = 0
        df_cleaned['merchant_zip'] = df_cleaned['merchant_zip'].fillna(0).astype(int)
        
        #create the date_key
        df_cleaned['transaction_date'] = pd.to_datetime(df_cleaned['transaction_date'],cache=True)
        df_cleaned['minute_block'] = pd.cut(pd.to_datetime(df_cleaned['transaction_date']).dt.minute, bins=bins, labels=labels).astype(str)
        
        dt = df_cleaned['transaction_date'].dt
        df_cleaned['date_key'] = (
            dt.year.astype(str) +
            dt.month.astype(str).str.zfill(2) +
            dt.day.astype(str).str.zfill(2) +
            dt.hour.astype(str).str.zfill(2) +
            df_cleaned['minute_block']
        ).astype(int)
        
        # merge the mcc data from dim_merchant in dwh
        engine = load.create_database_connection()
        mcc = pd.read_sql(query,engine)
        df_cleaned = df_cleaned.merge(mcc, left_on= merge_on, right_on= merge_on, how='left',sort=False)

        df_cleaned = df_cleaned.drop(columns= columns_to_remove)

        logger.info(f'✅ Successfully transformed {data_name} data with {len(df_cleaned)} records')
        return df_cleaned
        

    except Exception as e:
        logger.info(f'❌Error transforming {data_name} data: {e}')
        raise