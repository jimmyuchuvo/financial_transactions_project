#merchant ETL DAG
import sys
sys.path.append('/opt/airflow')

import logging
from datetime import timedelta
from airflow.decorators import (dag, task)
from airflow.utils.dates import days_ago

import ETL.config as config
import ETL.extract_data as extract
import ETL.transform_dim_merchant as transform
import ETL.load_data as load

logger = logging.getLogger(__name__)

# defining default arguments for the DAG
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),}

@dag(
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['financial', 'etl', 'MERCHANT'],
    description='ETL process for MERCHANT data',
    max_active_runs=1
) 

def dim_merchant_etl():
    
    @task(
        execution_timeout=timedelta(hours=0.5),
        doc_md='🛒 This DAG loads merchant dimension data.'
    )
    
    def etl_merchant_task():
        logger.info('Starting ETL for MERCHANT data')
        
        #EXTRACT
        columns_to_extract = ['merchant_id', 'merchant_city', 'merchant_state', 'zip', 'mcc']
        merchant_df = extract.extract_data(config.FILTERED_TRANSACTIONS_FILE, 'MERCHANT', columns_to_extract)
        
        #TRANSFORM
        merchant_df_clean = transform.transform_dim_merchant(merchant_df, 'MERCHANT')
        
        #LOAD
        engine = load.create_database_connection()
        load.load_dataframe_to_sql(
            merchant_df_clean, 
            'dim_merchant',
            engine, 
            'financial'
            )
        logger.info('Finished ETL for MERCHANT data')

    etl_merchant_task()

dim_merchant_etl()