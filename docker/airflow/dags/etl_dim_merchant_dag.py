#merchant ETL DAG
import sys
sys.path.append('/opt/airflow')

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import ETL.config as config
import ETL.extract_data as extract
import ETL.transform_dim_merchant as transform
import ETL.load_data as load

logger = logging.getLogger(__name__)

    
#etl function for task
def my_etl_task():
    
    logger.info('Starting ETL for MERCHANT data')
    columns_to_extract = ['merchant_id', 'merchant_city', 'merchant_state','zip', 'mcc']
    merchant_df = extract.extract_data(config.FILTERED_TRANSACTIONS_FILE,'MERCHANT',columns_to_extract)
    merchant_df_clean = transform.transform_dim_merchant(merchant_df,'MERCHANT')
    engine = load.create_database_connection()
    load.load_dataframe_to_sql(merchant_df_clean, 'dim_merchant', engine, 'financial')
    logger.info('Finished ETL for MERCHANT data')

# defining default arguments for the DAG
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=20),}

with DAG(
    'dim_merchant_etl',
    default_args=default_args,
    description='ETL process for MERCHANT data',
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
    tags=['MERCHANT','financial', 'etl' ],
    max_active_runs=1,
) as dag:

    etl_task = PythonOperator(
        task_id='etl_merchant_task',
        python_callable=my_etl_task,
        doc_md='✅Complete ETL process for merchant data'
    )
