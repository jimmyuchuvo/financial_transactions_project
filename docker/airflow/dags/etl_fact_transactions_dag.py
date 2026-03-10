#transactions ETL DAG
import sys
sys.path.append('/opt/airflow')  # ⚠️ Only needed if your modules aren't already in the path

import logging
from datetime import timedelta
from airflow.decorators import (dag, task)
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

import ETL.config as config
import ETL.extract_data as extract
import ETL.transform_fact_transactions as transform
import ETL.load_data as load


logger = logging.getLogger(__name__)

#define default args
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=60)
}

@dag(
    schedule_interval='@daily',
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    tags=['TRANSACTIONS', 'financial', 'etl'],
    max_active_runs=1
)
def fact_transactions_etl():

    @task(
        execution_timeout=timedelta(minutes=30),
        doc_md='🏦 This DAG loads transactions fact data.'
    )
    def etl_transactions_task():
        logger.info('Starting ETL for TRANSACTIONS data')

        transactions_df = extract.extract_data(config.FILTERED_TRANSACTIONS_FILE, 'TRANSACTIONS')
        transactions_df = transform.transform_fact_transactions(transactions_df, 'TRANSACTIONS')
        engine = load.create_database_connection()
        load.load_dataframe_to_sql(transactions_df, 'fact_transactions', engine, 'financial')

        logger.info('Finished ETL for TRANSACTIONS data')

    etl_transactions_task()

# Call the DAG to register it   
fact_transactions_etl()


    