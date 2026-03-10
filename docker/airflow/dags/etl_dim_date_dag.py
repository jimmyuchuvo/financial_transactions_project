#Date ETL DAG
import sys
sys.path.append('/opt/airflow')

import logging
from datetime import timedelta
from airflow.decorators import (dag, task)
from airflow.utils.dates import days_ago 
 
import ETL.config as config 
import ETL.extract_data as extract 
import ETL.transform_dim_date as transform 
import ETL.load_data as load 

logger = logging.getLogger(__name__)

# defining default arguments for the DAG
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['financial', 'etl', 'DATE'],
    description='ETL process for DATE data',
    max_active_runs=1
)

def dim_date_etl():

    @task(
        execution_timeout=timedelta(hours=0.5),
        doc_md='📆 This DAG loads card dimension data.'
    )

# ETL function for task
    def etl_date_task():
        logger.info("Starting ETL for dim_date")

        # EXTRACT
        dim_date = extract.extract_data(config.FILTERED_TRANSACTIONS_FILE, 'dim_date',['date'])

        # TRANSFORM
        dim_date_cleaned = transform.create_dim_date(dim_date,'dim_date')

        # LOAD
        engine = load.create_database_connection()
        load.load_dataframe_to_sql(dim_date_cleaned,'dim_date',engine,'financial')

        logger.info("Finished ETL for dim_date")
    
    etl_date_task()

dim_date_etl()








