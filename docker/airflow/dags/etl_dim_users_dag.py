#users ETL DAG
import sys
sys.path.append('/opt/airflow')

import logging
from datetime import timedelta
from airflow.decorators import (dag, task)
from airflow.utils.dates import days_ago

import ETL.config as config
import ETL.extract_data as extract
import ETL.transform_dim_users as transform
import ETL.load_data as load

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20)
}

@dag(
     schedule_interval= '@daily',
     start_date= days_ago(1),
     catchup= False,
     default_args= default_args,
     tags= ['USERS', 'financial', 'etl'],
     max_active_runs= 1   
)

def dim_users_etl():
    @task(
        execution_timeout=timedelta(hours=0.5),
        doc_md='👤 This DAG loads user dimension data.'
    )

    # ETL function for task
    def etl_users_task():

        logger.info('Starting ETL for USERS data')

        #EXTRACT
        users_df = extract.extract_data(config.USERS_FILE, 'USERS')
        #TRANSFORM
        users_df_clean = transform.users_transform(users_df)
        #LOAD
        engine = load.create_database_connection()
        load.load_dataframe_to_sql(users_df_clean, 'dim_users', engine, 'financial')

        logger.info('Finished ETL for USERS data')

    etl_users_task()

dim_users_etl()    
    