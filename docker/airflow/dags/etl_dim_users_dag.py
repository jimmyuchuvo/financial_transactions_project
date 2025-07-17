#users ETL DAG
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
import ETL.transform_dim_users as transform
import ETL.load_data as load

logger = logging.getLogger(__name__)

# ETL function for task
def my_etl_task():
    logger.info('Starting ETL for USERS data')
    users_df = extract.extract_data(config.USERS_FILE, 'USERS')
    users_df_clean = transform.users_transform(users_df)
    engine = load.create_database_connection()
    load.load_dataframe_to_sql(users_df_clean, 'dim_users', engine, 'financial')
    logger.info('Finished ETL for USERS data')

# defining default arguments for the DAG
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=20),}

with DAG(
    'dim_users_etl',
    default_args=default_args,
    description='ETL process for USERS data',
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
    tags=['USERS','financial', 'etl' ],
    max_active_runs=1,
) as dag:

    etl_task = PythonOperator(
        task_id='etl_users_task',
        python_callable=my_etl_task,
        doc_md='✅Complete ETL process for users data'
)