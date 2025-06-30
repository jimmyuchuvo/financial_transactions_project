#cards ETL DAG
import sys
sys.path.append('/opt/airflow')


import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import ETL.extract_data as extract
import ETL.transform_cards as transform
import ETL.load_data as load
import ETL.ETL_dim_card as etl_dim_card

logger = logging.getLogger(__name__)

# ETL function for task
def my_etl_task():
    logger.info("Starting ETL for cards")
    cards_df = etl_dim_card.extract_cards_data()
    cards_df_clean = etl_dim_card.cards_transform(cards_df)
    engine = load.create_database_connection()
    load.load_dataframe_to_sql(cards_df_clean,'dim_card',engine,'financial')
    logger.info("Finished ETL for cards")

# defining default arguments for the DAG
default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
}

with DAG(
    'cards_etl',
    default_args=default_args,
    description='ETL process for cards data',
    schedule_interval= '@daily',  
    start_date=days_ago(1),
    catchup=False,
    tags=['financial', 'etl', 'cards'],
    max_active_runs=1,
) as dag:

    etl_task = PythonOperator(
        task_id='etl_cards_task',
        python_callable=my_etl_task,
        doc_md='Complete ETL process for cards data'
    )




