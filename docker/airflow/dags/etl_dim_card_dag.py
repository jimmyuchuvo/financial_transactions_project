#cards ETL DAG
import sys
sys.path.append('/opt/airflow')

import logging
from datetime import timedelta
from airflow.decorators import (dag, task)
from airflow.utils.dates import days_ago

import ETL.config as config
import ETL.extract_data as extract
import ETL.transform_dim_card as transform
import ETL.load_data as load

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'jimmy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['financial', 'etl', 'CARDS'],
    description='ETL process for CARDS data',
    max_active_runs=1
)
def dim_card_etl():

    @task(
        execution_timeout=timedelta(hours=0.5),
        doc_md='💳This DAG loads card dimension data.'
    )
    def etl_cards_task():
        logger.info("Starting ETL for cards")

        # EXTRACT
        cards_df = extract.extract_data(config.CARDS_FILE, 'CARDS')
        logger.info(f"Extracted {len(cards_df)} rows")

        # TRANSFORM
        cards_df_clean = transform.cards_transform(cards_df)
        logger.info(f"Transformed {len(cards_df_clean)} rows")

        # LOAD
        engine = load.create_database_connection()
        load.load_dataframe_to_sql(
            cards_df_clean,
            'dim_card',
            engine,
            'financial'
        )

        logger.info("Finished ETL for cards")

    etl_cards_task()


dim_card_etl()




