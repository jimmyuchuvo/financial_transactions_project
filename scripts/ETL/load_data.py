import pandas as pd
import logging
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine
import ETL.config as config
import ETL.utils as utils

logger = logging.getLogger(__name__)

# Create database SQL Server connection
def create_database_connection() -> Engine:
    """
    Create and return a SQLAlchemy Engine for a SQL Server database connection.
    
    Returns:
        Engine: SQLAlchemy Engine connected to the SQL Server database
    """
    try:
        database_connection = URL.create(
            "mssql+pyodbc",
            username=config.DB_USER,
            password=config.DB_PASS,
            host=config.DOCKER_DB_SERVER,
            port=1433,  
            database=config.DB_NAME,
            query={
                "driver": "ODBC Driver 18 for SQL Server",
                "TrustServerCertificate": "yes",
                #"authentication": "ActiveDirectoryIntegrated",
            },
        )
        engine = create_engine(database_connection)
        logger.info('✅Successfully created database connection engine.')
        return engine

    except Exception as e:
        logger.error(f'❌Failed to create database connection: {e}')
        raise 

# Load the data into database sql server

def load_dataframe_to_sql(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    schema: str,
    if_exists: str = "append",
    index: bool = False
) -> None:
    """
    Load a Pandas DataFrame into a SQL Server table.

    Args:
        df (pd.DataFrame): The DataFrame to load.
        table_name (str): Name of the target SQL Server table.
        engine (Engine): SQLAlchemy Engine object.
        schema (str, optional): Target schema (e.g., 'dbo', 'financial').
        if_exists (str, optional): Behavior if table exists ('fail', 'replace', 'append').
        index (bool, optional): Whether to write DataFrame index as a column. Default is False.

    Raises:
        ValueError: If the DataFrame is empty.
        Exception: For other database-related errors.
    """
    
    if df.empty:
        logger.warning(f"DataFrame is empty. No data loaded into table '{table_name}'.")
        raise ValueError("DataFrame is empty. Aborting load.")

    try:
        with engine.begin() as conn:
            result = conn.execute(f"SELECT TOP 1 1 FROM {schema}.{table_name}")
            row_exists = result.fetchone() is not None

            if row_exists:
                logger.info(f"Table '{schema}.{table_name}' has data.")
                conn.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                
        # Load the DataFrame into the SQL Server table
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,  
            index=index,
            chunksize=1000  # Adjust chunk size as needed
        )    
        logger.info(f"✅Successfully loaded data into table '{schema + '.' if schema else ''}{table_name}'. {len(df)} rows and {df.shape[1]} columns")
    except Exception as e:
        logger.error(f"❌Error loading data into table '{table_name}': {e}")
        raise

