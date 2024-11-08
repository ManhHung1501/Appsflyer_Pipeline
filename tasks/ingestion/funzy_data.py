from utils.funzy_utils import connect_funzy_db
from utils.clickhouse_utils import connect_clickhouse, generate_create_table_query
import pandas as pd
import time
import logging
 


def incremental_ingest_funzy_data(
        source_table: str, 
        filter_date_col: str, 
        target_db: str, 
        target_table: str, 
        engine: str, 
        primary_column: str,
        **kwargs):
    logging.info(f'Starting to ingest table {source_table} from Funzy')

    ti = kwargs['ti']
    date = ti.xcom_pull(task_ids='load_config', key='start_date')

    # READ DATA FROM FUNZY DATABASE SQL SERVER
    start_read = time.time()
    # Get source table
    if source_table == 'Vw_Account_Login':
        excution_year = date.split('-')[0]
        source_table = f'Vw_Account_Login_{excution_year}'
    
    # Init connection to sql server
    funzy_db_con = connect_funzy_db()
    query = f"SELECT * FROM {source_table} WHERE {filter_date_col} = '{date}'"
    df = pd.read_sql(query, funzy_db_con)
    df[filter_date_col] = pd.to_datetime(df[filter_date_col])
    funzy_db_con.close()

    logging.info(f'Complete Read data from FUNZY database in {time.time() - start_read}')

    # LOAD DATA TO CLICKHOUSE
    start_load = time.time()
    # ClickHouse connection
    clickhouse_client = connect_clickhouse()
   
    # Create  database and table if not exist
    clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
    create_tbl_query = generate_create_table_query(df, target_db, target_table, engine, primary_column)
    clickhouse_client.execute(create_tbl_query)
 
    # Write DataFrame to ClickHouse
    clickhouse_client.insert_dataframe(f"INSERT INTO {target_db}.{target_table} VALUES", df)

    # Deduplicate after ingest data from funzy
    clickhouse_client.execute(f"OPTIMIZE TABLE {target_db}.{target_table} DEDUPLICATE BY {primary_column}")
    
    logging.info(f'Complete Load data to Clickhouse in {time.time() - start_load}')
    logging.info("Data successfully loaded into ClickHouse.")

def full_ingest_funzy_data(
        source_table: str,  
        target_db: str, 
        target_table: str, 
        engine: str):
    
    # READ DATA FROM FUNZY DATABASE SQL SERVER
    start_read = time.time()
    
    # Init connection to sql server
    funzy_db_con = connect_funzy_db()
    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, funzy_db_con)
    funzy_db_con.close()
    logging.info(f'Complete Read data from FUNZY database in {time.time() - start_read}')

    # LOAD DATA TO CLICKHOUSE
    start_load = time.time()
    # ClickHouse connection
    clickhouse_client = connect_clickhouse()
   
    # Create  database and table if not exist
    clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
    create_tbl_query = generate_create_table_query(df, target_db, target_table, engine)
    clickhouse_client.execute(create_tbl_query)
 
    # Write DataFrame to ClickHouse
    clickhouse_client.execute(f"TRUNCATE TABLE {target_db}.{target_table}")
    clickhouse_client.insert_dataframe(f"INSERT INTO {target_db}.{target_table} VALUES", df)

    logging.info(f'Complete Load data to Clickhouse in {time.time() - start_load}')
    logging.info("Data successfully loaded into ClickHouse.")