import logging
from utils.minio_utils import read_text_file_minio
from utils.clickhouse_utils import connect_clickhouse, generate_create_table_query

def generate_report(query_path: str,  target_db:str, target_tbl: str):
    clickhouse_client = connect_clickhouse()
    logging.info('Connect to Clickhouse Success')

    # Read data from query
    query = read_text_file_minio(query_path)
    df = clickhouse_client.query_dataframe(query=query)
    logging.info(f'Read data from query to DataFrame Success')
    
    # Create Database
    clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")

    # Create Table 
    clickhouse_client.execute(f"DROP TABLE IF EXISTS {target_db}.{target_tbl}")
    logging.info(f'Drop Table Success')

    create_tbl_query = generate_create_table_query(df=df, target_db=target_db, target_table=target_tbl)
    clickhouse_client.execute(create_tbl_query)
    logging.info(f'Create Table Success')

    # Execute the query
    clickhouse_client.insert_dataframe(f"INSERT INTO {target_db}.{target_tbl} VALUES", df)
    logging.info(f'Load Data Success')