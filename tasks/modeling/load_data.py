import logging
from datetime import datetime, timedelta
from utils.clickhouse_utils import connect_clickhouse
from utils.common_utils import project_dir
from constants.modeling import table


def increamental_load_modeling_table(target_db:str, target_table:str, game_code:str, config: dict, **context):
        date = context["ti"].xcom_pull(task_ids="load_config", key="start_date")

        clickhouse_client =connect_clickhouse()

        # Create table and database if not exist
        clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        with open(f'{project_dir}/resources/table_modeling/{target_table}.sql') as f:
            clickhouse_client.execute(f.read().format(target_db=target_db))

        # Write DataFrame to ClickHouse
        query_template_file= target_table
        if config['is_web'] ==1 and  target_table in ['fact_device', 'dim_user']:
            query_template_file += '_web'

        with open(f'{project_dir}/resources/query_template/{query_template_file}.sql') as f:
            filter_date_col = table[target_table]['filter_date_col']
            order_col = table[target_table]['order_col']

            query = f.read().format(game_code=game_code, order_col=order_col)
            if date == 'all':
                filter_query = query  
            else:
                seven_days_ago = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
                filter_query = query + f" WHERE {filter_date_col} >= '{seven_days_ago}' AND {filter_date_col} <= '{date}' "
            logging.info(f"FILTER QUERY: {filter_query}")

            view_name = f'da_cdp_{game_code}.{target_table}_update_v'
            clickhouse_client.execute(f"DROP VIEW IF EXISTS {view_name};")
            clickhouse_client.execute(f"CREATE VIEW {view_name} AS {filter_query}")
            logging.info('Create update view success!')

            target_tbl_name = f'{target_db}.{target_table}' 
            if date == 'all':
                clickhouse_client.execute(f"TRUNCATE TABLE {target_tbl_name}")

            clickhouse_client.execute(f"INSERT INTO {target_tbl_name} ({order_col}) SELECT {order_col} FROM {view_name}")
            logging.info(f'Update data to modeling table {target_tbl_name} success!')

            # Deduplicate after ingest data from funzy
            primary_column = table[target_table]['primary_key']
            clickhouse_client.execute(f"OPTIMIZE TABLE {target_tbl_name} DEDUPLICATE BY {primary_column}")
            logging.info(f'Deduplication success!')


    