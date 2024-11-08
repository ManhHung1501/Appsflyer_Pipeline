import logging
from datetime import datetime, timedelta, date
from tasks.ingestion.funzy_data import incremental_ingest_funzy_data, full_ingest_funzy_data
from tasks.validation.load_config import load_config_task
from config.game import game_config
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Funzy_Ingestion_DAG',
    default_args=default_args,
    tags=["Funzy", "Ingestion", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1  
) as dag:

    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config_task,
        provide_context=True,
    )

    ingest_login_data_tasks = PythonOperator(
        task_id='ingest_login_data',
        python_callable=incremental_ingest_funzy_data,
        op_kwargs={
            'source_table': 'Vw_Account_Login',
            'filter_date_col': 'LoginDate',
            'target_db': 'da_cdp_funzy',
            'target_table': 'Vw_Account_Login',
            'engine': 'ReplacingMergeTree',
            'primary_column': 'LogID'
        },
        provide_context=True,
        execution_timeout=timedelta(hours=2)
    )

    ingest_purchase_data_tasks = PythonOperator(
        task_id='ingest_purchase_data',
        python_callable=incremental_ingest_funzy_data,
        op_kwargs={
            'source_table': 'Vw_WebSale',
            'filter_date_col': 'TopupDate',
            'target_db': 'da_cdp_funzy',
            'target_table': 'vw_websale_prod',
            'engine': 'ReplacingMergeTree',
            'primary_column': 'TransID'
        },
        provide_context=True,
        execution_timeout=timedelta(hours=2)
    )

    cms_task_groups = []
    for game in game_config:
        game_code = game['game_code']
        config = game['config']
        with TaskGroup(group_id = f"Ingest_funzy_data_{game_code}") as task_group:
            for cms_view in config['cms_views']:
                ingest_funzy_data_tasks = PythonOperator(
                    task_id=f'ingest_{cms_view}',
                    python_callable=full_ingest_funzy_data,
                    op_kwargs={
                        'source_table': cms_view,
                        'target_db': f"da_cdp_{game_code}",
                        'target_table': cms_view,
                        'engine': 'MergeTree',
                    },
                    provide_context=True,
                    execution_timeout=timedelta(hours=2)
                )
            cms_task_groups.append(task_group)
    load_config_task >> [ingest_purchase_data_tasks, ingest_login_data_tasks] + cms_task_groups

    