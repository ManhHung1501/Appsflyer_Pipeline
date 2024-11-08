from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.ingestion.appsflyer_api_data import ingest_appsflyer_raw_data_api, ingest_appsflyer_agg_data_api
from tasks.ingestion.crawl_appsflyer_cohort import crawl_cohort_data
from constants.appsflyer import Report
from tasks.validation.load_config import load_config_task
from config.game import game_config



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
    dag_id='Appsflyer_Ingestion_DAG',
    default_args=default_args,
    tags=["Appsflyer", "Ingestion", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=10,
    max_active_runs=1  
) as dag:
    
    load_config = PythonOperator(task_id='load_config',
                                 provide_context=True,
                                 python_callable=load_config_task)

    # Group data API
    game_task_groups = []
    for game in game_config:
        game_code = game['game_code']
        config = game['config']
        with TaskGroup(group_id = f"Ingest_Appsflyer_data_{game_code}") as task_group:
            with TaskGroup(group_id=f"group_data_Appsflyer_API_android") as task_group_API_android:
                for type_report, field in Report.reports.items():
                    # previous_task = None
                    pull_data_android = PythonOperator(task_id=f"pull_data_android_{type_report}",
                                                        python_callable=ingest_appsflyer_raw_data_api,
                                                        op_kwargs={
                                                            'platform': 'android',
                                                            'type_report': type_report,
                                                            'additional_field': field,
                                                            'game_code': game_code,
                                                            'config': config
                                                        })
                    # if previous_task:
                    #     previous_task >> pull_data_android
                    # previous_task = pull_data_android
                pull_agg_data = PythonOperator(task_id=f"pull_agg_data_android_partners_by_date_report",
                                                    python_callable=ingest_appsflyer_agg_data_api,
                                                    op_kwargs={
                                                        'platform': 'android',
                                                        'type_report': 'partners_by_date_report',
                                                        'game_code': game_code,
                                                        'config': config
                                                    })
                    
            with TaskGroup(group_id=f"group_data_Appsflyer_API_ios") as task_group_API_ios:
                for type_report, field in Report.reports.items():
                    pull_data_ios = PythonOperator(task_id=f"pull_data_ios_{type_report}",
                                                    python_callable=ingest_appsflyer_raw_data_api,
                                                    op_kwargs={
                                                        'platform': 'ios',
                                                        'type_report': type_report,
                                                        'additional_field': field,
                                                        'game_code': game_code,
                                                        'config': config
                                                    })
                pull_agg_data = PythonOperator(task_id=f"pull_agg_data_android_partners_by_date_report",
                                                    python_callable=ingest_appsflyer_agg_data_api,
                                                    op_kwargs={
                                                        'platform': 'ios',
                                                        'type_report': 'partners_by_date_report',
                                                        'game_code': game_code,
                                                        'config': config
                                                    })
                
            crawl_cohort_data_task = PythonOperator(task_id=f"crawl_cohort_data",
                                                    python_callable=crawl_cohort_data,
                                                    op_kwargs={
                                                        'game_config': config,
                                                        'target_db': f'da_cdp_{game_code}', 
                                                        'target_table': 'COHORT_MKT', 
                                                        'engine': 'MergeTree',
                                                        'primary_column': 'cohort_day,media_source,campaign,adset_name'
                                                    })
            game_task_groups.append(task_group)

    load_config >> game_task_groups
