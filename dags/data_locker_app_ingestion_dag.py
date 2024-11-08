import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from tasks.ingestion.data_locker import S3BucketSensor
from constants.data_locker import DATA_LOCKER_EVENT_APP 
from tasks.validation.load_config import load_config_task
from config import data_locker  
from config.game import game_config
from utils.common_utils import project_dir


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Data_Locker_App_Ingestion',
    default_args=default_args,
    tags=["Data_Locker","App", "Ingestion","Game"],
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
        config_dump = json.dumps(config)
        with TaskGroup(group_id = f"Ingest_data_locker_app_{game_code}") as task_group:
            for event_report in DATA_LOCKER_EVENT_APP:
                with TaskGroup(group_id=f"ingest_event_{event_report}"):
                    check_exist_data_locker_task = S3BucketSensor(task_id=f"check_exist_{event_report}",
                                        endpoint_url=data_locker.data_locker_endpoint,
                                        access_key=data_locker.data_locker_access_key,
                                        secret_key=data_locker.data_locker_secret_key,
                                        bucket_name=data_locker.data_locker_bucket_name,
                                        event_report=event_report,
                                        config=config,
                                        poke_interval=30 * 60,
                                        mode='reschedule',
                                        timeout=24 * 60 * 60,
                                        soft_fail=False)

                    ingest_data_locker_task = SparkSubmitOperator(task_id=f"ingest_data_locker_{event_report}",
                                                    conn_id='spark',
                                                    application=f"{project_dir}/tasks/ingestion/data_locker.py",
                                                    application_args=[
                                                        config_dump,
                                                        event_report,
                                                        "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
                                                    ],
                                                    conf={
                                                        "spark.local.dir": "/tmp/spark-temp",
                                                        "spark.cores.max": "4",
                                                        "spark.executor.instances": "2",
                                                        "spark.executor.cores": "2",
                                                        "spark.executor.memory": "2g",
                                                        "spark.driver.memory": "2g",
                                                    },
                                                    verbose=False,
                                                    execution_timeout=timedelta(hours=2)
                                                  )
                    
                    check_exist_data_locker_task >> ingest_data_locker_task


        game_task_groups.append(task_group)
    load_config >> game_task_groups