from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.validation.load_config import load_config_task
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Game_Master_DAG',
    default_args=default_args,
    tags=["Game", "Master_DAG"],
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1  
) as dag:
    # Define tasks to trigger the three DAGs
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config_task,
        provide_context=True,
    )
    with TaskGroup(group_id = f"Ingest_Data_Group") as ingest_task_group:
        trigger_ingest_appsflyer = TriggerDagRunOperator(
            task_id='ingest_appsflyer_data',
            trigger_dag_id='Appsflyer_Ingestion_DAG',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

        trigger_ingest_funzy = TriggerDagRunOperator(
            task_id='ingest_funzy_data',
            trigger_dag_id='Funzy_Ingestion_DAG',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

        trigger_ingest_locker_app = TriggerDagRunOperator(
            task_id='ingest_locker_data_app',
            trigger_dag_id='Data_Locker_App_Ingestion',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=False,
        )

        trigger_ingest_locker_web = TriggerDagRunOperator(
            task_id='ingest_locker_data_web',
            trigger_dag_id='Data_Locker_Web_Ingestion',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

    with TaskGroup(group_id = f"Transform_Data_Group") as transform_task_group:
        trigger_transformation_locker_web = TriggerDagRunOperator(
            task_id='transform_locker_data_web',
            trigger_dag_id='Data_Locker_Web_Transformation',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

        trigger_transformation_appsflyer = TriggerDagRunOperator(
            task_id='transform_appsflyer',
            trigger_dag_id='Appsflyer_Transformation_DAG',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

    with TaskGroup(group_id = f"Modeling_Data_Group") as modeling_task_group:
        trigger_modeling= TriggerDagRunOperator(
            task_id='load_data_to_modeling_table',
            trigger_dag_id='Game_Modeling_DAG',
            conf={
                'start_date': "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
            },
            wait_for_completion=True,
        )

    # Trigger all DAGs in parallel
    load_config_task >> [trigger_ingest_appsflyer, trigger_ingest_funzy, trigger_ingest_locker_app, trigger_ingest_locker_web]
    trigger_ingest_locker_web >> trigger_transformation_locker_web
    trigger_ingest_appsflyer >> trigger_transformation_appsflyer
    trigger_transformation_appsflyer >> trigger_modeling
    trigger_ingest_funzy >> trigger_modeling