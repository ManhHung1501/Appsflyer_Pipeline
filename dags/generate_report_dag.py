from datetime import datetime, timedelta
from tasks.modeling.generate_report import generate_report
from utils.minio_utils import list_txt_files
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    dag_id='Generate_Report_DAG',
    default_args=default_args,
    tags=["Modeling", "Report", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=5 
) as dag:
    prefix_path ='/generate_report/query'           
    query_files = list_txt_files(prefix_path=prefix_path)

    target_db = 'da_cdp_report'
    for query_path in query_files:
        previous_task = None
        target_tbl = query_path.split('/')[-1].split('.')[0]
        test_task = PythonOperator(
            task_id=f"generate_{target_tbl}",
            python_callable=generate_report,
            op_kwargs={
                'query_path': query_path,
                'target_db': target_db,
                'target_tbl':  target_tbl
            },
            execution_timeout=timedelta(hours=3),
            trigger_rule='all_done'
        )
        if previous_task:
            previous_task >> test_task
        previous_task = test_task