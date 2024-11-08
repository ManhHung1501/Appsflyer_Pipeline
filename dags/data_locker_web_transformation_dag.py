import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from tasks.validation.load_config import load_config_task
from utils.common_utils import project_dir
from config.game import game_config




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
    dag_id='Data_Locker_Web_Transformation',
    default_args=default_args,
    tags=["Data_Locker", "Web", "Transformation", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=10,
    max_active_runs=1  
) as dag:
    # SPARK CONF
    spark_conf = {
        "spark.local.dir": "/tmp/spark-temp",
        "spark.cores.max": "4",
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        "spark.sql.shuffle.partitions": "10",
    }
    #SPARK JARS
    # jar_packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.757,io.delta:delta-core_2.12:2.1.1"
    jars_path='hdfs:////spark/jars/delta-core_2.12-2.1.1.jar'

    load_config = PythonOperator(task_id='load_config',
                                 provide_context=True,
                                 python_callable=load_config_task)
    
    # Group data API
    data_locker_tasks = f"{project_dir}/tasks/transformations/data_locker"
    python_files = [f for f in os.listdir(data_locker_tasks) if f.endswith(".py") and f != "__init__.py"]
    game_task_groups = []

    # DAG
    for game in game_config:
        game_code = game['game_code']
        config = game['config']
        config_dumps = json.dumps(config)
        if config['is_web'] == 1:
            with TaskGroup(group_id = f"Transform_Data_Locker_{game_code}") as task_group:
                # previous_task = None
                for python_file in python_files:
                    task_id = f"spark_submit_{os.path.splitext(python_file)[0]}"
                    spark_submit_task = SparkSubmitOperator(task_id=task_id,
                                                        conn_id='spark',
                                                        application=f"{data_locker_tasks}/{python_file}",
                                                        application_args=[
                                                            config_dumps,
                                                            "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
                                                            game_code
                                                        ],
                                                        conf = spark_conf,
                                                        jars=jars_path if python_file == "web_registration.py" else None,
                                                        verbose=False,
                                                        execution_timeout=timedelta(hours=2),
                                                    )
                    # if previous_task:
                    #     previous_task >> spark_submit_task
                    # previous_task = spark_submit_task
                
            game_task_groups.append(task_group)

    load_config >> game_task_groups
