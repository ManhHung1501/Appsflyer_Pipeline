from datetime import datetime, timedelta
import logging
import boto3
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.base import BaseSensorOperator
from tasks.ingestion.data_locker import ingest_data_locker
from constants.data_locker import DATA_LOCKER_EVENT_APP, DATA_LOCKER_EVENT_WEB 
from tasks.validation.load_config import load_config_task
from config import data_locker, minio  
from config.game import game_config
from utils.common_utils import project_dir
from tasks.transformations.appsflyer.af_install import trans_af_install
from tasks.modeling.generate_report import generate_report
from utils.minio_utils import list_txt_files
from tasks.ingestion.appsflyer_api_data import ingest_appsflyer_raw_data_api, ingest_appsflyer_agg_data_api

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2024, 10, 1),
    'email_on_retry': False,
    'retries': 0,
}


# Define DAG
with DAG(
    dag_id='Test_Dag',
    default_args=default_args,
    tags=["Game", "Test"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=5
) as dag:
    
    load_config = PythonOperator(task_id='load_config',
                                 provide_context=True,
                                 python_callable=load_config_task)
    event_report = 'skad_installs'
    game_code='3qtrieuhoansu'
    config = {
      "data_locker_bucket_s3": "ftech-data-appsflyer-aws-bucket-3t",
      "data_locker_bucket_minio": "s3a://da-bucket/data_locker/conn=ftech-data-appsflyer-aws-bucket-3t",
      "android_app_id": "vn.funzy.trieuhoansu3q",
      "ios_app_id": "id6504213798",
      "cms_views": [
        "Vw_Game3QTrieuHoanSu_User",
        "Vw_Game3QTrieuHoanSu_Server",
        "Vw_Game3QTrieuHoanSu_Item",
        "Vw_Game3QTrieuHoanSu_CCU"
      ],
      "is_web": 0,
      "is_active": 1
    }
    config_dumps = json.dumps(config)
    jar_packages = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.12.757,io.delta:delta-core_2.12:2.1.1"
    jars_path='hdfs:////spark/jars/delta-core_2.12-2.1.1.jar'

    spark_conf = {
        # "spark.submit.deployMode": "cluster",
        # "spark.jars": ",".join(jar_list),
        "spark.local.dir": "/tmp/spark-temp",
        "spark.cores.max": "4",
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "2g",
        # "spark.driver.maxResultSize": "2g",
        "spark.sql.shuffle.partitions": "10",
        # .config("spark.dynamicAllocation.enabled", "true")
        # .config("spark.dynamicAllocation.minExecutors", "1") 
        # .config("spark.dynamicAllocation.maxExecutors", "8")  
        # .config("spark.dynamicAllocation.initialExecutors", "2")  
        # .config("spark.dynamicAllocation.schedulerBackend", "org.apache.spark.scheduler.cluster.YarnScheduler")
        # .config("spark.shuffle.service.enabled", "true")
    }
    appsflyer_tasks = f"{project_dir}/tasks/transformations/appsflyer"
    
  
    # test_task1 = PythonOperator(task_id=f"pull_agg_data_android_partners_by_date_report",
    #                                                 python_callable=ingest_appsflyer_agg_data_api,
    #                                                 op_kwargs={
    #                                                     'platform': 'ios',
    #                                                     'type_report': 'partners_by_date_report',
    #                                                     'game_code': game_code,
    #                                                     'config': config
    #                                                 })
    
    test_task2 = SparkSubmitOperator(task_id='generate_user_registration_task',
                                                        conn_id='spark',
                                                        application=f"{appsflyer_tasks}/af_registration.py",
                                                        application_args=[
                                                            config_dumps,
                                                            "{{ ti.xcom_pull(task_ids='load_config', key='start_date') }}",
                                                            game_code
                                                        ],
                                                        conf=spark_conf,
                                                        jars=jars_path,
                                                        verbose=False,
                                                        execution_timeout=timedelta(hours=2),
                                                    )
 
    load_config  >> test_task2
    
    
