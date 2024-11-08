from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from tasks.modeling.load_data import increamental_load_modeling_table
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
    dag_id='Game_Modeling_DAG',
    default_args=default_args,
    tags=["Modeling", "Trasnformation", "Game"],
    schedule_interval=None,
    catchup=False,
    max_active_tasks=10,
    max_active_runs=1  
) as dag:
    
    load_config = PythonOperator(task_id='load_config',
                                 provide_context=True,
                                 python_callable=load_config_task)

    # Group data API
    target_db = 'da_cdp_general'
    game_task_groups = []
    for game in game_config:
        game_code = game['game_code']
        config = game['config']
        with TaskGroup(group_id = f"Modeling_data_{game_code}") as task_group:
            load_dim_user_task = PythonOperator(task_id=f"load_dim_user",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'dim_user',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            load_fact_login_task = PythonOperator(task_id=f"load_fact_login",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'fact_login',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            load_fact_transaction_task = PythonOperator(task_id=f"load_fact_transaction",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'fact_transaction',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            load_install_task = PythonOperator(task_id=f"load_install",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'install',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            # load_fact_vip_level_task = PythonOperator(task_id=f"load_fact_vip_level",
            #                                     python_callable=increamental_load_modeling_table,
            #                                     op_kwargs={
            #                                         'target_db': target_db,
            #                                         'target_table': 'fact_vip_level',
            #                                         'game_code': game_code,
            #                                         'config': config
            #                                     })
            load_COHORT_MKT_task = PythonOperator(task_id=f"load_COHORT_MKT",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'COHORT_MKT',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            load_fact_device_task = PythonOperator(task_id=f"load_fact_device",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'fact_device',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
            load_partners_by_date_report_task = PythonOperator(task_id=f"load_partners_by_date_report",
                                                python_callable=increamental_load_modeling_table,
                                                op_kwargs={
                                                    'target_db': target_db,
                                                    'target_table': 'partners_by_date_report',
                                                    'game_code': game_code,
                                                    'config': config
                                                })
              
            load_config >> [load_fact_login_task, load_fact_transaction_task, load_install_task, load_COHORT_MKT_task, load_fact_device_task, load_partners_by_date_report_task]
            load_fact_login_task >> load_dim_user_task
            load_fact_transaction_task >> load_dim_user_task
            # load_fact_transaction_task >> load_fact_vip_level_task
                    
        game_task_groups.append(task_group)

    
