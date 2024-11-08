import logging
from datetime import datetime, timedelta, date



def load_config_task(**context):
    logger = logging.getLogger(__name__)

    # Get start date config
    if 'start_date' in context['dag_run'].conf:
        start_date = context['dag_run'].conf['start_date']
        logger.info(f"Use config excution date: {start_date}")
    else:
        yes = datetime.now() - timedelta(1)
        start_date = str(date(yes.year, yes.month, yes.day))
        logger.info(f"Use default for excution date yesterday: {start_date}")

    context['ti'].xcom_push(key="start_date", value=start_date)


    

