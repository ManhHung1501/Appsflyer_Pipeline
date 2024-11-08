import logging
import requests
from datetime import datetime, timedelta
from config.appsflyer import appsflyer_token
from config.minio import s3_cdp_bucket
from utils.common_utils import get_evt_name
from utils.minio_utils import connect_minio, check_and_create_bucket


def ingest_appsflyer_raw_data_api(
                            platform: str,
                            type_report: str,
                            additional_field: str,
                            game_code:str, 
                            config: dict,
                            **context
                            ):
    logger = logging.getLogger(__name__)

    # load excution_date
    date = context["ti"].xcom_pull(task_ids="load_config", key="start_date")

    logger.info(f"type_report: {type_report}, game:{game_code}, plaform: {platform}, excution_date: {date}")

    if platform == "android" and 'android_app_id' not in config:
        raise Exception("Not have config data android: android_app_id")
    if platform == "ios" and 'ios_app_id' not in config:
        raise Exception("Not have config data ios: ios_app_id")

    if platform == "android":
        app_id = config['android_app_id']
    elif platform == "ios":
        app_id = config['ios_app_id']
    else:
        raise Exception("Platform not supported")


    # Get data
    response = requests.get(f"https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/{type_report}/v5",
                            headers={
                                "accept": "text/csv",
                                "authorization": f"Bearer {appsflyer_token}",
                            },
                            params={
                                'from': date,
                                'to': date,
                                'timezone': 'Asia/Bangkok',
                                'maximum_rows': 1000000,
                                'additional_fields': additional_field
                            })
    if response.status_code != 200:
        raise Exception(f"Failed to get data from appsflyer with response error: {response.text}")

    logger.info(f"Request success from appsflyer")

    # Write data to MinIO
    organic = "organic" if "organic" in type_report else "non-organic"
    event_name = get_evt_name(type_report)
    object_key = f"cdp/stage/{game_code}/appsflyer/{platform}/evt_name={event_name}/type={organic}/{app_id}-{type_report}-from-{date}-to-{date}.csv"

    s3_client = connect_minio()
    # Check if bucket exist or not
    check_and_create_bucket(s3_client, s3_cdp_bucket)

    s3_client.put_object(Bucket=s3_cdp_bucket,
                         Key=object_key,
                         Body=response.text.encode('utf-8'),
                         ContentType='text/csv')

    logger.info(f"Complete ingest {type_report} to MinIO")

def ingest_appsflyer_agg_data_api(
                            platform: str,
                            type_report: str,
                            game_code:str, 
                            config: dict,
                            **context
                            ):
    logger = logging.getLogger(__name__)

    # load excution_date
    date = context["ti"].xcom_pull(task_ids="load_config", key="start_date")

    logger.info(f"type_report: {type_report}, game:{game_code}, plaform: {platform}, excution_date: {date}")

    if platform == "android" and 'android_app_id' not in config:
        raise Exception("Not have config data android: android_app_id")
    if platform == "ios" and 'ios_app_id' not in config:
        raise Exception("Not have config data ios: ios_app_id")

    if platform == "android":
        app_id = config['android_app_id']
    elif platform == "ios":
        app_id = config['ios_app_id']
    else:
        raise Exception("Platform not supported")

    # Subtract two days
    two_days_ago = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')
    # Get data
    response = requests.get(f"https://hq1.appsflyer.com/api/agg-data/export/app/{app_id}/{type_report}/v5",
                            headers={
                                "accept": "text/csv",
                                "authorization": f"Bearer {appsflyer_token}",
                            },
                            params={
                                'from': two_days_ago,
                                'to': date,
                                'timezone': 'Asia/Bangkok',
                                'maximum_rows': 1000000,
                            })
    if response.status_code != 200:
        raise Exception(f"Failed to get data from appsflyer with response error: {response.text}")

    logger.info(f"Request success from appsflyer")

    # Write data to MinIO
    object_key = f"cdp/stage/{game_code}/appsflyer/{platform}/evt_name={type_report}/{app_id}-{type_report}-from-{two_days_ago}-to-{date}.csv"

    s3_client = connect_minio()
    # Check if bucket exist or not
    check_and_create_bucket(s3_client, s3_cdp_bucket)

    s3_client.put_object(Bucket=s3_cdp_bucket,
                         Key=object_key,
                         Body=response.text.encode('utf-8'),
                         ContentType='text/csv')

    logger.info(f"Complete ingest {type_report} to MinIO")