import logging
import boto3
from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from config import data_locker, minio
from utils.minio_utils import connect_minio, check_and_create_bucket
from utils.spark_utils import create_spark_s3_session

class S3BucketSensor(BaseSensorOperator):

    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, bucket_name: str, event_report: str, config:dict, *args,
                 **kwargs):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.event_report = event_report
        self.config = config
        super().__init__(*args, **kwargs)

    def poke(self, context):
        logger = logging.getLogger(__name__)

        # Init client
        s3_client = boto3.client('s3',
                                 endpoint_url=self.endpoint_url,
                                 aws_access_key_id=self.access_key,
                                 aws_secret_access_key=self.secret_key)

        time_check = context["ti"].xcom_pull(task_ids="load_config", key="start_date")
        config = self.config


        if 'data_locker_bucket_s3' not in config:
            raise AirflowException("Not have config data locker")

        file_path = config['data_locker_bucket_s3']
        file_path = f"{data_locker.data_locker_bucket_unique_identifier}/{file_path}/t={self.event_report}/dt={time_check}"
        logger.info(f"bucket_name: {self.bucket_name}")
        logger.info(f"file_path: {file_path}")

        #
        objects = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=file_path)
        if objects is None or 'Contents' not in objects:
            return False

        logger.info(f"objects: {objects}")
        for object in objects['Contents']:
            logger.info(f"object: {object}")

        return True

def ingest_data_locker(config: dict, event_report: str, date:str):
    # Validate config for required keys
    required_keys = ['data_locker_bucket_s3', 'data_locker_bucket_minio']
    for key in required_keys:
        if key not in config:
            raise Exception(f"Missing config: {key}")
    
    data_locker_bucket_s3 = config['data_locker_bucket_s3']
   
    # Initialize Spark session 
    
    spark = create_spark_s3_session(f"Ingest Locker Data Event {event_report} to {data_locker_bucket_s3} on {date}", 
                                    data_locker.data_locker_endpoint,
                                    data_locker.data_locker_access_key,
                                    data_locker.data_locker_secret_key)

    try:
        # Optimize by reading only the necessary files using filter partition pruning (if possible)
        file_path = f"s3a://{data_locker.data_locker_bucket_name}/{data_locker.data_locker_bucket_unique_identifier}/{data_locker_bucket_s3}/t={event_report}/dt={date}/*"
        logging.info(f"Reading from file path: {file_path}")

        # Read CSV from S3 with optimized options
        df = (spark.read.csv(header=True,
                                     path=file_path,
                                     inferSchema=True,
                                     encoding="utf-8",
                                     quote='"',
                                     escape='"',
                                     multiLine=True,
                                     recursiveFileLookup=True))
        
        minio_client = connect_minio()
        check_and_create_bucket(minio_client, minio.s3_cdp_bucket)

        # Save to minIO local
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio.s3_endpoint)
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio.s3_access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio.s3_secret_key)

        output_path = f"s3a://{minio.s3_cdp_bucket}/data_locker/conn={data_locker_bucket_s3}/evt={event_report}/dt={date}/{event_report}_{date}.csv"
        logging.info(f"Saving to MinIO at: {output_path}")
        
        # Write the DataFrame directly to MinIO as CSV
        df.write.mode('overwrite').csv(output_path, header=True, encoding="utf-8")

        # object_key = f"data_locker/conn={data_locker_bucket_s3}/evt={event_report}/dt={date}/{event_report}_{date}.csv"
        # logging.info(f"Saving to MinIO at: {object_key}")
       
        # minio_client.put_object(Bucket=minio.s3_cdp_bucket,
        #                     Key=object_key,
        #                     Body=df.toPandas().to_csv(index=False).encode('utf-8'),
        #                     ContentType='text/csv')
 
        logging.info(f"Successfully saved file to MinIO at: {output_path}")

    except Exception as e:
        logging.error(f"Error in ingest_data_locker: {e}")
        raise e

    finally:
        # Stop the Spark session
        spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    event_report = sys.argv[2] 
    date = sys.argv[3]   

    ingest_data_locker(config, event_report, date)