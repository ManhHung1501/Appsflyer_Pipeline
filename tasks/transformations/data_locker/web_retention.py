import logging
from pyspark.sql.functions import col
from config.minio import s3_access_key,s3_endpoint,s3_secret_key, s3_cdp_bucket
from utils.common_utils import cast_columns_to_string
from utils.spark_utils import create_spark_s3_session, read_data_csv
from utils.clickhouse_utils import load_data_to_clickhouse

def trans_af_web_retention(config:dict, date: str, game_code: str):
    logging.info(f"config: {config}")
    if 'data_locker_bucket_s3' not in config:
        raise Exception("Not have config data locker: data_locker_bucket_s3")


    # Init spark session
    spark = create_spark_s3_session(f"Transform Event Web Retention game {game_code} on {date}", 
                                    s3_endpoint,
                                    s3_access_key,
                                    s3_secret_key)


    file_path = f"s3a://{s3_cdp_bucket}/data_locker/conn={config['data_locker_bucket_s3']}/evt=website_events/dt={date}/*"
    logging.info(f"file_path: {file_path}")

    # Read data from minIO by spark
    data_frame = read_data_csv(spark=spark, path=file_path)

    # Transform data
    data_frame = data_frame.drop("_c0").filter(col("event_name").rlike("^af_retention_"))
    col_cast_string = ["event_time"]
    df_casted_string = cast_columns_to_string(data_frame, col_cast_string)
    df_final  = (df_casted_string.where(col("bundle_id") == config.get('web_bundle_id'))
                .drop("postal_code")
                .fillna(""))

    # Save data to clickhouse
    logging.info(f'Loading data to Clickhouse ...')
    load_data_to_clickhouse(df=df_final,target_db=f"da_cdp_{game_code}", target_tbl="appsflyer_web_retention")

    spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    date = sys.argv[2]  
    game_code = sys.argv[3]  

    trans_af_web_retention(config, date, game_code)