import logging
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType
from config.minio import s3_access_key,s3_cdp_bucket,s3_endpoint,s3_secret_key
from constants.appsflyer import AppsflyerEventTrackingMKT
from utils.common_utils import rename_columns, cast_columns_to_string,get_user_id_for_all, get_server_id_for_all, parse_server_name_by_platform, \
    parse_mobile_carrier_by_platform, get_character_id_for_all
from utils.spark_utils import create_spark_s3_session, read_data_csv
from utils.clickhouse_utils import load_data_to_clickhouse

def trans_af_vip_level(config:dict, platform: str, date: str, game_code: str):
    logging.info(f"config: {config}")
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

    # Init spark session
    spark = create_spark_s3_session(f"Transform Event Appsflyer Vip Level game {game_code} platform {platform} on {date}", 
                                    s3_endpoint,
                                    s3_access_key,
                                    s3_secret_key)


    for type in ["organic", "non-organic"]:
        organic = "in_app_events_report" if type == "non-organic" else "organic_in_app_events_report"

        file_path = f"s3a://{s3_cdp_bucket}/cdp/stage/{game_code}/appsflyer/{platform}/evt_name=in_app_events/type={type}/{app_id}-{organic}-from-{date}-to-{date}.csv"
        logging.info(f"file_path: {file_path}")

        # Read data from minIO by spark
        data_frame = read_data_csv(spark=spark, path=file_path).select(AppsflyerEventTrackingMKT.level_fields)

        # Transform data
        data_frame = data_frame.na.fill("", data_frame.columns)
        data_frame = data_frame.filter(col("Event Name").rlike("^af_vip_level_"))
        df_renamed = rename_columns(data_frame)
        col_cast_string = ["dataset_id", "campaign_id", "is_retargeting", "event_revenue", "install_time", "event_time"]
        df_casted_string = cast_columns_to_string(df_renamed, col_cast_string)
        df_final  = (df_casted_string.withColumn("dataset_id", col("dataset_id").cast(StringType()))
                      .withColumn("user_id", get_user_id_for_all(col("event_value"), lit(platform)))
                      .withColumn("server_id", get_server_id_for_all(col("event_value"), lit(platform)))
                      .withColumn("server_name", parse_server_name_by_platform(col("event_value"), lit(platform)))
                      .withColumn("character_id", get_character_id_for_all(col("event_value"), lit(platform)))
                      .withColumn("mobile_carrier", parse_mobile_carrier_by_platform(col("event_value"), lit(platform)))
                      .fillna(""))
        # Save data to clickhouse
        logging.info(f'Loading data to Clickhouse ...')
        load_data_to_clickhouse(df=df_final,target_db=f"da_cdp_{game_code}", target_tbl="appsflyer_vip_level")

    spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    platform = sys.argv[2] 
    date = sys.argv[3]  
    game_code = sys.argv[4]  

    trans_af_vip_level(config, platform, date, game_code)