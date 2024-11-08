import logging
from pyspark.sql import Window
from pyspark.sql.functions import input_file_name, col, when, row_number
from config.minio import s3_access_key,s3_cdp_bucket,s3_endpoint,s3_secret_key
from constants.appsflyer import AppsflyerFields
from utils.common_utils import rename_columns, cast_columns_to_string, get_carrier, get_state, get_country, project_dir
from utils.spark_utils import create_spark_s3_session, read_data_csv
from utils.clickhouse_utils import load_data_to_clickhouse


def trans_af_install(config:dict, platform: str, date: str, game_code: str):
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
    spark = create_spark_s3_session(f"Transform Event Appsflyer Install game {game_code} platform {platform} on {date}", 
                                s3_endpoint,
                                s3_access_key,
                                s3_secret_key)
    
    spark.sparkContext.addFile(f'{project_dir}/resources/location/region.json')
    spark.sparkContext.addFile(f'{project_dir}/resources/location/state.json')

    for type_or in ["organic", "non-organic"]:
        if type_or == "non-organic":
            install_organic = "installs_report"
        else:
            install_organic = "organic_installs_report"

        file_path = f"s3a://{s3_cdp_bucket}/cdp/stage/{game_code}/appsflyer/{platform}/evt_name=install/type={type_or}/{app_id}-{install_organic}-from-{date}-to-{date}.csv"
        logging.info(f"file_path: {file_path}")

        
        # Read data from minIO by spark
        data_frame = read_data_csv(spark=spark, path=file_path).select(AppsflyerFields.af_install_fields)
        logging.info(f'Read data from {file_path} success')
        # Transform data
        window_spec = Window.partitionBy("appsflyer_id").orderBy("order_value", "event_time")
        df_renamed = rename_columns(data_frame)

        col_cast_string = ["adset_id", "os_version", "event_revenue_currency", "cost_model", "cost_value", "cost_currency", "channel"]
        df_casted_string = cast_columns_to_string(df_renamed, col_cast_string)
        
        df_final = (df_casted_string.withColumn("path", input_file_name())        
                .withColumn("campaign_type",
                            when(col("path").contains("non-organic"), "non-organic").otherwise("organic"))
                .withColumn("media_source", 
                    when(
                        col("media_source").isin("unknown", "", "null") & (col("campaign_type") == "non-organic"), 
                        "unknown"
                    ).when(
                        col("campaign_type") == "organic", "organic").otherwise(col("media_source")))
                .withColumn("campaign",
                            when(col("campaign_type") == "organic", "organic").otherwise(col("campaign")))
                .withColumn("adset", when(col("campaign_type") == "organic", "organic").otherwise(col("adset")))
                .withColumn("order_value", when(col("media_source").isin("unknown"), 2).otherwise(1))
                .withColumn("carrier", get_carrier(col("carrier")))
                .withColumn("state", get_state(col("country_code"), col("state")))
                .withColumn("country_code", get_country(col("country_code")))
                .withColumn("row_number", row_number().over(window_spec))
                .where(col("row_number") == 1)
                .drop("order_value", "path", "row_number")
                .fillna(""))

        # Save data to clickhouse
        logging.info(f'Loading data to Clickhouse ...')
        load_data_to_clickhouse(df=df_final,target_db=f"da_cdp_{game_code}", target_tbl="appsflyer_install")

    spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    platform = sys.argv[2] 
    date = sys.argv[3]  
    game_code = sys.argv[4]  

    trans_af_install(config, platform, date, game_code)