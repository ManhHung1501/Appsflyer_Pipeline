import logging
from pyspark.sql.functions import col, row_number, lit
from delta import DeltaTable
from pyspark.sql import Window
from pyspark.sql.types import StringType

from config.minio import s3_access_key,s3_endpoint,s3_secret_key, s3_cdp_bucket
from utils.common_utils import parse_nested_json_udf, fill_na_with_defaults
from utils.spark_utils import spark_s3_session_with_delta_pip, read_data_csv
from utils.clickhouse_utils import load_data_to_clickhouse

def trans_af_web_registration(config:dict, date: str, game_code: str):
    logging.info(f"config: {config}")
    if 'data_locker_bucket_s3' not in config:
        raise Exception("Not have config data locker: data_locker_bucket_s3")


    # Init spark session
    spark = spark_s3_session_with_delta_pip(f"Transform Web User Registration game {game_code} on {date}", 
                                s3_endpoint,
                                s3_access_key,
                                s3_secret_key)


    file_path = f"s3a://{s3_cdp_bucket}/data_locker/conn={config['data_locker_bucket_s3']}/evt=website_events/dt={date}/*"
    logging.info(f"file_path: {file_path}")

    # Read data from minIO by spark
    data_frame = read_data_csv(spark=spark, path=file_path)

    window_spec = Window.partitionBy("user_id").orderBy("event_time")

    data_frame = (data_frame.filter(col("event_name").isin("af_web_registration", "af_login", "af_h5_play_game",
                                                           "af_registration", "af_h5_tutorial_complete"))
                  .withColumn("postal_code", col("postal_code").cast(StringType()))
                  .withColumn("user_id", parse_nested_json_udf(col("event_value"),  lit("af_fid")))
                  .withColumn("character_id", parse_nested_json_udf(col("event_value"), lit("af_character_id")))
                  .withColumn("sign_up_method", parse_nested_json_udf(col("event_value"), lit("af_signup_method")))
                  .withColumn("row_number", row_number().over(window_spec))
                  .where(col("row_number") == 1)
                  .where(col("bundle_id") == config.get('web_bundle_id'))
                  .drop("postal_code", "_c0", "row_number")
                )
    data_frame = fill_na_with_defaults(data_frame)
    
    delta_tbl_path = f"s3a://{s3_cdp_bucket}/cdp/pub/{game_code}/af_web_user_registration"
    try:
        delta_table_user = DeltaTable.forPath(spark,delta_tbl_path)
        (delta_table_user.alias("user")
        .merge(data_frame.alias("update"),
                "user.user_id = update.user_id and user.media_source = 'unknown' and update.media_source <> 'unknown'")
        .whenMatchedUpdateAll()
        .execute())
        (delta_table_user.alias("user")
        .merge(data_frame.alias("update"), "user.user_id = update.user_id")
        .whenNotMatchedInsertAll()
        .execute())
    except Exception as e:
        print(e.__str__())

        (data_frame.write.format("delta")
        .mode("append")
        .save(delta_tbl_path))
        
        delta_table_user = DeltaTable.forPath(spark,delta_tbl_path)

    # Save data to clickhouse
    logging.info(f'Loading data to Clickhouse ...')
    target_db= f"da_cdp_{game_code}"
    target_tbl = "af_web_user_registration"

    
    load_data_to_clickhouse(df=delta_table_user.toDF(),target_db=target_db, target_tbl=target_tbl, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    date = sys.argv[2]  
    game_code = sys.argv[3]  

    trans_af_web_registration(config, date, game_code)