import logging
import functools
from delta import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import input_file_name, col, when, row_number
from pyspark.sql.types import StringType
from config.minio import s3_access_key,s3_cdp_bucket,s3_endpoint,s3_secret_key
from constants.appsflyer import AppsflyerFields
from utils.common_utils import project_dir, rename_columns, get_region, get_carrier, get_state, get_country, \
    get_character_id_for_all, get_user_id_for_all
from utils.spark_utils import spark_s3_session_with_delta_pip, read_data_csv, log_files_to_read
from utils.clickhouse_utils import load_data_to_clickhouse


def trans_af_registration(config:dict, date: str, game_code: str):
    logging.info(f"config: {config}")

    # Init spark session
    spark = spark_s3_session_with_delta_pip(f"Transform Event Appsflyer User Registration game {game_code} on {date}", 
                                s3_endpoint,
                                s3_access_key,
                                s3_secret_key)
    spark.sparkContext.addFile(f'{project_dir}/resources/location/continent.json')
    spark.sparkContext.addFile(f'{project_dir}/resources/location/region.json')
    spark.sparkContext.addFile(f'{project_dir}/resources/location/state.json')

    file_pattern = f"s3a://{s3_cdp_bucket}/cdp/stage/{game_code}/appsflyer/*/evt_name=in_app_events/*/*-from-{date}-to-{date}.csv"
    log_files_to_read(spark=spark, path_pattern=file_pattern)
    
    # Read data from minIO by spark
    data_frame = read_data_csv(spark=spark, path=file_pattern).select(AppsflyerFields.user_fields)
    
    # Transform data
    df_renamed = rename_columns(data_frame).withColumn("user_id", get_user_id_for_all(col("event_value"), col('platform'))) \
            .where(
                col("event_name").isin("af_registration", "af_login", "af_play_now", "af_play_game") 
                & col("user_id").isNotNull() 
                & ~col("user_id").isin("unknown", "", "null")
            )

    window_spec = Window.partitionBy("user_id").orderBy("order_value", "event_time")
    df_final = (df_renamed.withColumn("path", input_file_name())

                  .withColumn("campaign_type",
                              when(col("path").contains("non-organic"), "non-organic").otherwise("organic"))
                  .withColumn("media_source", when(
        col("media_source").isin("unknown", "", "null") & (col("campaign_type") == "non-organic"), "unknown").when(
        col("campaign_type") == "organic", "organic").otherwise(col("media_source")))
                  .withColumn("campaign", when(col("campaign_type") == "organic", "organic").otherwise(col("campaign")))
                  .withColumn("adset", when(col("campaign_type") == "organic", "organic").otherwise(col("adset")))
                  .withColumn("order_value", when(col("media_source").isin("unknown"), 2).otherwise(1))
                  .withColumn("character_id", get_character_id_for_all(col("event_value"), col('platform')))
                  .withColumn("carrier", get_carrier(col("carrier")))
                  .withColumn("state", get_state(col("country_code"), col("state")))
                  .withColumn("region", get_region(col("region")))
                  .withColumn("country_code", get_country(col("country_code")))
                  .withColumn("customer_user_id", col("customer_user_id").cast(StringType()))
                  .withColumn("os_version", col("os_version").cast(StringType()))
                  .withColumn("row_number", row_number().over(window_spec))
                  .where(col("row_number") == 1)
                  .drop("order_value", "path", "row_number")
                  .fillna(""))
    try:
        delta_table_user = DeltaTable.forPath(spark,
                                            f"s3a://{s3_cdp_bucket}/cdp/pub/{game_code}/af_user_registration_new")
        (delta_table_user.alias("user")
        .merge(df_final.alias("update"),
                "user.user_id = update.user_id and user.media_source = 'unknown' and update.media_source <> 'unknown'")
        .whenMatchedUpdateAll()
        .execute())
        (delta_table_user.alias("user")
        .merge(df_final.alias("update"), "user.user_id = update.user_id")
        .whenNotMatchedInsertAll()
        .execute())
    except Exception as e:
        print(e.__str__())

        (df_final.write.format("delta")
        .mode("append")
        .save(f"s3a://{s3_cdp_bucket}/cdp/pub/{game_code}/af_user_registration_new"))

        delta_table_user = DeltaTable.forPath(spark,
                                            f"s3a://{s3_cdp_bucket}/cdp/pub/{game_code}/af_web_user_registration")

    # Save data to clickhouse
    logging.info(f'Loading data to Clickhouse ...')
    target_db= f"da_cdp_{game_code}"
    target_tbl = "appsflyer_user_registration"

    load_data_to_clickhouse(df=delta_table_user.toDF(),target_db=target_db, target_tbl=target_tbl, mode="overwrite")

    spark.stop()

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    date = sys.argv[2]  
    game_code = sys.argv[3]  

    trans_af_registration(config, date, game_code)