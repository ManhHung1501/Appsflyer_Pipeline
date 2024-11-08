import logging
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, DateType, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, lit, when
from config.minio import s3_access_key,s3_cdp_bucket,s3_endpoint,s3_secret_key
from constants.appsflyer import AggReportFields
from utils.common_utils import rename_columns, fill_na_with_defaults
from utils.spark_utils import create_spark_s3_session, read_data_csv
from utils.clickhouse_utils import load_data_to_clickhouse, generate_create_table_query_spark_df, connect_clickhouse

def trans_partners_by_date(config:dict, platform: str, date: str, game_code: str):
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
    spark = create_spark_s3_session(f"Transform Event Appsflyer Login game {game_code} platform {platform} on {date}", 
                                    s3_endpoint,
                                    s3_access_key,
                                    s3_secret_key)

    two_days_ago = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')
    file_path = f"s3a://{s3_cdp_bucket}/cdp/stage/{game_code}/appsflyer/{platform}/evt_name=partners_by_date_report/{app_id}-partners_by_date_report-from-{two_days_ago}-to-{date}.csv"
    logging.info(f"file_path: {file_path}")

    # Read data from minIO by spark
    # Define the schema for partners_by_date
    partners_by_date_schema = StructType([
        StructField("Date", DateType(), True),
        StructField("Agency/PMD (af_prt)", StringType(), True),
        StructField("Media Source (pid)", StringType(), True),
        StructField("Campaign (c)", StringType(), True),
        StructField("Impressions", IntegerType(), True),
        StructField("Clicks", IntegerType(), True),
        StructField("CTR", FloatType(), True),
        StructField("Installs", IntegerType(), True),
        StructField("Conversion Rate", FloatType(), True),
        StructField("Sessions", IntegerType(), True),
        StructField("Loyal Users", IntegerType(), True),
        StructField("Loyal Users/Installs", FloatType(), True),
        StructField("Total Revenue", FloatType(), True),
        StructField("Total Cost", FloatType(), True),
        StructField("ROI", FloatType(), True),
        StructField("ARPU", FloatType(), True),
        StructField("Average eCPI", FloatType(), True)
    ])
    data_frame = read_data_csv(spark=spark, path=file_path, schema=partners_by_date_schema).select(AggReportFields.partners_by_date)
    data_frame = data_frame.withColumnRenamed('Date', 'report_date') \
                            .withColumnRenamed('Agency/PMD (af_prt)', 'partner') \
                            .withColumnRenamed('Loyal Users/Installs', 'loyal_users_installs_rate') \
                            .withColumn(
                                'media_source',
                                when(col('Media Source (pid)') == 'Organic', 'organic').otherwise(col('Media Source (pid)'))
                            ) \
                            .withColumn(
                                'campaign',
                                when(col('Media Source (pid)') == 'Organic', 'organic').otherwise(col('Campaign (c)'))
                            ) \
                            .withColumn("platform", lit(platform)) \
                            .drop('Media Source (pid)', 'Campaign (c)')
    # Transform data
    df_final = fill_na_with_defaults(rename_columns(data_frame))
    
    # Save data to clickhouse
    logging.info(f'Loading data to Clickhouse ...')
    target_db = f"da_cdp_{game_code}"
    target_tbl = "appsflyer_partners_by_date"
    primary_column = 'report_date,partner,media_source,campaign,platform'

    # Create table
    create_table_query  = generate_create_table_query_spark_df(df=df_final, engine='ReplacingMergeTree', 
                                         target_db=target_db, target_table=target_tbl,
                                         primary_column=primary_column
                                        )

    clickhouse_client = connect_clickhouse()
    clickhouse_client.execute(create_table_query)

    #Load data
    load_data_to_clickhouse(df=df_final,target_db=target_db, target_tbl=target_tbl)
    spark.stop()

    clickhouse_client.execute(f"OPTIMIZE TABLE {target_db}.{target_tbl} DEDUPLICATE BY {primary_column}")

if __name__ == "__main__":
    import sys
    import json
    config = json.loads(sys.argv[1])
    platform = sys.argv[2] 
    date = sys.argv[3]  
    game_code = sys.argv[4]  

    trans_partners_by_date(config, platform, date, game_code)