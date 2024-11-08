import logging
from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip
from typing import Optional
from pyspark.sql.types import StructType

def create_spark_s3_session(app_name: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str) -> SparkSession:
    try:
        spark = (SparkSession.builder
                .appName(app_name)
                .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
                .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.network.timeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                .config("spark.sql.debug.maxToStringFields", "2000")
            ).getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Spark session initialization failed. Error: {e}")
        return None
    
def spark_s3_session_with_delta_pip(app_name: str, s3_endpoint: str, s3_access_key: str, s3_secret_key: str) -> SparkSession:
    try:
        spark = configure_spark_with_delta_pip(
            SparkSession.builder
                    .appName(app_name)
                    .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
                    .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
                    .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
                    .config("spark.hadoop.fs.s3a.path.style.access", "true")
                    .config("spark.sql.parquet.writeLegacyFormat", "true")
                    .config("spark.network.timeout", "600s")
                    .config("spark.executor.heartbeatInterval", "60s")
                    .config("spark.sql.debug.maxToStringFields", "2000")
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            ).getOrCreate()
        return spark
    except Exception as e:
        logging.error(f"Spark session initialization failed. Error: {e}")
        return None
    
def read_data_csv(spark: SparkSession, path: str, schema: Optional[StructType] = None) -> DataFrame:
    try:
        if "s3a://" in path:
            if schema ==None:
                data = spark.read.csv(
                    path=path,
                    header=True,
                    inferSchema=True,
                    encoding="UTF-8",
                    quote='"',
                    escape='"',
                    multiLine=True,
                    recursiveFileLookup=True,
                )
            else:
                data = spark.read.csv(
                    path=path,
                    header=True,
                    schema=schema,
                    encoding="UTF-8",
                    quote='"',
                    escape='"',
                    multiLine=True,
                    recursiveFileLookup=True,
                )
        else:
            data = spark.read.csv(
                f"file://{path}",
                header=True,
                inferSchema=True,
                quote='"',
                escape='"',
                multiLine=True,
            )
        return data
    except Exception as e:
        logging.error(f"Read data from {path} get error:", e)

def log_files_to_read(spark: SparkSession, path_pattern: str):
    # Read data to trigger Spark reading the files and log the file paths
    file_df = spark.read.format("csv").option("header", "true").load(path_pattern)
    
    # Log the file paths being read by Spark
    files_read = file_df.inputFiles()  # Returns a list of file paths
    for file in files_read:
        logging.info(f"Reading file: {file}")