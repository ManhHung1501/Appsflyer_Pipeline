from clickhouse_driver import Client
from config.clickhouse import clickhouse_host, clickhouse_user, clickhouse_password, clickhouse_settings, clickhouse_port
import datetime
from pyspark.sql import DataFrame

def connect_clickhouse():
    client = Client(
        clickhouse_host,
        user=clickhouse_user,
        password=clickhouse_password,
        settings=clickhouse_settings,
    )
    return client


def generate_create_table_query(df, target_db: str, target_table: str, engine: str, primary_column: str = None) -> str:
    # Map pandas dtypes to ClickHouse data types
    dtype_mapping = {
        'int64': 'UInt64',
        'int32': 'Int32',
        'float64': 'Float64',
        'object': 'String',
        'datetime64[ns]': 'DateTime',
        'bool': 'UInt8',
    }

    # Start building the query
    create_table_query = f"CREATE TABLE IF NOT EXISTS {target_db}.{target_table} (\n"

    # Add columns and their corresponding types
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'datetime64[ns]' and (df[col].dt.time == datetime.time(0, 0)).all():
            clickhouse_type = 'Date' 
        else:
            clickhouse_type = dtype_mapping.get(str(dtype), 'String')
        columns.append(f"    {col} {clickhouse_type}")

    # Combine the column definitions
    create_table_query += ",\n".join(columns)

    # Add the engine and primary column (ORDER BY) if provided
    create_table_query += f"\n) ENGINE = {engine}()"

    if primary_column:
        create_table_query += f"\nORDER BY ({primary_column});"
    else:
        create_table_query += f"\nORDER BY tuple();"

    return create_table_query

def generate_create_table_query_spark_df(df: DataFrame, target_db: str, target_table: str, engine: str, primary_column: str = None) -> str:
    # Map Spark SQL dtypes to ClickHouse data types
    dtype_mapping = {
        'date': 'Date',
        'string': 'String',
        'int': 'Int32',
        'float': 'Float64',
        'double': 'Float64',
        'boolean': 'UInt8',
        'timestamp': 'DateTime'
    }

    # Start building the query
    create_table_query = f"CREATE TABLE IF NOT EXISTS {target_db}.{target_table} (\n"

    # Add columns and their corresponding types
    columns = []
    for col, dtype in df.dtypes:
        # If the column is a Timestamp and has only date part, use Date instead of DateTime
        if dtype == 'TimestampType':
            try:
                if df.select(col).filter(df[col].isNotNull()).rdd.map(lambda row: row[0].time() == datetime.time(0, 0)).reduce(lambda a, b: a and b):
                    clickhouse_type = 'Date'
                else:
                    clickhouse_type = 'DateTime'
            except:
                clickhouse_type = 'DateTime'
        else:
            clickhouse_type = dtype_mapping.get(dtype, 'String')  # Default to String if type is not in mapping

        columns.append(f"    {col} {clickhouse_type}")

    # Combine the column definitions
    create_table_query += ",\n".join(columns)

    # Add the engine and primary column (ORDER BY) if provided
    create_table_query += f"\n) ENGINE = {engine}()"

    if primary_column:
        create_table_query += f"\nORDER BY ({primary_column});"
    else:
        create_table_query += f"\nORDER BY tuple();"

    return create_table_query

def load_data_to_clickhouse(df: DataFrame, target_db: str, target_tbl: str, engine: str ='Log()',mode: str="append"):
    try:
        (df.write.format("jdbc")
         .option("driver", "com.github.housepower.jdbc.ClickHouseDriver")
         .option("url", f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}")
         .option("user", clickhouse_user)
         .option("password", clickhouse_password)
         .option("dbtable", f"{target_db}.{target_tbl}")
         .option("createTableOptions", f"ENGINE={engine}")
         .option("preferTimestampNTZ", "true")
         .option("isolationLevel", "NONE")
         .mode(mode)
         .save())
    except Exception as e:
        raise Exception(f"Load data to Clickhouse table {target_db}.{target_tbl} get error: {e}")
        