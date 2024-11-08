import os 
import json
from pyspark import SparkFiles
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf


def get_project_directory() -> str:
    current_file_path = os.path.abspath(__file__)
    current_directory = os.path.dirname(current_file_path)
    return os.path.dirname(current_directory)
project_dir = get_project_directory()

def get_evt_name(type_report: str) -> str:
    if "in_app_events" in type_report:
        return "in_app_events"
    if "uninstall" in type_report:
        return "uninstall"
    return "install"


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Rename columns in a DataFrame by standardizing their names.

    Parameters:
    - df: DataFrame to rename columns for.

    Returns:
    - DataFrame with renamed columns.
    """
    def standardize_column_name(name: str) -> str:
        return name.lower().replace(" ", "_")
    new_column_names = {col: standardize_column_name(col) for col in df.columns}
    
    for original, new in new_column_names.items():
        if original == 'Adset ID':
            new = 'dataset_id'
        df = df.withColumnRenamed(original, new)
    return df

def cast_columns_to_string(df: DataFrame, columns: list) -> DataFrame:
    """Cast specified columns to StringType."""
    for column in columns:
        df = df.withColumn(column, col(column).cast(StringType()))
    return df

def fill_na_with_defaults(df: DataFrame) -> DataFrame:
    # Fill NaN values in numeric columns with 0
    numeric_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() in ['int', 'bigint', 'double', 'float']]
    df = df.fillna(0, subset=numeric_columns)
    
    # Fill NaN values in string columns with ""
    string_columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
    df = df.fillna("", subset=string_columns)
    
    return df

def parse_nested_json(json_string: str, keys: list):
    try:
        data = json.loads(json_string)
        for key in keys:
            if isinstance(data, str):
                data = json.loads(data)
            data = data.get(key, "")
            if data == "":
                return data
        return data
    except json.JSONDecodeError:
        print(f"Error: json string not valid: {' '.join([str(elem) for elem in keys])}|{json_string}")
        return ""

@udf(returnType=StringType())
def get_carrier(value: str) -> str:
    if value == "VN VINAPHONE":
        value = "VINAPHONE"
    elif value == "VN Vietnamobile":
        value = "Vietnamobile"
    elif value == "VN MobiFone":
        value = "Mobifone"
    elif value == None:
        value = "null"
    return value.lower()

@udf(returnType=StringType())
def get_state(country: str, value: str) -> str:
    with open(SparkFiles.get("state.json"), encoding='utf-8') as f:
        state = json.loads(f.read())
    try:
        if len(value) > 3:
            return (
                value.replace("Djong", "Dong")
                .replace("Djinh", "Dinh")
                .replace(" Province", "")
                .replace(" City", "")
                .replace("Hanoi", "Ha Noi")
                .replace("Haiphong", "Hai Phong")
                .replace("Djak", "Dak")
            )
        return state.get(country, {}).get(value, "unknown")
    except Exception as e:
        return value

@udf(returnType=StringType())
def get_country(value: str) -> str:
    with open(SparkFiles.get("region.json"), encoding='utf-8') as f:
        region = json.loads(f.read())
    try:
        if value == "Vietnam":
            return "Viet Nam"
        if len(value) > 2:
            return value
        return region.get(value, "unknown")
    except:
        return value

@udf(returnType=StringType())
def get_region(value: str) -> str:
    with open(SparkFiles.get("continent.json"), encoding='utf-8') as f:
        continent = json.loads(f.read())
    try:
        if len(value) > 2:
            return value
        return continent.get(value, "unknown")
    except:
        return value

@udf(returnType=StringType())
def get_user_id_for_all(json_data: str, platform: str) -> str:
    try:
        data = json.loads(json_data)
    except Exception as e:
        return ""

    if platform == "ios":
        if 'user_id' in data:
            return data['user_id']
        else:
            return ""
    elif platform == "android":
        if 'af_fid' in data:
            return data['af_fid']
        else:
            if type(data["af_content"]) is str:
                af_content = json.loads(data["af_content"])
            elif type(data["af_content"]) is dict:
                af_content = data["af_content"]
            else:
                raise Exception("Data invalid")

            try:
                return af_content['user_id']
            except Exception as e:
                print(f"data: {data}")
                return ""
    else:
        return data['af_fid']

@udf(returnType=StringType())
def get_server_id_for_all(json_data: str, platform: str) -> str:
    try:
        data = json.loads(json_data)
    except Exception as e:
        return ""

    if platform == "ios":
        if 'server_id' in data:
            return data['server_id']
        else:
            return data['af_server_id']
    elif platform == "android":
        if 'af_server_id' in data:
            return data['af_server_id']
        else:
            if type(data["af_content"]) is str:
                af_content = json.loads(data["af_content"])
            elif type(data["af_content"]) is dict:
                af_content = data["af_content"]
            else:
                raise Exception("Data invalid")

            return af_content['server_id']
    else:
        return data['af_server_id']

@udf(returnType=StringType())
def parse_server_name_by_platform(json_data: str, platform: str):
    try:
        data = json.loads(json_data)
    
        if platform == "ios":
            result = data.get("server_name", "")
        elif platform == "android":
            af_content = data.get("af_content", {})
            if isinstance(af_content, str) and af_content != "":
                af_content = json.loads(af_content)

            af_extra_data = af_content.get("af_extra_data", {})
            if isinstance(af_extra_data, str) and af_extra_data != "":
                af_extra_data = json.loads(af_extra_data)
                result = af_extra_data.get("server_name", "")
            else:
                result = ""
        else:
            result = data.get("af_server_name", "")
        
        return result
    except json.JSONDecodeError:
        print(f"Error: json string not valid: {platform} | {json_data}")
        return ""

@udf(returnType=StringType())
def parse_mobile_carrier_by_platform(json_data: str, platform: str):
    try:
        data = json.loads(json_data)
        if platform == "ios":
            result = data.get("mobile_carrier", "")
        elif platform == "android":
            af_content = data.get("af_content", {})
            if isinstance(af_content, str) and af_content != "":
                af_content = json.loads(af_content)
                result = af_content.get("mobile_carrier", "")
            else:
                result =""
        else:
            result = data.get("af_mobile_carrier", "")
        return result
    except json.JSONDecodeError:
        print(f"Error: json string not valid: {platform} | {json_data}")
        return ""

@udf(returnType=StringType())
def get_character_id_for_all(json_data: str, platform: str) -> str:
    try:
        data = json.loads(json_data)
    except Exception as e:
        return ""

    if platform == "ios":
        if 'character_id' in data:
            return data['character_id']
        else:
            return data['af_character_id']
    elif platform == "android":
        if 'af_character_id' in data:
            return data['af_character_id']
        else:
            if type(data["af_content"]) is str:
                af_content = json.loads(data["af_content"])
            elif type(data["af_content"]) is dict:
                af_content = data["af_content"]
            else:
                raise Exception("Data invalid")

            return af_content['character_id']
    else:
        return data['af_character_id']

@udf(returnType=StringType())
def parse_login_method_by_platform(json_data: str, platform: str):
    try:
        data = json.loads(json_data)
        if platform == "ios":
            af_ext = data.get("af_ext", {})
            if isinstance(af_ext, str) and af_ext != "":
                af_ext = json.loads(af_ext)
                result = af_ext.get("login_method", "")
            else:
                result = ""
        else:
            result = data.get("af_login_method", "")
        return result
    except json.JSONDecodeError:
        print(f"Error: json string not valid: {platform} | {json_data}")
        return ""


@udf(returnType=StringType())
def parse_nested_json_udf(json_string: str, keys_str: str):
    try:
        data = json.loads(json_string)
        if keys_str in data:
            return data[keys_str]
        else:
            return ""
    except json.JSONDecodeError:
        print(f"Error: json string not valid: {keys_str} | {json_string}")
        return ""
