from config.load_env import config


data_locker_endpoint = config.get("DATA_LOCKER_ENDPOINT")
data_locker_access_key = config.get("DATA_LOCKER_ACCESS_KEY")
data_locker_secret_key = config.get("DATA_LOCKER_SECRET_KEY")
data_locker_bucket_name = config.get("DATA_LOCKER_BUCKET_NAME")
data_locker_bucket_unique_identifier = config.get("DATA_LOCKER_BUCKET_UNIQUE_IDENTIFIER")

