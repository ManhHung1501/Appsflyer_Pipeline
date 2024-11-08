from config.load_env import config

s3_endpoint = config.get("S3_END_POINT")
s3_access_key = config.get("S3_ACCESS_KEY")
s3_secret_key = config.get("S3_SECRET_KEY")
s3_cdp_bucket = config.get("S3_CDP_BUCKET")

