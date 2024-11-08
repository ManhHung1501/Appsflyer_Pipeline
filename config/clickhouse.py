from config.load_env import config

clickhouse_host = config.get("CLICKHOUSE_SOURCE_IP")
clickhouse_port = config.get("CLICKHOUSE_SOURCE_PORT")
clickhouse_user = config.get("CLICKHOUSE_SOURCE_USER")
clickhouse_password = config.get("CLICKHOUSE_SOURCE_PASSWORD")
clickhouse_settings = {"use_numpy": True}