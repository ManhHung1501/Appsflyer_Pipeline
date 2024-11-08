from config.load_env import config
funzy_host = config.get("FUNZY_SOURCE_IP")
funzy_port = config.get("FUNZY_SOURCE_PORT")
funzy_user = config.get("FUNZY_SOURCE_USER")
funzy_password = config.get("FUNZY_SOURCE_PASSWORD")
funzy_db = config.get("FUNZY_SOURCE_DB")