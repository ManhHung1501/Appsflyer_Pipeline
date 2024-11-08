from airflow.models import Variable
from decouple import Config, RepositoryEnv
from utils.common_utils import project_dir

# Current environment
ENVIRONMENT = Variable.get('DE_ENVIRONMENT', default_var="DEVELOPMENT")

dev_env_file = f'{project_dir}/.env.dev'
prod_env_file = f'{project_dir}/.env.prod'
# Env file path
env_files = {
    'DEVELOPMENT': dev_env_file,
    'PRODUCTION': prod_env_file,
}
# get .env by environment
env_file = env_files.get(ENVIRONMENT, dev_env_file)


# Config object create
config = Config(RepositoryEnv(env_file))