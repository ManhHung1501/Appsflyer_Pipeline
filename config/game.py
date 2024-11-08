import json
from utils.common_utils import project_dir

# Path to your JSON file
json_file_path = f"{project_dir}/config/game_config.json"

# Load the JSON file
with open(json_file_path, 'r') as f:
    game_config = json.load(f)