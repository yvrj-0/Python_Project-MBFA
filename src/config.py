import os
import yaml

BASE_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

def load_config(path: str | None = None) -> dict:
    actual_path = path or CONFIG_PATH
    with open(actual_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

cfg = load_config()