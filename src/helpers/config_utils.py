from pathlib import Path
import yaml
from typing import Any, Dict

def load_config() -> Dict[str, Any]:
    """
    Charge la config depuis src/config.yaml
    """
    cfg_path = Path(__file__).resolve().parents[1] / "config.yaml"
    with open(cfg_path, encoding="utf-8") as f:
        return yaml.safe_load(f)
