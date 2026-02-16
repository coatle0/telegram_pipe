import yaml
from pathlib import Path
import os

CONFIG_PATH = Path("configs/config.yaml")

def load_config():
    if not CONFIG_PATH.exists():
        # Fallback or default
        return {"channels": []}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_telegram_credentials():
    """
    Resolve Telegram API credentials from environment variables with backward compatibility.
    api_id  = first non-empty of [TELEGRAM_API_ID, TG_API_ID]
    api_hash = first non-empty of [TELEGRAM_API_HASH, TG_API_HASH]
    """
    api_id_raw = os.getenv("TELEGRAM_API_ID") or os.getenv("TG_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH") or os.getenv("TG_API_HASH")
    
    if not api_id_raw or not api_hash:
        raise RuntimeError(
            "Missing Telegram API credentials. "
            "Set TELEGRAM_API_ID/TELEGRAM_API_HASH (preferred) "
            "or TG_API_ID/TG_API_HASH (backward compatibility)."
        )
    try:
        api_id = int(api_id_raw)
    except ValueError:
        raise RuntimeError(
            "Invalid TELEGRAM_API_ID/TG_API_ID: must be numeric integer."
        )
    return api_id, api_hash
