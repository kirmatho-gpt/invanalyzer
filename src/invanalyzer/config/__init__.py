"""Configuration helpers for invanalyzer."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


def load_account_brokers(config_path: Path) -> Dict[str, str]:
    """Load account-to-broker mappings from a JSON configuration file."""
    data = json.loads(config_path.read_text(encoding="utf-8"))
    accounts = data.get("accounts", {})
    default_broker = data.get("default_broker")

    if not isinstance(accounts, dict):
        raise ValueError("accounts must be a mapping of account name to broker")

    if default_broker:
        accounts.setdefault("*", default_broker)

    return accounts
