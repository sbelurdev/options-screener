"""
Shared YAML profile load/merge/save helpers.

Extracted so new code (agent/daytrading) reuses the app's existing persistence
semantics rather than growing another copy. `app.py` and `main.py` still carry
their own equivalent helpers; unifying those is a separate follow-up and is
deliberately not bundled into the DayTrading work.

Merge semantics match app.py exactly, including the one non-obvious rule: an
explicitly empty mapping in an override means "clear the inherited mapping",
not "leave it alone".
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import yaml

BASE_CONFIG_PATH = Path("config/base.yaml")
USERS_CONFIG_DIR = Path("config/users")


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            # An explicit empty map is a full override so profiles can clear
            # inherited nested settings.
            merged[key] = {} if not value else deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_yaml(path: Path) -> Dict[str, Any]:
    if path.exists():
        with path.open(encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def load_merged_config(profile: str) -> Dict[str, Any]:
    base = load_yaml(BASE_CONFIG_PATH)
    if profile:
        return deep_merge(base, load_yaml(USERS_CONFIG_DIR / f"{profile}.yaml"))
    return base


def save_profile(profile: str, updates: Dict[str, Any]) -> None:
    path = USERS_CONFIG_DIR / f"{profile}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    merged = deep_merge(load_yaml(path), updates)
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(merged, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
