"""
active_assets_manager.py
========================================================================================
Dynamic Active Asset Registry & Configuration Manager
========================================================================================
Manages the live active asset universe for:
- API/standalone_coinglass_footprint_api.py
- API/standalone_coinglass_whales_api.py
- API/standalone_coinglass_liquidation_levels_api.py

Enables live adding/removing of assets without altering any scraper codebase.
"""

import os
import re
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
API_DIR = Path(__file__).resolve().parent
ROOT_DIR = API_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from coinglass_common import writer_lock, atomic_json

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

ACTIVE_ASSETS_FILE = API_DIR / "active_assets.json"
REGISTRY_FILE = ROOT_DIR / "Database" / "mutual_asset_capability_registry.json"

logger = logging.getLogger("ActiveAssetsManager")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [AssetsManager] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _normalize_symbols(symbols) -> List[str]:
    if not isinstance(symbols, list):
        raise ValueError("active_assets must be a list")
    result = set()
    for symbol in symbols:
        if not isinstance(symbol, str):
            raise ValueError(f"Invalid asset symbol: {symbol!r}")
        sym = symbol.upper().strip()
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9_.-]{0,31}", sym):
            raise ValueError(f"Unsafe asset symbol: {symbol!r}")
        result.add(sym)
    return sorted(result)


def _read_active_payload():
    with open(ACTIVE_ASSETS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    raw = data if isinstance(data, list) else data.get("active_assets") if isinstance(data, dict) else None
    symbols = _normalize_symbols(raw)
    if isinstance(data, dict) and data.get("total_active_assets") != len(symbols):
        raise ValueError("active_assets count metadata is inconsistent")
    return data, symbols


def load_active_assets() -> List[str]:
    """Fail closed on malformed configuration; never expand to the capability registry."""
    try:
        return _read_active_payload()[1]
    except Exception as exc:
        logger.error("Active universe unavailable: %s", exc)
        raise RuntimeError(f"Cannot load {ACTIVE_ASSETS_FILE}: {exc}") from exc


def save_active_assets(symbols: List[str], description: Optional[str] = None) -> None:
    clean_syms = _normalize_symbols(list(symbols))
    with writer_lock(ACTIVE_ASSETS_FILE):
        previous = {}
        if ACTIVE_ASSETS_FILE.exists():
            try:
                previous, _ = _read_active_payload()
            except Exception as exc:
                raise RuntimeError("Refusing to overwrite malformed active universe") from exc
        categories = previous.get("categories", {}) if isinstance(previous, dict) else {}
        categories = {k: [s for s in v if s in clean_syms]
                      for k, v in categories.items() if isinstance(v, list)}
        categorized = {s for values in categories.values() for s in values}
        uncategorized = [s for s in clean_syms if s not in categorized]
        if uncategorized:
            categories["Uncategorized"] = uncategorized
        payload = {
            "description": description or (previous.get("description") if isinstance(previous, dict) else None)
                or "Live Dynamic Active Assets Universe",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "total_active_assets": len(clean_syms),
            "active_assets": clean_syms,
            "categories": categories,
        }
        atomic_json(ACTIVE_ASSETS_FILE, payload)
    logger.info("Saved %s active assets", len(clean_syms))


def _mutate_active_assets(symbol: str, add: bool) -> List[str]:
    sym = _normalize_symbols([symbol])[0]
    with writer_lock(ACTIVE_ASSETS_FILE):
        current = load_active_assets()
        updated = sorted(set(current) | {sym}) if add else [s for s in current if s != sym]
        if updated != current:
            save_active_assets(updated)
        return updated


def add_active_asset(symbol: str) -> List[str]:
    return _mutate_active_assets(symbol, True)


def remove_active_asset(symbol: str) -> List[str]:
    return _mutate_active_assets(symbol, False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Active Assets Manager CLI")
    parser.add_argument("--list", action="store_true", help="List all current active assets")
    parser.add_argument("--add", type=str, help="Add a new symbol to active assets")
    parser.add_argument("--remove", type=str, help="Remove a symbol from active assets")
    parser.add_argument("--count", action="store_true", help="Print total count of active assets")
    args = parser.parse_args()

    if args.add:
        add_active_asset(args.add)
    elif args.remove:
        remove_active_asset(args.remove)
    elif args.count:
        assets = load_active_assets()
        print(f"Total active assets: {len(assets)}")
    else:
        assets = load_active_assets()
        print(f"Total Active Assets ({len(assets)}):")
        print(", ".join(assets))
