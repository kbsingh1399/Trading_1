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
import sys
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass

API_DIR = Path(__file__).resolve().parent
ROOT_DIR = API_DIR.parent
ACTIVE_ASSETS_FILE = API_DIR / "active_assets.json"
REGISTRY_FILE = ROOT_DIR / "Database" / "mutual_asset_capability_registry.json"

logger = logging.getLogger("ActiveAssetsManager")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [AssetsManager] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def load_active_assets() -> List[str]:
    """
    Dynamically reloads and normalizes active assets from API/active_assets.json.
    Ultra-resilient: supports raw list, object with 'active_assets' / 'assets',
    or category dictionary. Falls back to registry if file is unreadable.
    """
    if ACTIVE_ASSETS_FILE.exists():
        try:
            with open(ACTIVE_ASSETS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                return sorted(list({str(s).upper().strip() for s in data if s and isinstance(s, (str, int))}))
            
            if isinstance(data, dict):
                # 1. Check direct active_assets list
                if "active_assets" in data and isinstance(data["active_assets"], list):
                    return sorted(list({str(s).upper().strip() for s in data["active_assets"] if s}))
                
                # 2. Check assets key
                if "assets" in data and isinstance(data["assets"], list):
                    return sorted(list({str(s).upper().strip() for s in data["assets"] if s}))
                
                # 3. Check categories breakdown
                if "categories" in data and isinstance(data["categories"], dict):
                    syms = set()
                    for cat_list in data["categories"].values():
                        if isinstance(cat_list, list):
                            syms.update(str(s).upper().strip() for s in cat_list if s)
                        elif isinstance(cat_list, dict):
                            syms.update(str(s).upper().strip() for s in cat_list.keys() if s)
                    if syms:
                        return sorted(list(syms))
        except Exception as e:
            logger.warning(f"Error reading {ACTIVE_ASSETS_FILE}: {e}. Falling back to capability registry.")

    # Fallback to Database/mutual_asset_capability_registry.json
    if REGISTRY_FILE.exists():
        try:
            with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
                reg = json.load(f)
            syms = set()
            for cat in ["Metals", "Cryptos", "Equities"]:
                cat_dict = reg.get(cat, {})
                syms.update(cat_dict.keys())
            return sorted(list(syms))
        except Exception as e:
            logger.error(f"Failed to read capability registry: {e}")

    # Fallback minimum majors
    return ["BTC", "ETH", "SOL", "XAU", "XAG", "XRP", "ADA", "DOGE"]


def save_active_assets(symbols: List[str], description: Optional[str] = None) -> None:
    """Atomically updates API/active_assets.json with the provided symbol list."""
    clean_syms = sorted(list({str(s).upper().strip() for s in symbols if s}))
    
    # Try to retain categories if previously existing
    categories = {"Metals": [], "Cryptos": [], "Equities": []}
    if ACTIVE_ASSETS_FILE.exists():
        try:
            with open(ACTIVE_ASSETS_FILE, "r", encoding="utf-8") as f:
                prev = json.load(f)
                if isinstance(prev, dict) and "categories" in prev:
                    for k in categories:
                        categories[k] = [s for s in prev["categories"].get(k, []) if s in clean_syms]
        except Exception:
            pass

    payload = {
        "description": description or "Live Dynamic Active Assets Universe for Footprint, Whales, and Liquidation Levels Telemetry",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_active_assets": len(clean_syms),
        "active_assets": clean_syms,
        "categories": categories
    }

    tmp_path = ACTIVE_ASSETS_FILE.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp_path.replace(ACTIVE_ASSETS_FILE)
    logger.info(f"Successfully saved {len(clean_syms)} active assets to {ACTIVE_ASSETS_FILE}")


def add_active_asset(symbol: str) -> List[str]:
    """Adds a symbol to API/active_assets.json if not already present."""
    sym = symbol.upper().strip()
    current = load_active_assets()
    if sym in current:
        logger.info(f"Symbol '{sym}' is already active.")
        return current
    current.append(sym)
    save_active_assets(current)
    logger.info(f"Added '{sym}'. Total active assets: {len(current)}.")
    return current


def remove_active_asset(symbol: str) -> List[str]:
    """Removes a symbol from API/active_assets.json if present."""
    sym = symbol.upper().strip()
    current = load_active_assets()
    if sym not in current:
        logger.warning(f"Symbol '{sym}' not found in active assets.")
        return current
    current.remove(sym)
    save_active_assets(current)
    logger.info(f"Removed '{sym}'. Total active assets: {len(current)}.")
    return current


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
