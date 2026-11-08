# CoinGlass Standalone APIs & Dynamic Telemetry Hub (`API/`)
========================================================================================
**System**: `_Trading_1` Quantitative Pipeline Architecture  
**Directory**: `C:\Users\SIGMA\Documents\_Trading_1\API\`  
**Execution Runtime**: Headless Chrome DevTools Protocol (CDP port 9222) + Local Cloudflare WARP IP Rotation  
========================================================================================

## 1. Architectural Overview & Component Inventory

The `API/` directory houses the standalone telemetry harvesters and the live, dynamic active asset manager. All three standalone scrapers operate independently of third-party cloud APIs and read from a single, shared active asset source without requiring code changes to scale the universe:

```
API/
  ├── active_assets.json                              <- Live dynamic asset universe (Human & Machine Editable)
  ├── active_assets_manager.py                        <- CLI & Programmatic Manager (Add/Remove/List/Count)
  ├── standalone_coinglass_footprint_api.py           <- Pillar 1: Footprint Order Flow Engine
  ├── standalone_coinglass_whales_api.py              <- Pillar 2: Whale Limit Order Book Blotter
  └── standalone_coinglass_liquidation_levels_api.py  <- Pillar 3: Liquidation Levels & Magnet Zones Engine
```

---

## 2. Dynamic Active Asset Management (`API/active_assets.json`)

Assets can be added or subtracted dynamically at runtime without modifying or recompiling any scraper codebase. 

### File Schema:
```json
{
  "description": "Live Dynamic Active Assets Universe for Footprint, Whales, and Liquidation Levels Telemetry",
  "updated_at": "2026-11-07T16:20:00Z",
  "total_active_assets": 121,
  "active_assets": [
    "0G", "1INCH", "2Z", "A", "AAOI", "AAPL", "AAVE", "ABBV", "ACE", "ACH", ...
  ],
  "categories": {
    "Metals": ["PAXG", "XAG", "XAU", "XAUT"],
    "Cryptos": ["0G", "1INCH", "AAVE", "ADA", "BTC", "ETH", "SOL", ...],
    "Equities": ["AAPL", "ABBV", "ADBE", "AMZN", "ARM", "NVDA", ...]
  }
}
```

### CLI Asset Management:
Manage the universe on the fly using `active_assets_manager.py`:
* **List All Active Assets**:
  ```bash
  python API/active_assets_manager.py --list
  ```
* **Print Active Asset Count**:
  ```bash
  python API/active_assets_manager.py --count
  ```
* **Add an Asset Dynamically**:
  ```bash
  python API/active_assets_manager.py --add PEPE
  ```
* **Remove an Asset Dynamically**:
  ```bash
  python API/active_assets_manager.py --remove AAPL
  ```

---

## 3. Persistent Completion Guarantee While-Loop

All three standalone harvesters implement a **Zero-Drop Completion Guarantee Engine**:
1. **Dynamic Queue Evaluation**: At the start of every cycle, the scraper dynamically loads `load_active_assets()` from `API/active_assets.json`.
2. **Pending Set Resolution**: The engine inspects target disk storage and identifies any pending assets (e.g., missing Parquets or failed extractions):
   $$\text{Pending} = \{ s \in \text{ActiveAssets} \mid \neg \text{IsComplete}(s) \}$$
3. **Execution Loop**: The scraper continues looping through pending assets until $\text{len}(\text{Pending}) == 0$.
4. **Hot Asset Additions**: If an operator adds a new asset to `API/active_assets.json` mid-flight, the scraper detects it on its next iteration and appends it to the pending queue immediately—zero downtime, zero restarts.
5. **Execution Modes**:
   - `--once` (default): Sweeps until 100% of current active assets are verified on disk, then exits with code 0.
   - `--daemon`: Operates continuously in the background, sleeping for `--interval` seconds between refresh cycles.

---

## 4. In-Page Tab Navigation & Performance Optimization

Rather than tearing down and reopening browser tabs for each symbol (which causes CPU spikes and cold JS hydration), the scrapers use **Sliding-Window In-Page Soft Navigation**:
- A single warm CDP tab is created per chunk window (e.g. 15–25 assets).
- Subsequent assets navigate via lightweight DOM route shifts (`window.location.href = ...` or `Page.navigate`).
- This preserves the Chrome V8 heap cache, keeps React component layouts and fonts warm, and drops average per-asset harvest time to **5–8 seconds**.
- Tabs are recycled cleanly between chunk windows alongside proactive Cloudflare WARP IP rotations.

---

## 5. Execution Reference

### Pillar 1: Footprint Engine
```bash
# Sweep all active assets until 100% complete
python API/standalone_coinglass_footprint_api.py --all-assets

# Continuous daemon mode (refreshing every 300s)
python API/standalone_coinglass_footprint_api.py --all-assets --daemon --interval 300
```

### Pillar 2: Whale Limit Order Blotter
```bash
# Sweep all active assets until 100% complete
python API/standalone_coinglass_whales_api.py --all-assets

# Continuous daemon mode (refreshing every 300s)
python API/standalone_coinglass_whales_api.py --all-assets --daemon --interval 300
```

### Pillar 3: Liquidation Levels & Magnet Zones
```bash
# Sweep all active assets until 100% complete (15 Parquets per asset)
python API/standalone_coinglass_liquidation_levels_api.py

# Continuous daemon mode (refreshing every 300s)
python API/standalone_coinglass_liquidation_levels_api.py --daemon --interval 300
```
