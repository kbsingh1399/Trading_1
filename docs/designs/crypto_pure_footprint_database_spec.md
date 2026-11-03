# Architecture Design Specification: Pure Crypto Footprint Database Restructuring

**Document ID**: `docs/designs/crypto_pure_footprint_database_spec.md`  
**Author**: Elephant (Deep Context Architectural Orchestrator)  
**Status**: REVISED POST-AUDIT (All 5 Auditor Items Remediated)  
**Target Repository**: `C:\Users\SIGMA\Documents\_Trading_1` (`Database/`)  
**Target Prop Firm Account**: FundingPips MatchTrader Competition Account `101305360`  

---

## 1. System Invariants & Core Mandate

1. **Crypto-Only Scope**:
   Per explicit user directive, `Database/` must store **only cryptocurrency assets**. All legacy non-crypto assets (Forex pairs, Precious Metals, Industrial Commodities, Energy/Oil contracts, Equity Indices, and Tokenized Stocks) must be removed from active database partitions and archived cleanly to `archive/non_crypto_database/`.
2. **CoinGlass Footprint Catalog Invariant**:
   Only cryptos actively present on the CoinGlass Footprint tab list (`Database/coinglass_footprint_assets.json['aggregatedSymbols']`) and registered in the execution pipeline are retained in `Database/`. Any symbol without CoinGlass Footprint capability (such as `ZRX`, which is verified absent from `aggregatedSymbols`) must be archived.
3. **Zero Data Loss Invariant**:
   No non-crypto or unsupported data will be destroyed. All moved files and filtered rows from summary parquet tables and streaming JSONL logs are persisted to explicit paths under `archive/non_crypto_database/` with matching directory hierarchies.
4. **Primary IST Timestamp Invariant**:
   All retained cryptocurrency Parquet tables maintain primary Indian Standard Time (IST, UTC+5:30) columns: `open_time_ist`, `close_time_ist`, `time_ist`, `candle_ist`, `open_time_ist_ms`, and `close_time_ist_ms`.
5. **Exact 300-Bar M15 Rolling Windows**:
   `Database/Candle_Data/<SYMBOL>.parquet` maintains exactly 300 bars of completed 15m candles with atomic temporary persistence (`.tmp` -> `os.replace`).

---

## 2. Asset Classification & Single-Source-of-Truth Taxonomy

### 2.1. Whitelisted Active Cryptos (The 31 Footprint Cryptos)
These 31 assets are verified to be present in CoinGlass Footprint `aggregatedSymbols`, supported in MatchTrader, and active in `mutual_asset_capability_registry.json['Cryptos']`:

`['1INCH', 'AAVE', 'ADA', 'ALGO', 'AR', 'ATOM', 'AVAX', 'AXS', 'BAT', 'BCH', 'BNB', 'BTC', 'CHZ', 'COMP', 'CRV', 'DOGE', 'DOT', 'ETH', 'FIL', 'HBAR', 'LINK', 'LTC', 'NEAR', 'NEO', 'SNX', 'SOL', 'SUSHI', 'TRX', 'UNI', 'XRP', 'ZEC']`

### 2.2. Supplemental TV-Only Crypto Liquidations (28 Symbols)
In `Database/coinglass_tv_symbol_map.json` and `Database/Live_Liquidations/`, the allowed symbols consist strictly of the 28 cryptocurrency symbols (pruned of non-cryptos `XAU`, `XAG`, `UKOIL`, `BRENTOIL`, `NDX100`, `SPX500`, `DJI30`, `JP225`):
`BTC`, `ETH`, `SOL`, `XRP`, `DOGE`, `BNB`, `ADA`, `AVAX`, `LINK`, `SUI`, `NEAR`, `TRX`, `LTC`, `HYPE`, `DOT`, `POL`, `BCH`, `UNI`, `APT`, `ATOM`, `ARB`, `OP`, `FIL`, `RENDER`, `FET`, `TAO`, `1000PEPE`, `1000SHIB`.

### 2.3. Assets to Archive to `archive/non_crypto_database/`
- **Forex (20 Pairs)**: `AUDCAD`, `AUDJPY`, `AUDUSD`, `CADJPY`, `CHFJPY`, `EURAUD`, `EURCAD`, `EURCHF`, `EURGBP`, `EURJPY`, `EURUSD`, `GBPAUD`, `GBPCAD`, `GBPJPY`, `GBPUSD`, `NZDCAD`, `NZDUSD`, `USDCAD`, `USDCHF`, `USDJPY`.
- **Precious Metals & Commodities (11 Assets)**: `XAU`, `XAUUSD`, `XAG`, `XAGUSD`, `COPPER`, `GAS`, `BRENTOIL`, `UKOIL`, `UKOUSD`, `USOIL`, `USOUSD`.
- **Equity Indices (8 Assets)**: `SP500`, `SPX500`, `US30`, `DJI30`, `NAS100`, `NDX100`, `GER30`, `JP225`.
- **Non-Footprint Crypto (1 Asset)**: `ZRX` (absent from `aggregatedSymbols` on CoinGlass Footprint).

---

## 3. Step-by-Step Implementation Protocol

### Phase 0: Daemon Quiescence & Lock Release (Windows OS Safety)
Prior to moving any files or modifying parquet structures, the executing agent must ensure zero file-lock contention:
1. Stop background daemon tasks:
   - Terminate `1_coinglass_footprint_scraper.py`, `2_coinglass_whales.py`, `3_coinglass_liquidation_levels.py`, `4_coinglass_tv_scraper.py`, and `5_wake_on_footprint_signal.py`.
2. Kill any lingering child python processes using PowerShell:
   `Get-WmiObject Win32_Process | Where-Object { $_.CommandLine -like "*_coinglass_*" -or $_.CommandLine -like "*wake_on_footprint*" } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`
3. Clean all active writer and singleton lock files:
   - Remove `Database/.locks/*.lock` so all `msvcrt` handles are fully released.

### Phase 1: Physical File Relocation to `archive/non_crypto_database/`
All non-crypto files and `ZRX` files must be moved into structured subfolders under `archive/non_crypto_database/`:

1. **`Candle_Data/`**:
   - Move non-crypto parquets (`AUDCAD.parquet`, `SP500.parquet`, `XAU.parquet`, `ZRX.parquet`, etc.) to:
     `archive/non_crypto_database/Candle_Data/<FILENAME>`
2. **`Auction_Levels/`**:
   - Move non-crypto auction parquets (`AUDCAD_auction_levels.parquet`, `SP500_auction_levels.parquet`, etc.) to:
     `archive/non_crypto_database/Auction_Levels/<FILENAME>`
3. **`Liquidation_Data/`**:
   - For each subdirectory (`Key_Levels/`, `Leverage_Tiers/`, `Magnet_Zones/`, `Near_Field_Balance/`, `Strength_By_Distance/`):
     Move matching non-crypto/ZRX files to `archive/non_crypto_database/Liquidation_Data/<SubDir>/<FILENAME>`
4. **`Live_Liquidations/`**:
   - Move non-crypto files (`BRENTOIL_tv_liquidations.parquet`, `UKOIL_tv_liquidations.parquet`, `NDX100_tv_liquidations.parquet`, `SPX500_tv_liquidations.parquet`, `DJI30_tv_liquidations.parquet`, `JP225_tv_liquidations.parquet`, `XAU_tv_liquidations.parquet`, `XAG_tv_liquidations.parquet`) to:
     `archive/non_crypto_database/Live_Liquidations/<FILENAME>`
5. **`Whale_Orders/`**:
   - Create `archive/non_crypto_database/Whale_Orders/Open_Orders` and `History_Orders` (currently all 12 whale symbols are cryptos; verify clean).

### Phase 2: Row-Level Filtering & Archival for Summary Tables & Logs
To uphold Zero Data Loss, records filtered out of composite tables are preserved into `archive/non_crypto_database/`:

1. **`Database/Signals/footprint_signals.jsonl`**:
   - Read lines; if `line['symbol']` in non-crypto or `ZRX`, append to:
     `archive/non_crypto_database/Signals/footprint_signals_non_crypto.jsonl`
   - Atomically overwrite `Database/Signals/footprint_signals.jsonl` with crypto-only lines.
2. **`Database/Signals/coinglass_aggregated_futures_signals.parquet` & `footprint_signals.parquet`**:
   - Separate into crypto rows and non-crypto rows.
   - Save non-crypto rows to `archive/non_crypto_database/Signals/<FILE>_non_crypto.parquet`.
   - Atomically overwrite active tables with `atomic_parquet(crypto_df, <FILE>)`.
3. **`Database/Liquidation_Data/liquidation_snapshots.parquet`**:
   - Separate non-crypto rows, save to `archive/non_crypto_database/Liquidation_Data/liquidation_snapshots_non_crypto.parquet`.
   - Atomically overwrite active snapshots with crypto-only rows.
4. **`Database/Live_Liquidations/summary_liquidations.parquet` & `latest_reversal_signals.json`**:
   - Filter to the 28 active TV cryptos.
   - Archive filtered-out entries to `archive/non_crypto_database/Live_Liquidations/summary_liquidations_non_crypto.parquet`.
   - Overwrite active summary parquet and json atomically.

### Phase 3: Programmatic Re-indexing of Auction Profiles
- Run:
  `python -c "from Terminal.auction_indicators import update_all_auction_profiles; update_all_auction_profiles()"`
- This will automatically scan the sanitized `Database/Candle_Data/*.parquet` directory and atomically regenerate `Database/Auction_Levels/all_symbols_auction_levels.parquet` and `all_symbols_auction_levels.json` with 100% pure crypto records.

### Phase 4: Configuration & Scraper Code Sanitization
1. **`Database/mutual_asset_capability_registry.json`**:
   - Remove `"Forex"`, `"Metals_Commodities"`, and `"Equity_Indices"` blocks entirely.
   - Remove `"ZRX"` from `"Cryptos"`.
   - Active registry contains strictly the 31 mutual footprint cryptos.
2. **`Database/coinglass_tv_symbol_map.json`**:
   - Remove non-crypto keys (`XAU`, `XAG`, `UKOIL`, `BRENTOIL`, `NDX100`, `SPX500`, `DJI30`, `JP225`).
3. **`4_coinglass_tv_scraper.py`**:
   - Remove hardcoded `"XAU": "Binance_XAUUSDT"` and `"XAG": "Binance_XAGUSDT"` from `DEFAULT_SYMBOL_MAP` (lines 91-92).

### Phase 5: Verification & Daemon Relaunch
1. **Verification**:
   - Run audit script to assert:
     - 0 non-crypto files in `Candle_Data/`, `Auction_Levels/`, `Liquidation_Data/`, `Live_Liquidations/`.
     - `all_symbols_auction_levels.parquet` contains only the 31 crypto symbols.
     - `coinglass_tv_symbol_map.json` contains only the 28 crypto symbols.
     - `mutual_asset_capability_registry.json` contains only `"Cryptos"`.
2. **Relaunch**:
   - Launch all 5 standalone daemons:
     - `python 1_coinglass_footprint_scraper.py --daemon`
     - `python 2_coinglass_whales.py --daemon`
     - `python 3_coinglass_liquidation_levels.py --daemon`
     - `python 4_coinglass_tv_scraper.py --daemon`
     - `python 5_wake_on_footprint_signal.py`
   - Monitor logs to ensure smooth, lock-free execution.
