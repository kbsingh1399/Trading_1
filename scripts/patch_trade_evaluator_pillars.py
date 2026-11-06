"""
scripts/patch_trade_evaluator_pillars.py
========================================================================================
Applies Pillar 6, 7, and 8 loaders and Confluence scoring integration to trade_evaluator.py
========================================================================================
"""
import sys
from pathlib import Path

FILE_PATH = Path(r"c:\Users\SIGMA\Documents\_Trading_1\trade_evaluator.py")

LOADERS_CODE = '''    @classmethod
    def load_oi_funding_context(cls, symbol: str) -> dict:
        """Pillar 6: Loads Open Interest and Predicted Funding Rate telemetry."""
        p = DB_DIR / "Market_Data" / "oi_and_funding.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "open_interest_usd": safe_float(row.get("open_interest_usd"), 0.0),
                "funding_rate": safe_float(row.get("funding_rate"), 0.0),
                "funding_rate_pct": safe_float(row.get("funding_rate_pct"), 0.0),
                "squeeze_bias": str(row.get("squeeze_bias") or "NEUTRAL"),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading OI/Funding data: {e}")
            return {}

    @classmethod
    def load_sentiment_context(cls, symbol: str) -> dict:
        """Pillar 7: Loads Top Trader vs Global Retail Sentiment Divergence."""
        p = DB_DIR / "Market_Data" / "sentiment_divergence.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "top_long_pct": safe_float(row.get("top_long_pct"), 50.0),
                "top_short_pct": safe_float(row.get("top_short_pct"), 50.0),
                "global_long_pct": safe_float(row.get("global_long_pct"), 50.0),
                "global_short_pct": safe_float(row.get("global_short_pct"), 50.0),
                "divergence_spread": safe_float(row.get("divergence_spread"), 0.0),
                "sentiment_bias": str(row.get("sentiment_bias") or "NEUTRAL"),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading Sentiment data: {e}")
            return {}

    @classmethod
    def load_heatmap_density_context(cls, symbol: str, current_price: float) -> dict:
        """Pillar 8: Loads Multi-Timeframe Liquidation Heatmap Intensity."""
        p = DB_DIR / "Market_Data" / "heatmap_density.parquet"
        if not p.exists():
            return {}
        try:
            df = pd.read_parquet(p)
            match = df[df["symbol"].str.upper() == symbol.upper()]
            if match.empty:
                return {}
            row = match.iloc[0].to_dict()
            return {
                "upper_magnet_price": safe_float(row.get("upper_magnet_price")),
                "upper_magnet_dist_pct": safe_float(row.get("upper_magnet_dist_pct")),
                "lower_magnet_price": safe_float(row.get("lower_magnet_price")),
                "lower_magnet_dist_pct": safe_float(row.get("lower_magnet_dist_pct")),
                "asymmetry_ratio": safe_float(row.get("asymmetry_ratio"), 1.0),
                "magnet_bias": str(row.get("magnet_bias") or "BALANCED"),
                "is_magnet_pool": bool(row.get("is_magnet_pool", False)),
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading Heatmap Density data: {e}")
            return {}
'''

with open(FILE_PATH, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Insert Loaders right after load_tv_liquidation_context
target1 = '''            return {
                "latest_long_liq": safe_float(last_row.get("long_liq_usd"), 0.0),
                "latest_short_liq": safe_float(last_row.get("short_liq_usd"), 0.0),
                "recent_long_liq_1h": safe_float(recent_long_liq, 0.0),
                "recent_short_liq_1h": safe_float(recent_short_liq, 0.0),
                "is_spike": is_spike,
                "reversal_bias": reversal_bias,
            }
        except Exception as e:
            logger.debug(f"[{symbol}] Failed reading TV liquidations: {e}")
            return {}'''

if target1 not in content:
    print("ERROR: Target 1 not found!")
    sys.exit(1)

replacement1 = target1 + "\n\n" + LOADERS_CODE
content = content.replace(target1, replacement1, 1)

# 2. Insert loader calls inside evaluate_symbol
target2 = '''        auction_ctx = cls.load_auction_context(symbol)
        whale_ctx = cls.load_whale_context(symbol, ref_price)
        magnet_ctx = cls.load_magnet_context(symbol, ref_price)
        tv_liq_ctx = cls.load_tv_liquidation_context(symbol)'''

replacement2 = '''        auction_ctx = cls.load_auction_context(symbol)
        whale_ctx = cls.load_whale_context(symbol, ref_price)
        magnet_ctx = cls.load_magnet_context(symbol, ref_price)
        tv_liq_ctx = cls.load_tv_liquidation_context(symbol)
        oi_ctx = cls.load_oi_funding_context(symbol)
        sentiment_ctx = cls.load_sentiment_context(symbol)
        heatmap_ctx = cls.load_heatmap_density_context(symbol, ref_price)'''

if target2 not in content:
    print("ERROR: Target 2 not found!")
    sys.exit(1)

content = content.replace(target2, replacement2, 1)

# 3. Add Confluence Modifiers for Pillars 6, 7, and 8
target3 = '''        # Total Confluence Score via Non-Linear Regime-Dependent Confluence Scoring (RDNL-CS)
        raw_confluence = round(w1_score + w2_score + w3_score + w4_score + w5_score, 2)
        regime = classify_market_regime(candle_ctx, auction_ctx, whale_ctx, tv_liq_ctx)
        rdnl_score, m_interact, active_couplings = compute_rdnl_confluence(
            {"W1": w1_score, "W2": w2_score, "W3": w3_score, "W4": w4_score, "W5": w5_score},
            regime
        )
        confluence_score = rdnl_score'''

replacement3 = '''        # -------------------------------------------------------------------------
        # Pillars 6, 7, 8: Microstructure Confluence Expansion Modifiers
        # -------------------------------------------------------------------------
        expansion_boost = 0.0

        # Pillar 6: Open Interest & Predicted Funding Rate Squeeze Acceleration
        squeeze_bias = oi_ctx.get("squeeze_bias", "NEUTRAL")
        if target_direction == "LONG" and squeeze_bias == "BULLISH_SHORT_SQUEEZE":
            expansion_boost += 8.0
        elif target_direction == "SHORT" and squeeze_bias == "BEARISH_LONG_SQUEEZE":
            expansion_boost += 8.0

        # Pillar 7: Top Trader Sentiment Divergence (Smart vs Retail Money)
        sentiment_bias = sentiment_ctx.get("sentiment_bias", "NEUTRAL")
        div_spread = sentiment_ctx.get("divergence_spread", 0.0)
        if target_direction == "LONG" and (sentiment_bias == "BULLISH_SMART_MONEY" or div_spread >= 5.0):
            expansion_boost += 6.0
        elif target_direction == "SHORT" and (sentiment_bias == "BEARISH_SMART_MONEY" or div_spread <= -5.0):
            expansion_boost += 6.0

        # Pillar 8: Multi-Timeframe Heatmap Magnet Intensity
        mag_bias = heatmap_ctx.get("magnet_bias", "BALANCED")
        if target_direction == "LONG" and mag_bias == "BULLISH_UPPER_MAGNET":
            expansion_boost += 6.0
        elif target_direction == "SHORT" and mag_bias == "BEARISH_LOWER_MAGNET":
            expansion_boost += 6.0

        # Total Confluence Score via Non-Linear Regime-Dependent Confluence Scoring (RDNL-CS)
        raw_confluence = round(w1_score + w2_score + w3_score + w4_score + w5_score + expansion_boost, 2)
        regime = classify_market_regime(candle_ctx, auction_ctx, whale_ctx, tv_liq_ctx)
        rdnl_score, m_interact, active_couplings = compute_rdnl_confluence(
            {"W1": w1_score, "W2": w2_score, "W3": w3_score, "W4": w4_score, "W5": w5_score},
            regime
        )
        confluence_score = min(100.0, round(rdnl_score + expansion_boost, 2))'''

if target3 not in content:
    print("ERROR: Target 3 not found!")
    sys.exit(1)

content = content.replace(target3, replacement3, 1)

with open(FILE_PATH, "w", encoding="utf-8") as f:
    f.write(content)

print("SUCCESS: Successfully patched trade_evaluator.py with Pillars 6, 7, and 8!")
