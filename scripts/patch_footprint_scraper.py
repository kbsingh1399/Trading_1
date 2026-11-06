import os
import sys
from pathlib import Path

target_file = Path("1_coinglass_footprint_scraper.py")
content = target_file.read_text(encoding="utf-8")

# 1. Ensure update_all_auction_profiles is in a finally block
old_sweep_code = """    for idx, sym in enumerate(symbols):
        try:
            mt5_sym = mt5_map.get(sym, f"{sym}USD")
            res = await extract_symbol_footprint(ws, sym, mt5_sym)
            if res.get("error"):
                logger.debug(f"[{sym}] Extraction notice: {res.get('error')}")
                continue

            signals = res.get("signals", [])
            candles = res.get("candles", [])

            if candles:
                dump_candles(sym, candles)

            if signals:
                new_s = dump_signals(signals, candles=candles)
                total_signals += len(signals)
                total_new += len(new_s)
                try:
                    latest_px = float(candles[-1].get("close")) if candles else None
                    update_asset_queue_and_evaluate(sym, new_s if new_s else signals, current_price=latest_px)
                except Exception as eval_err:
                    logger.error(f"[{sym}] Trade evaluator error: {eval_err}")

        except Exception as e:
            logger.error(f"[{sym}] Sweep error: {e}")

        await asyncio.sleep(0.05)

    # Update local auction levels atomically
    try:
        update_all_auction_profiles()
        logger.info("Updated all local Auction Profile tables (Database/Auction_Levels/).")
    except Exception as e:
        logger.warning(f"Error updating auction profiles: {e}")

    logger.info(f"Footprint sweep complete: {total_signals} total signals harvested ({total_new} new).")
    return total_signals, total_new"""

new_sweep_code = """    try:
        for idx, sym in enumerate(symbols):
            try:
                mt5_sym = mt5_map.get(sym, f"{sym}USD")
                res = await extract_symbol_footprint(ws, sym, mt5_sym)
                if res.get("error"):
                    logger.debug(f"[{sym}] Extraction notice: {res.get('error')}")
                    continue

                signals = res.get("signals", [])
                candles = res.get("candles", [])

                if candles:
                    dump_candles(sym, candles)

                if signals:
                    new_s = dump_signals(signals, candles=candles)
                    total_signals += len(signals)
                    total_new += len(new_s)
                    try:
                        latest_px = float(candles[-1].get("close")) if candles else None
                        update_asset_queue_and_evaluate(sym, new_s if new_s else signals, current_price=latest_px)
                    except Exception as eval_err:
                        logger.error(f"[{sym}] Trade evaluator error: {eval_err}")

            except Exception as e:
                logger.error(f"[{sym}] Sweep error: {e}")

            await asyncio.sleep(0.05)
    finally:
        # Guarantee local auction levels are updated atomically from all harvested candles
        try:
            update_all_auction_profiles()
            logger.info("Updated all local Auction Profile tables (Database/Auction_Levels/).")
        except Exception as e:
            logger.warning(f"Error updating auction profiles: {e}")

    logger.info(f"Footprint sweep complete: {total_signals} total signals harvested ({total_new} new).")
    return total_signals, total_new"""

if old_sweep_code in content:
    content = content.replace(old_sweep_code, new_sweep_code, 1)
    print("Guaranteed update_all_auction_profiles via try...finally.")
else:
    print("WARNING: old_sweep_code not matched exactly.")

target_file.write_text(content, encoding="utf-8")
print("Successfully patched 1_coinglass_footprint_scraper.py!")
