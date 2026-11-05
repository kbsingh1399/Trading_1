"""
Test Suite for Aggregated Futures Multi-Exchange Feed
====================================================
Verifies that Binance, OKX, Bybit, and Hyperliquid can be queried concurrently,
synchronized across common timestamps, and merged into unified aggregated bars
with complete volume, delta, trade count parity, and dynamic failover.
"""

import pytest
from unittest.mock import patch, MagicMock
from Terminal.aggregated_futures_feed import AggregatedFuturesFeed, VENUE_SYMBOL_MAP


def test_venue_symbol_map_contains_major_assets():
    for sym in ["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB"]:
        assert sym in VENUE_SYMBOL_MAP
        assert "binance" in VENUE_SYMBOL_MAP[sym]
        assert "okx" in VENUE_SYMBOL_MAP[sym]
        assert "bybit" in VENUE_SYMBOL_MAP[sym]
        assert "hl" in VENUE_SYMBOL_MAP[sym]


def test_aggregated_bars_merging_logic():
    feed = AggregatedFuturesFeed()

    # Mock responses for all 4 venues with identical timestamp
    t0 = 1700000000000
    t_close = t0 + 899999

    mock_binance = {
        t0: {
            "close_time_ms": t_close,
            "open": 80000.0, "high": 80500.0, "low": 79900.0, "close": 80400.0,
            "total_volume": 1000.0, "buy_volume": 600.0, "sell_volume": 400.0,
            "delta": 200.0, "total_trades": 5000.0,
            "has_real_taker": True
        }
    }
    mock_okx = {
        t0: {
            "close_time_ms": t_close,
            "open": 80010.0, "high": 80550.0, "low": 79920.0, "close": 80390.0,
            "total_volume": 600.0, "buy_volume": 300.0, "sell_volume": 300.0,
            "delta": 0.0, "total_trades": 0.0,
            "has_real_taker": False
        }
    }
    mock_bybit = {
        t0: {
            "close_time_ms": t_close,
            "open": 79990.0, "high": 80480.0, "low": 79880.0, "close": 80410.0,
            "total_volume": 400.0, "buy_volume": 200.0, "sell_volume": 200.0,
            "delta": 0.0, "total_trades": 0.0,
            "has_real_taker": False
        }
    }
    mock_hl = {
        t0: {
            "close_time_ms": t_close,
            "open": 80005.0, "high": 80510.0, "low": 79910.0, "close": 80405.0,
            "total_volume": 200.0, "buy_volume": 100.0, "sell_volume": 100.0,
            "delta": 0.0, "total_trades": 1200.0,
            "has_real_taker": False
        }
    }

    with patch.object(feed, "fetch_binance", return_value=mock_binance), \
         patch.object(feed, "fetch_okx", return_value=mock_okx), \
         patch.object(feed, "fetch_bybit", return_value=mock_bybit), \
         patch.object(feed, "fetch_hyperliquid", return_value=mock_hl):

        bars = feed.fetch_aggregated_bars("BTC", interval="15m", limit=1)

    assert len(bars) == 1
    bar = bars[0]
    assert bar["open_time_ms"] == t0
    assert bar["close_time_ms"] == t_close
    # Volume: 1000 + 600 + 400 + 200 = 2200.0
    assert bar["total_volume"] == pytest.approx(2200.0)
    # Buy Volume scaled by Binance taker buy ratio (60%): 2200 * 0.60 = 1320.0
    assert bar["buy_volume"] == pytest.approx(1320.0)
    # Sell Volume: 2200 - 1320 = 880.0
    assert bar["sell_volume"] == pytest.approx(880.0)
    # Delta: 1320 - 880 = 440.0
    assert bar["delta"] == pytest.approx(440.0)
    # Price is anchored to primary reference venue
    assert bar["high"] == 80500.0
    assert bar["low"] == 79900.0
    # Total Trades: 5000 + 1200 = 6200.0
    assert bar["total_trades"] == 6200.0
    assert bar["aggregated_venues"] == 4


def test_aggregated_bars_graceful_degradation_missing_venue():
    feed = AggregatedFuturesFeed()
    t0 = 1700000000000
    mock_binance = {
        t0: {
            "close_time_ms": t0 + 899999,
            "open": 80000.0, "high": 80500.0, "low": 79900.0, "close": 80400.0,
            "total_volume": 1000.0, "buy_volume": 550.0, "sell_volume": 450.0,
            "delta": 100.0, "total_trades": 5000.0,
            "has_real_taker": True
        }
    }

    # OKX and Hyperliquid fail / empty
    with patch.object(feed, "fetch_binance", return_value=mock_binance), \
         patch.object(feed, "fetch_okx", return_value={}), \
         patch.object(feed, "fetch_bybit", return_value={}), \
         patch.object(feed, "fetch_hyperliquid", return_value={}):

        bars = feed.fetch_aggregated_bars("BTC", interval="15m", limit=1)

    assert len(bars) == 1
    assert bars[0]["total_volume"] == 1000.0
    assert bars[0]["delta"] == 100.0
    assert bars[0]["aggregated_venues"] == 1


def test_aggregated_bars_binance_failover_to_bybit():
    """Verify that if Binance is down, the feed anchors to surviving venue (Bybit)."""
    feed = AggregatedFuturesFeed()
    t0 = 1700000000000
    mock_bybit = {
        t0: {
            "close_time_ms": t0 + 899999,
            "open": 80010.0, "high": 80520.0, "low": 79910.0, "close": 80410.0,
            "total_volume": 800.0, "buy_volume": 400.0, "sell_volume": 400.0,
            "delta": 0.0, "total_trades": 3000.0,
            "has_real_taker": False
        }
    }
    mock_okx = {
        t0: {
            "close_time_ms": t0 + 899999,
            "open": 80015.0, "high": 80525.0, "low": 79915.0, "close": 80415.0,
            "total_volume": 400.0, "buy_volume": 200.0, "sell_volume": 200.0,
            "delta": 0.0, "total_trades": 1000.0,
            "has_real_taker": False
        }
    }

    # Binance is down
    with patch.object(feed, "fetch_binance", return_value={}), \
         patch.object(feed, "fetch_okx", return_value=mock_okx), \
         patch.object(feed, "fetch_bybit", return_value=mock_bybit), \
         patch.object(feed, "fetch_hyperliquid", return_value={}):

        bars = feed.fetch_aggregated_bars("BTC", interval="15m", limit=1)

    assert len(bars) == 1
    assert bars[0]["total_volume"] == 1200.0  # 800 + 400
    assert bars[0]["open"] == 80010.0
    assert bars[0]["aggregated_venues"] == 2
