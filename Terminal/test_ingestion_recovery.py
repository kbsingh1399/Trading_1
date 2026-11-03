"""Provider quota, conversion and source-isolation regression coverage."""
import io
import json
from urllib.error import HTTPError
from unittest.mock import Mock

import pandas as pd
import pytest
from Terminal import coinalyze_rest_collector as rest


def test_quota_counts_symbols_and_waits_full_window(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(rest, 'DB_DIR', tmp_path)
    monkeypatch.setattr(rest.time, 'time', lambda: clock[0])
    waits = []
    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds + .01
    monkeypatch.setattr(rest.time, 'sleep', sleep)
    rest._reserve_api_cost(20)
    rest._reserve_api_cost(20)
    rest._reserve_api_cost(1)
    assert waits and waits[0] >= 61


def test_retry_after_not_truncated_and_key_only_in_header(tmp_path, monkeypatch):
    monkeypatch.setattr(rest, 'DB_DIR', tmp_path)
    clock = [1000.0]
    monkeypatch.setattr(rest.time, 'time', lambda: clock[0])
    waits = []
    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds + .01
    monkeypatch.setattr(rest.time, 'sleep', sleep)
    response = io.BytesIO(b'[]')
    provider = Mock(side_effect=[HTTPError('redacted', 429, 'limit', {'Retry-After': '57'}, None), response])
    monkeypatch.setattr(rest.urllib.request, 'urlopen', provider)
    assert rest.fetch_coinalyze_json('liquidation-history', {'symbols': 'BTC'}) == []
    assert waits[0] >= 57
    assert rest.API_KEY not in provider.call_args[0][0].full_url


def test_usd_notional_is_not_multiplied_by_price_again(tmp_path, monkeypatch):
    monkeypatch.setattr(rest, 'LIVE_LIQ_DIR', tmp_path)
    result = rest.process_symbol_liquidations('BTC', 'BTCUSDT_PERP.A',
        [{'t': 1793603700, 'l': 120000, 's': 20000, '_usd': True}], 80000, {})
    assert result['latest_total_liq_usd'] == 140000
    assert not (tmp_path / 'BTC_tv_liquidations.parquet').exists()
    frame = pd.read_parquet(tmp_path / 'BTC_coinalyze_liquidations.parquet')
    assert frame.source.iloc[0] == 'Coinalyze_REST_API'


def test_unknown_historical_conversion_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(rest, 'LIVE_LIQ_DIR', tmp_path)
    with pytest.raises(ValueError, match='conversion'):
        rest.process_symbol_liquidations('BTC', 'BTCUSDT_PERP.A',
            [{'t': 1793603700, 'l': 1, 's': 2}], 80000, {})


def test_bad_rest_notional_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(rest, 'LIVE_LIQ_DIR', tmp_path)
    with pytest.raises(ValueError):
        rest.process_symbol_liquidations('BTC', 'BTCUSDT_PERP.A',
            [{'t': 1793603700, 'l': float('nan'), 's': 2, '_usd': True}], 80000, {})


def test_tv_quote_failure_does_not_publish_zero_as_market_price(tmp_path, monkeypatch):
    from Terminal import coinglass_tv_liquidation_collector as tv
    monkeypatch.setattr(tv, 'LIVE_LIQ_DIR', tmp_path)
    tv.process_and_persist_liquidations('BTC', 'Binance_BTCUSDT', [[1793603700, 100, 200]], 0)
    frame = pd.read_parquet(tmp_path / 'BTC_tv_liquidations.parquet')
    assert pd.isna(frame.close_price.iloc[-1])
    assert not bool(frame.price_available.iloc[-1])
    assert frame.source.iloc[-1] == 'CoinGlass_TradingView'
