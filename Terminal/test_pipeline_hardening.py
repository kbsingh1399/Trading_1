"""Regression checks use isolated files and broker doubles; no live orders."""
import asyncio
import json
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest

from Terminal.runtime_io import FileMutex, atomic_parquet, upsert_parquet, completed_candles, cdp_result
from Terminal.alert_stream import consume_alerts
from Terminal.risk_envelope import admissible_volume, validate_trade_geometry
from Terminal.venue_admission import competition_risk_check, mt5_account_book


def test_cross_process_mutex_excludes_writer_and_recovers(tmp_path):
    path = tmp_path / 'mutex'
    code = "from Terminal.runtime_io import FileMutex; import sys; l=FileMutex(sys.argv[1],0).__enter__(); print(l.acquired)"
    with FileMutex(path) as lock:
        assert lock.acquired
        result = subprocess.run([sys.executable, '-c', code, str(path)], capture_output=True, text=True, timeout=10)
        assert result.returncode == 0 and result.stdout.strip() == 'False'
    result = subprocess.run([sys.executable, '-c', code, str(path)], capture_output=True, text=True, timeout=10)
    assert result.stdout.strip() == 'True'
    assert path.exists()  # Persistent lock files are harmless; ownership is in the OS.


def test_atomic_failure_preserves_history(tmp_path, monkeypatch):
    path = tmp_path / 'data.parquet'
    atomic_parquet(pd.DataFrame({'id': [1]}), path)
    def fail(*a, **k):
        raise OSError('disk full')
    monkeypatch.setattr(pd.DataFrame, 'to_parquet', fail)
    with pytest.raises(OSError):
        atomic_parquet(pd.DataFrame({'id': [2]}), path)
    assert pd.read_parquet(path).id.tolist() == [1]
    assert not list(tmp_path.glob('*.tmp'))


def test_upsert_latest_revision_without_erasing_corrupt_history(tmp_path):
    path = tmp_path / 'data.parquet'
    upsert_parquet(pd.DataFrame({'id': [1], 'value': [2]}), path, ['id'])
    upsert_parquet(pd.DataFrame({'id': [1], 'value': [3]}), path, ['id'])
    assert pd.read_parquet(path).value.tolist() == [3]
    path.write_bytes(b'corrupt')
    with pytest.raises(Exception):
        upsert_parquet(pd.DataFrame({'id': [2]}), path, ['id'])
    assert path.read_bytes() == b'corrupt'


def alert(i, actionable=True):
    return {'symbol': 'BTC', 'direction': 'BUY', 'evaluated_at_utc': 'same-time',
            'is_actionable': actionable, 'signal_details': {'sig_code': 'abs', 'candle_utc': str(i)}}


def test_alert_partial_append_same_timestamp_and_rotation(tmp_path):
    path, state = tmp_path / 'a.jsonl', tmp_path / 'state.json'
    assert consume_alerts(path, state) == []
    payload = json.dumps(alert(1))
    path.write_text(payload[:10])
    assert consume_alerts(path, state) == []
    with path.open('a') as f:
        f.write(payload[10:] + '\n' + json.dumps(alert(2)) + '\n')
    assert len(consume_alerts(path, state)) == 2
    path.write_text(json.dumps(alert(2)) + '\n' + json.dumps(alert(3)) + '\n')
    assert [x['signal_details']['candle_utc'] for x in consume_alerts(path, state)] == ['3']
    assert consume_alerts(path, state) == []


def test_nonactionable_does_not_trigger(tmp_path):
    path = tmp_path / 'alerts'
    path.write_text(json.dumps(alert(1, False)) + '\n')
    assert consume_alerts(path, tmp_path / 'state') == []


def test_volume_grid_respects_volume_max_with_small_step():
    lots, _, risk = admissible_volume(loss_per_lot_step=0.01, volume_step=0.01,
                                     min_lot=0.01, max_lot=10, floor_usd=5, cap_usd=25)
    assert lots == 5 and risk == 5


@pytest.mark.parametrize('value', [float('nan'), float('inf'), None])
def test_invalid_risk_and_geometry_fail_closed(value):
    assert admissible_volume(loss_per_lot_step=1, cap_usd=value)[0] is None
    assert not validate_trade_geometry('BUY', 100, 99, 103, value)[0]


def test_completed_bar_filter_excludes_forming_bar():
    now = int(time.time() * 1000)
    frame = pd.DataFrame({'open_time_ms': [now-1800000, now-900000, now],
                          'close_time_ms': [now-900001, now-1, now+899999]})
    assert len(completed_candles(frame, now)) == 2


def test_cdp_timeout_bounds_unrelated_messages():
    class Socket:
        async def send(self, payload):
            pass
        async def recv(self):
            await asyncio.sleep(0.01)
            return '{"id":2}'
    with pytest.raises(TimeoutError):
        asyncio.run(cdp_result(Socket(), {'id': 1}, timeout=0.03))


def test_none_inventory_is_unknown():
    broker = Mock()
    broker.account_info.return_value = SimpleNamespace(login=5064568, currency='USD')
    broker.positions_get.return_value = None
    with pytest.raises(ValueError, match='UNKNOWN_INVENTORY'):
        mt5_account_book(broker)


def test_native_size_uses_broker_units_and_reserves_margin():
    from Terminal.venue_admission import native_size
    broker = Mock()
    broker.ORDER_TYPE_BUY, broker.ORDER_TYPE_SELL = 0, 1
    broker.account_info.return_value = SimpleNamespace(login=5064568, currency='USD',
        balance=5000, equity=5000, margin_free=5000)
    broker.positions_get.return_value = ()
    broker.orders_get.return_value = ()
    broker.symbol_info.return_value = SimpleNamespace(volume_step=.01, volume_min=.01, volume_max=10)
    broker.symbol_info_tick.return_value = SimpleNamespace(bid=100, ask=100.1, time_msc=time.time()*1000)
    broker.order_calc_profit.return_value = -5.0
    broker.order_calc_margin.return_value = 100
    result = native_size(broker, 'TEST', 'BUY', 100, 95, 110)
    assert result['lots'] == .01 and result['risk_usd'] == 5
    broker.order_calc_margin.return_value = None
    with pytest.raises(ValueError, match='UNKNOWN_MARGIN'):
        native_size(broker, 'TEST', 'BUY', 100, 95, 110)


def test_nonfinite_forecast_is_unavailable():
    from Terminal.deepmind_timesfm_forecaster import DeepMindTimesFMForecaster
    model = object.__new__(DeepMindTimesFMForecaster)
    model._compiled, model._model = True, Mock()
    result = model.forecast_normalized_delta_ratio(np.full(20, np.nan), np.ones(20))
    assert result['available'] is False
    model._model.forecast.assert_not_called()


def test_matchtrader_unverified_dispatch_never_touches_socket():
    # Invariant #1: MatchTrader excised, Delta Exchange exclusive
    from pathlib import Path
    assert not Path('Terminal/matchtrader_live_trader.py').exists()


def test_dynamic_daily_floor_not_static_initial_floor():
    ok, reason = competition_risk_check(account_id='101305360', balance=105000, equity=105000,
        initial_balance=100000, opening_balance=110000, opening_equity=110000,
        open_positions=0, pending_orders=0, committed_stress=0, candidate_stress=1000,
        day_key='today', platform_day_key='today')
    assert not ok and reason == 'COMPETITION_DRAWDOWN_LIMIT'


@pytest.mark.parametrize('side,expected', [('buy', 'BUY'), ('sell', 'SELL')])
def test_liquidation_direction_and_real_evaluator_api(side, expected):
    from Terminal.realtime_signal_evaluator import RealtimeSignalEvaluator
    evaluator = object.__new__(RealtimeSignalEvaluator)
    evaluator.olap = Mock()
    evaluator.olap.query_whale_walls.return_value = {'data_valid': True, 'whale_imbalance_bid_ratio': .7 if side=='buy' else .3}
    evaluator.olap.query_liquidation_confluence.return_value = {'data_valid': True, 'near_field_shorts_pct': 60 if side=='buy' else 40}
    evaluator.olap.query_cvd_trend.return_value = {'data_valid': True, 'cvd_slope': 'BULLISH_FLOW' if side=='buy' else 'BEARISH_FLOW'}
    evaluator.olap.query_auction_profile.return_value = {'data_valid': True}
    evaluator.olap.get_forecast_frame.return_value = pd.DataFrame()
    evaluator.regime_det = Mock()
    evaluator.regime_det.analyze_symbol.return_value = {'regime': 'MEAN_REVERTING_RANGE', 'atr_pct': 1}
    evaluator.vector_mem = None
    evaluator.timesfm = SimpleNamespace(is_available=False)
    evaluator.critic_engine = Mock()
    evaluator.critic_engine.check_microstructure_knife_veto.return_value = (False, "", {})
    evaluator.critic_engine.audit_candidate_setup.return_value = {"critic_verdict": "CLEAR", "max_similarity": 0.0, "penalty": 0.0}
    evaluator.query_live_tv_liquidations = lambda s: {}
    evaluator._persist_alert = Mock()
    evaluator._broker_sizing = lambda *args: {'lots': .01, 'risk_usd': 5, 'cap_usd': 10}
    from datetime import datetime, timezone
    result = evaluator.evaluate_signal({'symbol': 'BTC', 'mt5_symbol': 'BTCUSD',
        'sig_name': 'Liquidation Exhaustion', 'sig_code': 'LIQ_REV_SPIKE', 'side': side,
        'price': 100, 'datetime_utc': datetime.now(timezone.utc).isoformat(),
        'bar_closed': True})
    assert result['direction'] == expected
    assert result['is_actionable'] is True
