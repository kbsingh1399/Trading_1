"""
Unit tests for DeepSeek Harness (dsh) Antigravity Intelligence Bridge
"""

import os
import sys
import json
import time
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from Terminal.deepseek_harness_bridge import (
    DeepSeekHarnessBridge,
    create_win32_kill_on_close_job,
    assign_process_to_job,
    terminate_process_tree,
    DSH_HOME_DIR,
    CACHE_DIR
)


def test_dsh_directories_exist():
    assert DSH_HOME_DIR.exists()
    assert CACHE_DIR.exists()
    assert ".dsh_home" in str(DSH_HOME_DIR)


def test_win32_job_object_creation():
    job = create_win32_kill_on_close_job()
    if sys.platform == "win32":
        assert job is not None
        # Clean up handle
        import ctypes
        ctypes.windll.kernel32.CloseHandle(job)
    else:
        assert job is None


def test_unconfigured_bridge_behavior():
    bridge = DeepSeekHarnessBridge(api_key="")
    assert not bridge.is_configured()

    success, msg, latency = bridge.call_deepseek_api([{"role": "user", "content": "hi"}])
    assert not success
    assert msg == "DEEPSEEK_API_KEY_NOT_CONFIGURED"

    critique = bridge.evaluate_signal_confluence({
        "symbol": "BTCUSD",
        "direction": "BUY",
        "confluence_score": 85.0,
        "signal_details": {"title": "Test Absorption"},
        "confluence_reasons": ["Test Reason"]
    })
    assert critique["ok"] is False
    assert critique["status"] == "REJECTED_MISSING_API_KEY"
    assert critique["approved"] is False
    bridge.close()


def test_bridge_timeout_clamping():
    bridge = DeepSeekHarnessBridge(api_key="mock", timeout_seconds=60.0)
    assert bridge.timeout_seconds <= 5.0
    bridge.close()


def test_bridge_child_process_containment():
    bridge = DeepSeekHarnessBridge(api_key="mock")
    proc = bridge.spawn_contained_process([sys.executable, "-c", "import time; time.sleep(10)"])
    assert proc.pid > 0
    bridge.terminate_children()
    assert len(bridge._active_children) == 0
    bridge.close()


def test_configured_bridge_success_critique():
    bridge = DeepSeekHarnessBridge(api_key="mock_key_123")
    assert bridge.is_configured()

    mock_resp_json = {
        "choices": [
            {
                "message": {
                    "content": '```json\n{"approved": true, "confidence": 0.92, "reason": "Strong order flow alignment"}\n```'
                }
            }
        ]
    }

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_cm = MagicMock()
        mock_cm.read.return_value = json.dumps(mock_resp_json).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_cm

        critique = bridge.evaluate_signal_confluence({
            "symbol": "ARUSD",
            "direction": "SELL",
            "confluence_score": 82.0,
            "signal_details": {"title": "Rally without buying"},
            "confluence_reasons": ["Bearish CVD", "Below VWAP"]
        })

        assert critique["ok"] is True
        assert critique["status"] == "CRITIQUE_COMPLETE"
        assert critique["approved"] is True
        assert critique["confidence"] == 0.92
        assert "Strong order flow" in critique["reason"]
        assert critique["latency_ms"] >= 0.0

    bridge.close()


def test_configured_bridge_veto_critique():
    bridge = DeepSeekHarnessBridge(api_key="mock_key_123")

    mock_resp_json = {
        "choices": [
            {
                "message": {
                    "content": '{"approved": false, "confidence": 0.88, "reason": "Severe resistance wall at entry"}'
                }
            }
        ]
    }

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_cm = MagicMock()
        mock_cm.read.return_value = json.dumps(mock_resp_json).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_cm

        critique = bridge.evaluate_signal_confluence({
            "symbol": "HBARUSD",
            "direction": "BUY",
            "confluence_score": 76.0,
            "signal_details": {"title": "Absorption at low"},
            "confluence_reasons": ["Absorption"]
        })

        assert critique["ok"] is True
        assert critique["status"] == "CRITIQUE_COMPLETE"
        assert critique["approved"] is False
        assert critique["confidence"] == 0.88
        assert "resistance wall" in critique["reason"]

    bridge.close()


def test_bridge_timeout_fails_closed():
    bridge = DeepSeekHarnessBridge(api_key="mock_key_123", timeout_seconds=0.1)

    with patch("urllib.request.urlopen", side_effect=TimeoutError("Request timed out")):
        critique = bridge.evaluate_signal_confluence({
            "symbol": "TRXUSD",
            "direction": "BUY",
            "confluence_score": 80.0
        })

        assert critique["ok"] is False
        assert critique["status"] == "CRITIC_TIMEOUT_OR_ERROR"
        assert critique["approved"] is False
        assert "TimeoutError" in critique["reason"]

    bridge.close()


def test_context_manager_cleanup():
    with DeepSeekHarnessBridge(api_key="mock") as bridge:
        assert bridge is not None
        job = bridge.job_handle
    # On exit, job_handle should be cleared
    assert bridge.job_handle is None
