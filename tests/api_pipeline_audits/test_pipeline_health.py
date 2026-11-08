import asyncio, json, time, threading
from pathlib import Path
import pytest
import run_parallel_standalone as runner
import coinglass_common as cc


def test_emit_pipeline_health(tmp_path, monkeypatch):
    health_file = tmp_path / "pipeline_health.json"
    monkeypatch.setattr(runner, "HEALTH_FILE", health_file)
    stats = {
        "footprint": {"dumped_files": 42, "expected_files": 42, "status": "HEALTHY"},
        "whales": {"dumped_assets": 14, "expected_assets": 14, "status": "HEALTHY"},
        "liquidation_levels": {"dumped_files": 210, "expected_files": 210, "status": "HEALTHY"},
        "live_legend": {"dumped_assets": 14, "expected_assets": 14, "status": "HEALTHY"}
    }
    path = runner.emit_pipeline_health(
        cycle_num=1,
        current_ip="1.2.3.4",
        cycles_on_ip=1,
        ip_age_sec=120.5,
        pillar_stats=stats,
        symbols=["BTC", "ETH"],
        is_degraded=False
    )
    assert path == health_file
    assert health_file.exists()
    data = json.loads(health_file.read_text(encoding="utf-8"))
    assert data["cycle_number"] == 1
    assert data["current_ip"] == "1.2.3.4"
    assert data["status"] == "HEALTHY"
    assert data["cycles_on_current_ip"] == 1
    assert data["pillars"]["footprint"]["dumped_files"] == 42
    assert "timestamp_ist" in data
    assert "timestamp_epoch" in data


def test_emit_pipeline_health_degraded(tmp_path, monkeypatch):
    health_file = tmp_path / "pipeline_health.json"
    monkeypatch.setattr(runner, "HEALTH_FILE", health_file)
    stats = {
        "footprint": {"dumped_files": 0, "expected_files": 42, "status": "FAILED"},
        "whales": {"dumped_assets": 14, "expected_assets": 14, "status": "HEALTHY"}
    }
    path = runner.emit_pipeline_health(
        cycle_num=2,
        current_ip="5.6.7.8",
        cycles_on_ip=2,
        ip_age_sec=300.0,
        pillar_stats=stats,
        symbols=["BTC"],
        is_degraded=True
    )
    data = json.loads(health_file.read_text(encoding="utf-8"))
    assert data["status"] == "DEGRADED"


def test_runner_inter_cycle_ceiling_rotation(tmp_path, monkeypatch):
    health_file = tmp_path / "pipeline_health.json"
    monkeypatch.setattr(runner, "HEALTH_FILE", health_file)

    async def ok(n):
        async def harvest(chunk, idx, signals):
            return len(chunk) * n
        return harvest

    monkeypatch.setattr(runner, "harvest_footprint_chunk", asyncio.run(ok(3)))
    monkeypatch.setattr(runner, "harvest_whale_chunk", asyncio.run(ok(1)))
    monkeypatch.setattr(runner, "harvest_liquidation_chunk", asyncio.run(ok(15)))
    monkeypatch.setattr(runner, "get_current_ip", lambda: "1.1.1.1")

    rotations = []
    def rotate(context, reason):
        rotations.append((context, reason))
        return "2.2.2.2"
    monkeypatch.setattr(runner, "rotate_warp_ip", rotate)

    cycles_seen = []
    orig_sleep = asyncio.sleep
    async def fast_sleep(sec):
        cycles_seen.append(len(rotations))
        if len(cycles_seen) >= 2:
            raise asyncio.CancelledError("Stop test after 2 cycles")
        await orig_sleep(0.01)
    monkeypatch.setattr(asyncio, "sleep", fast_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner.run_parallel_cycle(
            chunk_size=1,
            symbols_override=["BTC"],
            continuous=True,
            delay_between_cycles=0.01,
            max_cycles_per_ip=2,
            max_ip_age_min=60.0
        ))

    # After cycle 1: no rotation (cycles_on_ip = 1 < 2)
    # After cycle 2: rotation triggered with CYCLE_AGE_CEILING
    assert len(rotations) == 1
    assert rotations[0][1] == "CYCLE_AGE_CEILING"
    assert "Cycle2_Ceiling" in rotations[0][0]
