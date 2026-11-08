import json, threading, time
from pathlib import Path
import pandas as pd
import pytest
import coinglass_common as cc
import active_assets_manager as manager
import standalone_coinglass_whales_api as whales
import standalone_coinglass_footprint_api as fp
import standalone_coinglass_liquidation_levels_api as liq


def test_parquet_readback_and_schema(tmp_path):
    p = tmp_path / "x.parquet"
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    cc.atomic_parquet(df, p)
    assert cc.verify_parquet(p, ["a", "b"]) == 2
    with pytest.raises(ValueError):
        cc.verify_parquet(p, ["b", "a"])
    with pytest.raises(ValueError):
        cc.verify_parquet(p, min_rows=3)


def test_failed_write_and_fsync_preserve_history(tmp_path, monkeypatch):
    p = tmp_path / "x.parquet"
    old = pd.DataFrame({"a": [7]})
    cc.atomic_parquet(old, p)
    before = p.read_bytes()
    def broken(*a, **k):
        raise OSError("simulated disk failure")
    monkeypatch.setattr(cc.os, "fsync", broken)
    with pytest.raises(OSError):
        cc.atomic_parquet(pd.DataFrame({"a": [8]}), p)
    assert p.read_bytes() == before
    assert not list(tmp_path.glob("*.tmp"))


def test_lock_inode_never_unlinked_on_contention(tmp_path):
    p = tmp_path / "stable.lock"
    with cc.FileMutex(p, timeout=0) as first:
        assert first.acquired
        inode = p.stat().st_ino
        with cc.FileMutex(p, timeout=0.01) as second:
            assert not second.acquired
        assert p.stat().st_ino == inode
    assert p.exists()
    with cc.FileMutex(p, timeout=0) as third:
        assert third.acquired


def test_manager_fails_closed_and_rejects_injection(tmp_path, monkeypatch):
    p = tmp_path / "active.json"
    monkeypatch.setattr(manager, "ACTIVE_ASSETS_FILE", p)
    p.write_text("{broken")
    with pytest.raises(RuntimeError):
        manager.load_active_assets()
    with pytest.raises(RuntimeError):
        manager.save_active_assets(["BTC"])
    assert p.read_text() == "{broken"
    p.write_text('{"active_assets":["BTC"],"total_active_assets":1}')
    manager.save_active_assets(["BTC", "ETH"])
    with pytest.raises(ValueError):
        manager.add_active_asset("../../evil")
    assert manager.load_active_assets() == ["BTC", "ETH"]


def test_manager_concurrent_mutations_no_lost_updates(tmp_path, monkeypatch):
    p = tmp_path / "active.json"
    monkeypatch.setattr(manager, "ACTIVE_ASSETS_FILE", p)
    manager.save_active_assets(["BTC"])
    errors = []
    def worker(i):
        try:
            manager.add_active_asset(f"ASSET{i}")
        except Exception as exc:
            errors.append(exc)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert not errors
    assert len(manager.load_active_assets()) == 13


def whale_dataset(n=1):
    now = "2026-11-08T00:00:00+00:00"
    orders = [whales.WhaleOrder("BINANCE Ask", "$100.00", "1%", "$500", "$500", "$0", "1s", "Open",
                                100.0, 500.0, 500.0, 0.0, 0, str(i), "bn-perp", "ask", 0, now, now)
              for i in range(n)]
    return whales.WhaleDataset("BTC", 99.0, now, now, orders, n, n, 0, 500.0*n, 0.0)


def test_whale_empty_and_corrupt_never_erase(tmp_path):
    repo = whales.ParquetWhaleRepository(tmp_path)
    p = repo.persist_dataset(whale_dataset())
    before = p.read_bytes()
    with pytest.raises(ValueError): repo.persist_dataset(whale_dataset(0))
    assert p.read_bytes() == before
    p.write_bytes(b"corrupt")
    with pytest.raises(Exception): repo.persist_dataset(whale_dataset())
    assert p.read_bytes() == b"corrupt"


def liq_payload():
    out = {}
    for tf in ("3D", "7D", "30D"):
        data = {"horizon": tf}
        for structure, (columns, key) in liq.STRUCTURE_SCHEMAS.items():
            row = {c: (1.0 if c not in {"Side","Distance","Leverage","Band","Tier","Price_Range","Bias","Interpretation","Status","Short_Share_Pct","Long_Share_Pct","Long_Pct","Short_Pct","Band_Width"} else "x") for c in columns[2:]}
            data[key] = row if structure == "NearFieldBalance" else [row]
        out[tf] = data
    return out


def test_liq_15_files_and_empty_preservation(tmp_path, monkeypatch):
    monkeypatch.setattr(liq, "LIQ_DUMP_DIR", tmp_path)
    assert liq.persist_asset_liquidation("BTC", liq_payload()) == 15
    assert liq.is_asset_dump_complete("BTC")
    assert len(list((tmp_path / "BTC").glob("*.parquet"))) == 15
    target = tmp_path / "BTC" / "3D_BTC_KeyLiquidationLevel.parquet"
    before = target.read_bytes()
    bad = liq_payload(); bad["3D"]["keyLevels"] = []
    with pytest.raises(ValueError): liq.persist_asset_liquidation("BTC", bad)
    assert target.read_bytes() == before
    bad = liq_payload(); bad["7D"]["horizon"] = "3D"
    with pytest.raises(ValueError): liq.persist_asset_liquidation("BTC", bad)


def test_footprint_merge_rebases_cvd_and_adds_profile(tmp_path):
    payload = fp.generate_mock_footprint_payload("BTC")
    datasets = fp.FootprintDomainService.build_datasets(payload, "BTC")
    result = fp.FootprintDumpAdapter.dump_three_datasets(datasets, tmp_path)
    p = Path(result["15Min_time"]["parquet"])
    old = pd.read_parquet(p)
    assert len(old) == 20
    # Legacy 16-column history must migrate without deleting rows.
    old[fp.EXCEL_SPEC_COLUMNS[:16]].to_parquet(p, index=False)
    fp.FootprintDumpAdapter.dump_three_datasets(datasets, tmp_path)
    merged = pd.read_parquet(p)
    assert len(merged) == 20
    assert list(merged.columns) == fp.EXCEL_SPEC_COLUMNS
    assert merged.Candle_CVD.tolist() == pytest.approx(merged.Candle_Delta.cumsum().tolist())
