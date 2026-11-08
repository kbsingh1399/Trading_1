import asyncio, json
from pathlib import Path
import pytest
import coinglass_common as cc
import standalone_coinglass_footprint_api as fp
import run_parallel_standalone as runner


def test_scheduled_rotation_never_calls_cli(monkeypatch):
    monkeypatch.setattr(cc, "_warp_command", lambda *a, **k: pytest.fail("CLI called"))
    monkeypatch.setattr(cc, "get_current_ip", lambda: "1.2.3.4")
    for reason in ("SCHEDULED", "NORMAL", "RATE_LIMITED", "EMPTY_SCRAPE"):
        assert cc.rotate_warp_ip(reason=reason, force=True) == "1.2.3.4"


def warp_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(cc, "LOCK_DIR", tmp_path)
    monkeypatch.setattr(cc, "WARP_ROTATION_FILE", tmp_path / "last.json")
    cli = tmp_path / "warp-cli.exe"; cli.write_text("")
    monkeypatch.setattr(cc, "WARP_CLI", cli)
    monkeypatch.setattr(cc, "get_current_ip", lambda: "old")
    monkeypatch.setattr(cc, "is_warp_connected", lambda: True)
    monkeypatch.setattr(cc, "verify_network_stabilization", lambda *a, **k: (True, "new"))
    calls = []
    def command(*args, **kwargs):
        calls.append(args)
        if args == ("tunnel", "dump"): return "Split tunnel mode: exclude\n"
        if args == ("tunnel", "host", "list"): return "\n".join(cc.DELTA_EXCLUSION_HOSTS)
        return "OK"
    monkeypatch.setattr(cc, "_warp_command", command)
    return calls


def test_reactive_full_registration_sequence(tmp_path, monkeypatch):
    calls = warp_fixture(tmp_path, monkeypatch)
    assert cc.rotate_warp_ip(reason="HTTP_429", force=True) == "new"
    core = [c for c in calls if c[0] in {"disconnect","registration","connect"}]
    assert core == [("disconnect",), ("registration","delete"), ("registration","new"), ("connect",)]
    assert cc._get_last_rotation_time() > 0


def test_cycle_age_ceiling_is_authorized_rotation_reason(tmp_path, monkeypatch):
    calls = warp_fixture(tmp_path, monkeypatch)
    assert cc.rotate_warp_ip(reason="CYCLE_AGE_CEILING", force=True) == "new"
    core = [c for c in calls if c[0] in {"disconnect", "registration", "connect"}]
    assert core == [("disconnect",), ("registration", "delete"), ("registration", "new"), ("connect",)]
    assert cc._get_last_rotation_time() > 0


def test_order_inflight_blocks_rotation(tmp_path, monkeypatch):
    calls = warp_fixture(tmp_path, monkeypatch)
    with cc.FileMutex(tmp_path / "order_execution_inflight.lock") as guard:
        assert guard.acquired
        with pytest.raises(RuntimeError, match="order execution"):
            cc.rotate_warp_ip(reason="CHALLENGE")
    assert not calls


def test_include_mode_fails_closed_and_restores(tmp_path, monkeypatch):
    calls = warp_fixture(tmp_path, monkeypatch)
    def include(*args, **kwargs):
        calls.append(args)
        if args == ("tunnel","dump"): return "Split tunnel mode: include"
        if args == ("tunnel","host","list"): return "\n".join(cc.DELTA_EXCLUSION_HOSTS)
        return "OK"
    monkeypatch.setattr(cc, "_warp_command", include)
    with pytest.raises(RuntimeError, match="Exclude mode"):
        cc.rotate_warp_ip(reason="HTTP_429")
    assert ("registration","delete") not in calls
    assert calls[-1] == ("tunnel","host","list")
    assert ("connect",) in calls
    assert cc._get_last_rotation_time() == 0


def test_ownership_exact_query_not_substring():
    owned = cc.bot_tab_url("https://www.coinglass.com/footprint/BTC?x=bot=whales_api", "footprint_api")
    assert cc.is_process_bot_tab({"url": owned}, "footprint_api")
    assert not cc.is_process_bot_tab({"url": "https://evil.test/?bot=footprint_api&owner="+cc.CDP_OWNER_ID}, "footprint_api")
    assert not cc.is_process_bot_tab({"url": owned.replace(cc.CDP_OWNER_ID,"other")}, "footprint_api")
    assert not cc.is_process_bot_tab({"url": "https://www.coinglass.com/footprint/BTC?note=bot=footprint_api"}, "footprint_api")


def test_global_runner_reaper_is_noop(monkeypatch):
    monkeypatch.setattr(runner.urllib.request, "urlopen", lambda *a, **k: pytest.fail("global tab access"))
    runner.cleanup_scraper_tabs()


def test_footprint_owned_tab_closed_on_error(monkeypatch):
    adapter = fp.CDPBrowserAdapter()
    closed = []
    monkeypatch.setattr(adapter, "get_or_create_footprint_tab", lambda sym: ("owned", "ws://owned"))
    monkeypatch.setattr(adapter, "close_tab", lambda tid: closed.append(tid))
    async def fail(*a, **k): raise RuntimeError("hydration failed")
    monkeypatch.setattr(adapter, "hook_terminal", fail)
    with pytest.raises(RuntimeError): asyncio.run(adapter.fetch_footprint_data("BTC"))
    assert closed == ["owned"]


class FakeWS:
    def __init__(self): self.sent=[]; self.n=0
    async def send(self, raw): self.sent.append(json.loads(raw))
    async def recv(self):
        self.n += 1
        if self.n == 1: return json.dumps({"method":"Network.event"})
        return json.dumps({"id":self.sent[-1]["id"],"result":{"result":{"value":42}}})


def test_cdp_filters_events_and_unique_ids():
    async def run():
        ws=FakeWS(); assert await cc.cdp_eval(ws,"1") == 42; assert await cc.cdp_eval(ws,"2") == 42
        assert ws.sent[0]["id"] != ws.sent[1]["id"]
    asyncio.run(run())


def test_runner_rotates_only_after_all_pillars_close(monkeypatch):
    import threading
    active = {"n": 0}
    observed = []
    lock = threading.Lock()
    async def pillar(kind):
        async def harvest(chunk, idx, signals):
            with lock: active["n"] += 1
            try:
                await asyncio.sleep(0.01)
                if kind == "whale": signals.add("HTTP_429")
                return len(chunk) * (3 if kind == "footprint" else 15 if kind == "liq" else 1)
            finally:
                with lock: active["n"] -= 1
        return harvest
    monkeypatch.setattr(runner, "harvest_footprint_chunk", asyncio.run(pillar("footprint")))
    monkeypatch.setattr(runner, "harvest_whale_chunk", asyncio.run(pillar("whale")))
    monkeypatch.setattr(runner, "harvest_liquidation_chunk", asyncio.run(pillar("liq")))
    monkeypatch.setattr(runner, "get_current_ip", lambda: "offline")
    def rotate(context, reason):
        observed.append((active["n"], reason, threading.get_ident()))
        return "new"
    monkeypatch.setattr(runner, "rotate_warp_ip", rotate)
    asyncio.run(runner.run_parallel_cycle(chunk_size=2, symbols_override=["BTC","ETH"], rotate_chunks=True))
    assert observed == [(0, "RATE_LIMIT_BANNER", observed[0][2])]
    assert observed[0][2] != threading.get_ident()  # asyncio.to_thread, not event-loop blocking


def test_runner_scheduled_flag_alone_never_rotates(monkeypatch):
    async def ok(n):
        async def harvest(chunk, idx, signals): return len(chunk)*n
        return harvest
    monkeypatch.setattr(runner, "harvest_footprint_chunk", asyncio.run(ok(3)))
    monkeypatch.setattr(runner, "harvest_whale_chunk", asyncio.run(ok(1)))
    monkeypatch.setattr(runner, "harvest_liquidation_chunk", asyncio.run(ok(15)))
    monkeypatch.setattr(runner, "get_current_ip", lambda: "offline")
    monkeypatch.setattr(runner, "rotate_warp_ip", lambda *a, **k: pytest.fail("scheduled rotation"))
    asyncio.run(runner.run_parallel_cycle(chunk_size=1, symbols_override=["BTC","ETH"], rotate_chunks=True))
