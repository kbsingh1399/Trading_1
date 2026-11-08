import json, os, subprocess
from pathlib import Path
import pytest
import standalone_coinglass_whales_api as whales
import standalone_coinglass_liquidation_levels_api as liq
import standalone_coinglass_footprint_api as fp
HARNESS=Path(__file__).with_name("jsdom_harness.cjs")

def run(kind, code, mode="normal"):
    env=os.environ.copy(); env.setdefault("NODE_PATH", str(Path(__file__).resolve().parents[2] / "node_modules"))
    p=subprocess.run(["node",str(HARNESS)],input=json.dumps({"kind":kind,"code":code,"mode":mode}),text=True,capture_output=True,env=env,timeout=30)
    assert p.returncode==0,p.stderr
    return json.loads(p.stdout)

def test_whale_zero_threshold_keeps_midcap_and_zero():
    out=run("whale",whales.WHALE_EXTRACTION_JS)
    r=out["result"]; assert r["success"] and r["complete"] and r["threshold_usd"]==0
    assert out["clicks"]>0
    assert {o["raw_remaining_usd"] for o in r["orders"]}=={2000000,250,0}
    assert r["orders"][1]["status"]=="Partially Filled"

def test_whale_empty_is_not_success():
    r=run("whale",whales.WHALE_EXTRACTION_JS,"empty")["result"]
    assert not r["success"] and r["error"]=="empty_unverified"

def test_whale_dom_suffix_and_partial_flag():
    r=run("whale",whales.WHALE_EXTRACTION_JS,"dom")["result"]
    assert r["method"]=="dom_fallback" and r["orders"][0]["raw_initial_usd"]==1200
    assert not r["complete"]  # no scroll container: cannot prove virtualized completeness

def test_liq_decimal_suffixes_filters_and_horizons():
    r=run("liq",liq.EXTRACTION_JS)["result"]
    assert not r["isRateLimited"] and set(r["allData"])=={"3D","7D","30D"}
    d=r["allData"]["3D"]
    assert d["horizon"]=="3D" and len(d["keyLevels"])==2
    assert [x["Strength"] for x in d["keyLevels"]]==[1200000,750500]
    assert d["strengthByDistance"][0]["Total_Strength"]==1950500
    assert d["leverageStrength"][0]["Total_Strength"]==3750000
    assert d["nearField"]["Long_Strength"]==1250000
    assert d["magnetZones"][0]["Strength"]==12500

def test_liq_unverified_horizon_rejected():
    r=run("liq",liq.EXTRACTION_JS,"unverified")["result"]
    assert r["error"].startswith("horizon selection unverified") and r["allData"]=={}

def test_footprint_forces_15m_and_computes_profile():
    out=run("fp",fp.EXTRACT_MULTI_BAR_FOOTPRINT_JS)
    assert out["itv"]=="15m"
    r=out["result"]; assert len(r["sets"])==3
    bar=r["sets"][0]["bars"][0]
    assert (bar["poc"],bar["val"],bar["vah"])==(101,101,102)
    assert bar["cvd"]==9

def test_footprint_stale_loading_rejected():
    r=run("fp",fp.EXTRACT_MULTI_BAR_FOOTPRINT_JS,"stale")["result"]
    assert "hydration unverified" in r["error"]
