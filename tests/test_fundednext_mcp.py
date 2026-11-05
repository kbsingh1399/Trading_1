"""
Unit and Integration Tests for FundedNext MCP Server Connection
Verifies live JSON-RPC handshake, account retrieval, risk cushion parsing, and stdio bridge.
"""

import pytest
import subprocess
import json
from fundednext_mcp_client import FundedNextMCPClient

def test_mcp_handshake():
    client = FundedNextMCPClient()
    init_res = client.initialize()
    assert "serverInfo" in init_res
    assert init_res["serverInfo"]["name"] == "FundedNext MCP"

def test_mcp_get_accounts():
    client = FundedNextMCPClient()
    accounts = client.get_accounts()
    assert len(accounts) >= 1
    acc = accounts[0]
    assert acc["login"] == 11808165
    assert "Stellar Lite" in acc["type"]
    assert acc["breached"] == 0

def test_mcp_risk_cushion():
    client = FundedNextMCPClient()
    cushion = client.get_risk_cushion()
    assert cushion["balance"] > 4000
    assert cushion["equity"] > 4000
    assert cushion["effective_risk_buffer"] > 0
    assert cushion["daily_loss_remaining"] > 0

def test_mcp_risk_governor_validation():
    client = FundedNextMCPClient()
    # $12.50 risk should pass
    ok, msg = client.validate_order_risk(12.50)
    assert ok is True
    assert "APPROVE" in msg

    # $200 risk should fail against $98 cushion
    ok_fail, msg_fail = client.validate_order_risk(200.0)
    assert ok_fail is False
    assert "VETO" in msg_fail

def test_mcp_stdio_bridge():
    proc = subprocess.Popen(
        ["python", "fundednext_mcp_bridge.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    init_req = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-bridge", "version": "1.0"}
        }
    }
    proc.stdin.write(json.dumps(init_req) + "\n")
    proc.stdin.flush()
    line = proc.stdout.readline()
    proc.terminate()
    
    assert line.strip() != ""
    res = json.loads(line)
    assert res.get("result", {}).get("serverInfo", {}).get("name") == "FundedNext MCP"
