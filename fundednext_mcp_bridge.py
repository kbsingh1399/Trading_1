#!/usr/bin/env python3
"""
FundedNext MCP stdio Bridge for Antigravity & Agent IDEs
Acts as a standard stdio MCP server that proxies JSON-RPC requests to https://mcp.fundednext.com/
with Bearer token authentication.
"""

import sys
import json
import urllib.request
import urllib.error
from fundednext_mcp_client import get_token, FUNDEDNEXT_MCP_URL

def main():
    token = get_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Antigravity-Bridge/1.0",
    }

    # Set stdout to unbuffered utf-8
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stdin.reconfigure(encoding="utf-8")

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        line = line.strip()
        if not line:
            continue

        try:
            req_data = json.loads(line)
        except Exception:
            continue

        # Forward request to FundedNext MCP
        try:
            req_bytes = json.dumps(req_data).encode("utf-8")
            req = urllib.request.Request(FUNDEDNEXT_MCP_URL, data=req_bytes, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as res:
                res_body = res.read().decode("utf-8")
                sys.stdout.write(res_body + "\n")
                sys.stdout.flush()
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8")
            err_resp = {
                "jsonrpc": "2.0",
                "id": req_data.get("id"),
                "error": {"code": -32000, "message": f"HTTP {e.code}: {err_body}"}
            }
            sys.stdout.write(json.dumps(err_resp) + "\n")
            sys.stdout.flush()
        except Exception as e:
            err_resp = {
                "jsonrpc": "2.0",
                "id": req_data.get("id"),
                "error": {"code": -32603, "message": str(e)}
            }
            sys.stdout.write(json.dumps(err_resp) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
