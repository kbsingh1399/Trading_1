from fundednext_mcp_client import FundedNextMCPClient
import json

client = FundedNextMCPClient()
overview = client.get_account_overview()
cushion = client.get_risk_cushion()
trades = client.get_running_trades()

print("OVERVIEW:", json.dumps(overview, indent=2))
print("CUSHION:", json.dumps(cushion, indent=2))
print("TRADES TYPE:", type(trades))
print("TRADES:", trades)
