"""
Generate self-contained Markdown prompt for Chatbot Arena Quant Review.
Bundles architecture and source code so that Arena can review it directly
without encountering 404 errors on private GitHub repositories.
"""

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent

FILES_TO_BUNDLE = [
    "Terminal/risk_envelope.py",
    "Terminal/realtime_signal_evaluator.py",
    "Terminal/deepmind_timesfm_forecaster.py",
    "Terminal/vector_memory.py",
    "Terminal/crypto_live_trader.py"
]

def main():
    lines = []
    lines.append("# Institutional Quantitative Audit Submission: Order-Flow & Microstructure Execution Engine\n\n")
    lines.append("You are acting as an Elite Principal Quantitative Trader & Execution Architect.\n")
    lines.append("Please perform a rigorous, institutional critique of our algorithmic trading architecture and order-flow execution pipeline.\n\n")
    
    lines.append("## Institutional Upgrades Implemented:\n")
    lines.append("1. **Canonical Risk Envelope (`risk_envelope.py`)**:\n")
    lines.append("   - Single source of truth. Fails closed on inverted stops, non-positive levels, and spread > 45 bps.\n")
    lines.append("   - Exact discrete grid volume solver `admissible_volume()` using integer step bounds $k_{min} = \\lceil floor / loss\\_per\\_step \\rceil$ and $k_{max} = \\lfloor cap / loss\\_per\\_step \\rfloor$.\n")
    lines.append("   - Hard $50.00 max risk ceiling per trade, $5.00 min risk floor, max 2 concurrent positions.\n")
    lines.append("   - Prop-firm floor defense: capital budget $\\le 50\\%$ of available equity buffer above $4,700 floor; halt if buffer $\\le $10.00.\n\n")
    
    lines.append("2. **Google DeepMind TimesFM 2.5 Stationary Increment Forecaster (`deepmind_timesfm_forecaster.py`)**:\n")
    lines.append("   - Replaced non-stationary CVD level forecasting with first differences: $\\Delta CVD_t = CVD_t - CVD_{t-1}$.\n")
    lines.append("   - Forecasts future stationary increments and sums them for directional consensus.\n")
    lines.append("   - Quantifies epistemic uncertainty via $(q_{90} - q_{10})$ quantile spread relative to series standard deviation, generating an `uncertainty_multiplier` (0.40 to 1.0) that discounts max risk ceiling during high uncertainty.\n\n")

    lines.append("3. **Counterfactual Memory in LanceDB (`vector_memory.py`)**:\n")
    lines.append("   - Added `counterfactual_setups` vector table.\n")
    lines.append("   - Records all rejected setups with geometry and rejection reason to track opportunity cost and quantify filter efficiency over time.\n\n")

    lines.append("4. **Autonomous Realtime Evaluator (`realtime_signal_evaluator.py`)**:\n")
    lines.append("   - Automatically triggered on footprint and order-flow signal discoveries.\n")
    lines.append("   - Synthesizes DuckDB OLAP microstructure, regime classification, vector memory analogs, and TimesFM stationary increments.\n")
    lines.append("   - Discretely solves volume and validates geometry before declaring a setup actionable.\n\n")

    lines.append("## Full Source Code of Core Modules:\n\n")

    for rel_path in FILES_TO_BUNDLE:
        full_p = ROOT_DIR / rel_path
        if full_p.exists():
            code = full_p.read_text(encoding="utf-8", errors="replace")
            lines.append(f"---\n### File: `{rel_path}`\n\n```python\n{code}\n```\n\n")
        else:
            lines.append(f"---\n### File: `{rel_path}` (NOT FOUND)\n\n")

    out_file = ROOT_DIR / "arena_quant_audit_bundle.md"
    out_file.write_text("".join(lines), encoding="utf-8")
    print(f"Generated {out_file.name} successfully ({len(''.join(lines)):,} characters).")

if __name__ == "__main__":
    main()
