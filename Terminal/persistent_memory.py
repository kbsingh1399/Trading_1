"""
Persistent Agent Memory & Knowledge Graph Engine
================================================
Durable, cross-session memory bank for the autonomous trading agent.
Stores:
- Prop-firm & broker constraints and risk rules.
- Asset microstructure profiles and spread thresholds.
- Strategy weights and performance metrics.
- Forensic trade post-mortems and evolutionary lessons.
- Auto-syncs to Database/MEMORY_BANK.md for immediate readability.
"""

import os
import sys
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
DB_DIR.mkdir(parents=True, exist_ok=True)
SQLITE_DB = DB_DIR / "persistent_memory.db"
MARKDOWN_EXPORT = DB_DIR / "MEMORY_BANK.md"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category TEXT NOT NULL,
            name TEXT NOT NULL UNIQUE,
            summary TEXT NOT NULL,
            details_json TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            target_name TEXT NOT NULL,
            relation_type TEXT NOT NULL,
            weight REAL DEFAULT 1.0,
            notes TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(source_name, target_name, relation_type)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_reflections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            strategy TEXT NOT NULL,
            outcome TEXT NOT NULL,
            pnl_usd REAL NOT NULL,
            risk_usd REAL NOT NULL,
            root_cause TEXT NOT NULL,
            lesson_learned TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS strategy_weights (
            strategy_name TEXT PRIMARY KEY,
            weight REAL NOT NULL DEFAULT 1.0,
            total_trades INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            breakevens INTEGER NOT NULL DEFAULT 0,
            net_pnl_usd REAL NOT NULL DEFAULT 0.0,
            updated_at TEXT NOT NULL
        )
        """)
        
        conn.commit()


def save_entity(category: str, name: str, summary: str, details: Optional[Dict] = None):
    now = datetime.now(timezone.utc).isoformat()
    det_json = json.dumps(details or {}, default=str)
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO entities (category, name, summary, details_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            category=excluded.category,
            summary=excluded.summary,
            details_json=excluded.details_json,
            updated_at=excluded.updated_at
        """, (category, name, summary, det_json, now, now))
        conn.commit()


def save_relation(source: str, target: str, rel_type: str, weight: float = 1.0, notes: str = ""):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO relations (source_name, target_name, relation_type, weight, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_name, target_name, relation_type) DO UPDATE SET
            weight=excluded.weight,
            notes=excluded.notes
        """, (source, target, rel_type, weight, notes, now))
        conn.commit()


def record_trade_reflection(
    ticket_id: str,
    symbol: str,
    direction: str,
    strategy: str,
    outcome: str,
    pnl_usd: float,
    risk_usd: float,
    root_cause: str,
    lesson_learned: str
):
    now = datetime.now(timezone.utc).isoformat()
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO trade_reflections (
            ticket_id, symbol, direction, strategy, outcome, pnl_usd, risk_usd, root_cause, lesson_learned, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticket_id) DO UPDATE SET
            outcome=excluded.outcome,
            pnl_usd=excluded.pnl_usd,
            root_cause=excluded.root_cause,
            lesson_learned=excluded.lesson_learned
        """, (ticket_id, symbol, direction, strategy, outcome, pnl_usd, risk_usd, root_cause, lesson_learned, now))
        
        # Update strategy weights
        cursor.execute("SELECT * FROM strategy_weights WHERE strategy_name = ?", (strategy,))
        row = cursor.fetchone()
        if not row:
            total = 1
            wins = 1 if outcome == "WIN" else 0
            losses = 1 if outcome == "LOSS" else 0
            be = 1 if outcome == "BREAKEVEN" else 0
            net = pnl_usd
            w = 1.0 + (0.1 if outcome == "WIN" else (-0.15 if outcome == "LOSS" else 0.0))
            cursor.execute("""
            INSERT INTO strategy_weights (strategy_name, weight, total_trades, wins, losses, breakevens, net_pnl_usd, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (strategy, max(0.2, min(2.0, w)), total, wins, losses, be, net, now))
        else:
            total = row["total_trades"] + 1
            wins = row["wins"] + (1 if outcome == "WIN" else 0)
            losses = row["losses"] + (1 if outcome == "LOSS" else 0)
            be = row["breakevens"] + (1 if outcome == "BREAKEVEN" else 0)
            net = row["net_pnl_usd"] + pnl_usd
            adj = 0.1 if outcome == "WIN" else (-0.15 if outcome == "LOSS" else 0.0)
            w = max(0.2, min(2.0, row["weight"] + adj))
            cursor.execute("""
            UPDATE strategy_weights
            SET weight = ?, total_trades = ?, wins = ?, losses = ?, breakevens = ?, net_pnl_usd = ?, updated_at = ?
            WHERE strategy_name = ?
            """, (w, total, wins, losses, be, net, now, strategy))
            
        conn.commit()


def get_rules() -> List[Dict]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM entities WHERE category = 'RULE' ORDER BY name ASC")
        return [dict(r) for r in cursor.fetchall()]


def get_lessons() -> List[Dict]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM entities WHERE category = 'LESSON' ORDER BY updated_at DESC")
        return [dict(r) for r in cursor.fetchall()]


def get_strategy_weight(strategy: str) -> float:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT weight FROM strategy_weights WHERE strategy_name = ?", (strategy,))
        row = cursor.fetchone()
        return row["weight"] if row else 1.0


def sync_to_markdown():
    """Generates a human- and agent-readable Markdown memory bank."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM entities ORDER BY category, name")
        entities = [dict(r) for r in cursor.fetchall()]
        
        cursor.execute("SELECT * FROM strategy_weights ORDER BY weight DESC")
        weights = [dict(r) for r in cursor.fetchall()]
        
        cursor.execute("SELECT * FROM trade_reflections ORDER BY created_at DESC LIMIT 20")
        reflections = [dict(r) for r in cursor.fetchall()]

    lines = [
        "# Agent Persistent Memory Bank",
        f"*Synchronized: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}*",
        "",
        "## 1. Non-Negotiable Operational Rules & Guardrails",
        ""
    ]

    rules = [e for e in entities if e["category"] == "RULE"]
    for r in rules:
        lines.append(f"### 🛡️ {r['name']}")
        lines.append(f"{r['summary']}")
        if r["details_json"] and r["details_json"] != "{}":
            try:
                det = json.loads(r["details_json"])
                for k, v in det.items():
                    lines.append(f"- **{k}**: `{v}`")
            except Exception:
                pass
        lines.append("")

    lines.append("## 2. Institutional Lessons & Forensic Traps")
    lines.append("")
    lessons = [e for e in entities if e["category"] == "LESSON"]
    for les in lessons:
        lines.append(f"### ⚠️ {les['name']}")
        lines.append(f"{les['summary']}")
        lines.append("")

    lines.append("## 3. Dynamic Strategy Weights & Edge Scores")
    lines.append("")
    if weights:
        lines.append("| Strategy | Weight | Trades | Win Rate | Net PnL ($) |")
        lines.append("| :--- | :---: | :---: | :---: | :---: |")
        for w in weights:
            win_rate = (w["wins"] / w["total_trades"] * 100) if w["total_trades"] > 0 else 0.0
            lines.append(f"| **{w['strategy_name']}** | `{w['weight']:.2f}` | {w['total_trades']} | {win_rate:.1f}% | ${w['net_pnl_usd']:.2f} |")
        lines.append("")
    else:
        lines.append("*No strategy performance recorded yet.*")
        lines.append("")

    lines.append("## 4. Trade Reflection History (Last 20)")
    lines.append("")
    if reflections:
        for ref in reflections:
            lines.append(f"- **Ticket #{ref['ticket_id']}** ({ref['symbol']} {ref['direction']}) -> `{ref['outcome']}` (${ref['pnl_usd']:.2f})")
            lines.append(f"  - *Strategy*: {ref['strategy']}")
            lines.append(f"  - *Root Cause*: {ref['root_cause']}")
            lines.append(f"  - *Lesson*: {ref['lesson_learned']}")
        lines.append("")
    else:
        lines.append("*No trade reflections logged yet.*")
        lines.append("")

    with open(MARKDOWN_EXPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def seed_initial_knowledge():
    init_database()
    
    # 1. Rules
    save_entity(
        category="RULE",
        name="Hard Max Dollar Loss per Trade",
        summary="Under NO circumstances may any single trade risk more than $50.00 USD. If calculated risk exceeds $50.00, trade lot sizing MUST be clamped downward before order submission.",
        details={"HARD_MAX_LOSS_USD": 50.00}
    )
    
    save_entity(
        category="RULE",
        name="Prop Firm Floor Guardrail (Dual Capital Budget)",
        summary="On prop firm challenge accounts (e.g. Blueberry Funded 6% max drawdown), position dollar risk must never exceed 50% of the remaining buffer above the liquidation floor: risk_usd = min($50.00, max(0, (current_equity - floor_equity) * 0.50)). If buffer <= $10.00, trading must immediately halt.",
        details={"MAX_OVERALL_DD_PCT": 6.0, "BUFFER_SAFETY_MULTIPLIER": 0.50, "MIN_HALT_BUFFER_USD": 10.00}
    )
    
    save_entity(
        category="RULE",
        name="Broker Spread Gatekeeper",
        summary="Reject any crypto trade where the broker ask-bid spread exceeds 0.45% of price. Assets like AXS (24% spread) and SUSHI (11% spread) cause massive immediate slippage loss and are strictly blacklisted.",
        details={"MAX_SPREAD_PCT": 0.45, "PREFERRED_TIGHT_PAIRS": ["BTCUSD.pi", "ETHUSD.pi", "SOLUSD.p", "XRPUSD.pi", "AVXUSD.p"]}
    )

    save_entity(
        category="RULE",
        name="Emergency Circuit Breaker",
        summary="The live trader daemon must continuously check open positions every 10 seconds. If any position floating loss touches -$48.00 USD, an emergency market close order is sent immediately.",
        details={"CIRCUIT_BREAKER_LOSS_USD": -48.00}
    )

    # 2. Lessons
    save_entity(
        category="LESSON",
        name="Lesson #1: Prop Firm Floor vs Trade Sizing Collision",
        summary="On 2026-11-01, a trade on SOL was submitted with calculated risk $46.19 (compliant with the $50 cap). However, the account balance was $4,714.57 against the $4,700.00 Blueberry Funded 6% floor. The intraday fluctuation touched the $4,700 floor, triggering broker auto-liquidation. Sizing must always factor in remaining floor margin, not just starting account balance.",
        details={"event": "MaxDrawdownBreached", "date": "2026-11-01"}
    )

    save_entity(
        category="LESSON",
        name="Lesson #2: Native Direct Page CDP over Browser CDP",
        summary="Playwright's default browser CDP connection hangs when Chrome/Edge has 20+ tabs open due to browser target contention with MCP servers. Using direct Page WebSocket endpoints ('ws://127.0.0.1:9222/devtools/page/<id>') achieves 10-millisecond execution with zero contention.",
        details={"protocol": "Direct Page WebSocket CDP"}
    )

    # 3. Initial Trade Reflection
    record_trade_reflection(
        ticket_id="19548368",
        symbol="XRPUSD.pi",
        direction="BUY",
        strategy="Whale Bid Wall Support Fade",
        outcome="WIN",
        pnl_usd=1.00,
        risk_usd=30.00,
        root_cause="Clean entry above $2.05M whale bid wall; held comfortably through volatility.",
        lesson_learned="Whale bid order book walls provide solid structural defense when combined with low spread."
    )
    
    record_trade_reflection(
        ticket_id="19549297",
        symbol="SOLUSD.p",
        direction="BUY",
        strategy="Order-flow Absorption Reversal",
        outcome="LOSS",
        pnl_usd=-14.06,
        risk_usd=46.19,
        root_cause="Account lacked sufficient buffer ($14.57) above the prop firm $4,700.00 drawdown floor.",
        lesson_learned="Never open a trade whose dollar risk exceeds remaining account floor margin."
    )

    sync_to_markdown()
    print("Persistent Memory Bank initialized and synchronized successfully.")


if __name__ == "__main__":
    seed_initial_knowledge()
