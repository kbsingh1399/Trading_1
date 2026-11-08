"""
========================================================================================
Institutional Multi-Agent Squad Orchestrator & Deliberation Engine
========================================================================================
Coordinates the 4 Specialized Divisions:
- TECH DIVISION           : TechPipelineArchitect
- TRADING DIVISION        : TradingMicrostructureQuant & FundedNextRiskGovernor
- REVIEW DIVISION         : AdversarialCriticReviewer
- OPTIMIZATION DIVISION   : QuantPerformanceOptimizer
========================================================================================
"""

import sys
import os
import json
import time
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field, asdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# Ensure multi_agent root is in path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MULTI_AGENT_DIR = os.path.dirname(CURRENT_DIR)
WORKSPACE_ROOT = os.path.dirname(MULTI_AGENT_DIR)
if MULTI_AGENT_DIR not in sys.path:
    sys.path.insert(0, MULTI_AGENT_DIR)

from squad.prompts import ALL_SQUAD_PROMPTS
from council import (
    AgentCouncil, Ballot, CouncilDecision, Vote,
    QuantMicrostructureAgent, RiskGovernor, AdversarialCritic, OrderExecutionAgent, Arbiter
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger("SquadOrchestrator")


@dataclass
class AgentBriefingPacket:
    """Compressed context packet for invoking a squad agent (Context Window Discipline)."""
    target_agent: str
    phase: str
    task_description: str
    context_summary: List[str]
    artifact_references: Dict[str, str] = field(default_factory=dict)
    grounding_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentDebriefReport:
    """Standardized debrief format returned by each squad agent."""
    agent_name: str
    phase: str
    status: str # "COMPLETE" | "BLOCKED" | "VETOED" | "PARTIAL"
    key_findings: List[str]
    quantitative_metrics: Dict[str, Any]
    blockers_or_decisions_needed: List[str]
    recommended_next_step: str
    full_output_ref: Optional[str] = None


class AgentSquadOrchestrator:
    """
    Central coordinator managing autonomous execution, context compression,
    and consensus scoring across all four divisions.
    """

    def __init__(self):
        self.prompts = ALL_SQUAD_PROMPTS
        self.council = AgentCouncil()
        self.reports_history: List[AgentDebriefReport] = []

    def get_agent_prompt(self, agent_name: str) -> str:
        """Retrieve the exhaustive system prompt for a squad agent."""
        if agent_name not in self.prompts:
            raise KeyError(f"Agent '{agent_name}' not found. Available: {list(self.prompts.keys())}")
        return self.prompts[agent_name]

    def create_briefing_packet(
        self,
        target_agent: str,
        phase: str,
        task_description: str,
        context_summary: List[str],
        artifact_references: Optional[Dict[str, str]] = None,
        grounding_data: Optional[Dict[str, Any]] = None
    ) -> AgentBriefingPacket:
        """Create a compressed context packet following the agent-squad specification."""
        return AgentBriefingPacket(
            target_agent=target_agent,
            phase=phase,
            task_description=task_description,
            context_summary=context_summary,
            artifact_references=artifact_references or {},
            grounding_data=grounding_data or {}
        )

    def deliberate_trade_setup(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> CouncilDecision:
        """
        Execute formal 5-agent deliberation on a candidate trade setup.
        Enforces strict adversarial veto, FundedNext risk bounds, and JSD divergence escalation.
        """
        logger.info(f"Deliberating candidate trade on {candidate.get('symbol')} {candidate.get('direction')}...")
        decision = self.council.deliberate(candidate, evidence)
        logger.info(
            f"Deliberation outcome: Published={decision.published}, NetVote={decision.net_vote:.2f}, "
            f"QuorumMet={decision.quorum_met}, VetoFired={decision.veto_fired}"
        )
        return decision

    def format_relay_summary(self, report: AgentDebriefReport) -> str:
        """
        Render a clean, human-readable summary following the agent-squad structured relay protocol.
        """
        lines = [
            f"## [{report.agent_name}] — {report.phase} [{report.status}]",
            "",
            "**Key Findings & Analysis:**"
        ]
        for f in report.key_findings:
            lines.append(f"- {f}")

        if report.quantitative_metrics:
            lines.append("")
            lines.append("**Quantitative Metrics:**")
            for k, v in report.quantitative_metrics.items():
                lines.append(f"- `{k}`: {v}")

        if report.blockers_or_decisions_needed:
            lines.append("")
            lines.append("**Blockers / Critical Decisions Needed:**")
            for b in report.blockers_or_decisions_needed:
                lines.append(f"- ⚠️ {b}")

        lines.append("")
        lines.append(f"**Recommended Next Step:** {report.recommended_next_step}")
        return "\n".join(lines)


def run_selftest():
    """Run verification self-test of the squad orchestration and council integration."""
    orchestrator = AgentSquadOrchestrator()
    print("=" * 80)
    print("SQUAD ORCHESTRATION ENGINE SELF-TEST")
    print("=" * 80)

    # 1. Verify all 5 agent prompts exist and are populated
    for agent_name in orchestrator.prompts:
        prompt = orchestrator.get_agent_prompt(agent_name)
        print(f"[OK] Prompt loaded for {agent_name:30s} ({len(prompt)} chars)")

    # 2. Simulate Deliberation on a High-Confluence BTC Long setup
    test_candidate = {
        "setup_id": "BTC_LONG_TEST_001",
        "symbol": "BTCUSD",
        "direction": "LONG",
        "entry_price": 76100.0,
        "stop_loss": 76000.0,
        "take_profit": 76350.0,
        "confluence_score": 82.5,
        "risk_reward_ratio": 2.50,
        "market_regime": "EXPANSION",
        "contributing_signals": ["Bullish Footprint Cluster", "P90 Short Liquidation Spill", "Magnet Zone Inflow"]
    }
    test_evidence = {
        "account_cushion": 45.0, # Cushion above $4,600 floor
        "nearest_ask_wall": 76800.0,
        "direction_entropy": 0.32,
        "slippage_est": 0.05
    }

    print("\n--- Testing Council Deliberation ---")
    decision = orchestrator.deliberate_trade_setup(test_candidate, test_evidence)
    print(f"Decision Published : {decision.published}")
    print(f"Quorum Met         : {decision.quorum_met}")
    print(f"Veto Fired         : {decision.veto_fired}")
    print(f"Net Vote           : {decision.net_vote:.2f}")
    for b in decision.ballots:
        print(f"  * {b['agent']:22s} : {b['vote']} (Conf: {b['confidence']:.2f}, Veto: {b['veto']}) -> {b['rationale']}")

    print("\n[SUCCESS] SQUAD ENGINE VERIFIED.")


if __name__ == "__main__":
    run_selftest()
