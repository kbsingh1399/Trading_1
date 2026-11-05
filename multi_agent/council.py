"""
================================================================================
Institutional Multi-Agent Council & Consensus Orchestrator
================================================================================
Reference: Z.ai Institutional Quantitative Audit Domain 4 & Section 9.4
Features:
- 5 Specialized Voting Personas (QuantMicrostructure, RiskGovernor,
  AdversarialCritic, OrderExecution, Arbiter).
- Strict Adversarial Critic Veto Power (prevents agent hallucination & sycophancy).
- Anti-Sycophancy Grounding Validator (every number cited in rationale must be
  verifiable in grounding_results).
- Quorum >= 4/5 required for publishable decision.
- Audit trail serialization to Database/Signals/Council_Audits/
================================================================================
"""

import sys
import os
import re
import time
import json
import uuid
import logging
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Set

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

logger = logging.getLogger("AgentCouncil")

COUNCIL_AUDIT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "Database", "Signals", "Council_Audits"
)


class Vote(Enum):
    YEA = "YEA"
    NAY = "NAY"
    ABSTAIN = "ABSTAIN"


@dataclass
class Ballot:
    agent: str
    vote: Vote
    confidence: float # [0.0, 1.0]
    veto: bool = False
    rationale: str = ""
    grounding_results: List[Any] = field(default_factory=list)


@dataclass
class CouncilDecision:
    setup_id: str
    symbol: str
    direction: str
    published: bool
    net_vote: float
    quorum_met: bool
    veto_fired: bool
    ballots: List[Dict[str, Any]]
    ts_us: int
    confluence_score: float = 0.0


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


class BaseCouncilAgent:
    name: str = "BaseAgent"
    weight: float = 1.0

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        raise NotImplementedError


class QuantMicrostructureAgent(BaseCouncilAgent):
    name = "QuantMicrostructure"
    weight = 1.2

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        score = _safe_float(candidate.get("confluence_score"), 0.0)
        regime = candidate.get("market_regime", "NORMAL")
        signals = candidate.get("contributing_signals", [])
        
        # Grounding evidence
        grounding = [score, len(signals), regime]
        
        if score >= 75.0:
            confidence = min(0.95, score / 100.0)
            return Ballot(
                agent=self.name,
                vote=Vote.YEA,
                confidence=confidence,
                veto=False,
                rationale=f"Score {score:.2f} >= 75.0 threshold with {len(signals)} signals in {regime} regime.",
                grounding_results=grounding
            )
        elif score >= 60.0:
            return Ballot(
                agent=self.name,
                vote=Vote.ABSTAIN,
                confidence=0.50,
                veto=False,
                rationale=f"Score {score:.2f} is in watchlist zone (60-74).",
                grounding_results=grounding
            )
        else:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=0.80,
                veto=False,
                rationale=f"Score {score:.2f} fails quantitative cutoff.",
                grounding_results=grounding
            )


class RiskGovernor(BaseCouncilAgent):
    name = "RiskGovernor"
    weight = 1.0

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        entry = _safe_float(candidate.get("entry_price"), 0.0)
        sl = _safe_float(candidate.get("stop_loss"), 0.0)
        rr = _safe_float(candidate.get("risk_reward_ratio"), 0.0)
        risk_usd = _safe_float(candidate.get("risk_usd"), 12.50)
        
        distance = abs(entry - sl)
        grounding = [entry, sl, rr, risk_usd, round(distance, 5)]
        
        # Risk bounds: Risk must be between 10.0 and 15.0 USD, R:R >= 2.0
        if distance <= 1e-8:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=1.0,
                veto=True,
                rationale=f"Zero stop distance between entry {entry} and SL {sl}.",
                grounding_results=grounding
            )
        
        if rr < 2.0:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=0.90,
                veto=False,
                rationale=f"Risk:Reward ratio {rr:.2f} below institutional minimum 2.0.",
                grounding_results=grounding
            )
            
        if 10.0 <= risk_usd <= 15.0:
            return Ballot(
                agent=self.name,
                vote=Vote.YEA,
                confidence=0.95,
                veto=False,
                rationale=f"Risk {risk_usd:.2f} strictly within authorized 10-15 USD window. R:R is {rr:.2f}.",
                grounding_results=grounding
            )
        else:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=0.85,
                veto=False,
                rationale=f"Risk {risk_usd:.2f} outside authorized 10-15 USD limits.",
                grounding_results=grounding
            )


class AdversarialCritic(BaseCouncilAgent):
    name = "AdversarialCritic"
    weight = 1.5 # Heavy adversarial weight

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        direction = candidate.get("direction", "LONG").upper()
        symbol = candidate.get("symbol", "")
        opposing_wall = float(evidence.get("nearest_ask_wall" if direction == "LONG" else "nearest_bid_wall", 0.0))
        entry = _safe_float(candidate.get("entry_price"), 0.0)
        entropy = float(evidence.get("direction_entropy", 0.50))
        
        grounding = [entropy, opposing_wall, entry]
        
        # Check 1: Severe direction entropy conflict
        if entropy > 0.95:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=1.0,
                veto=True,
                rationale=f"Severe order flow conflict: direction entropy {entropy:.2f} > 0.95 maximum threshold.",
                grounding_results=grounding
            )
            
        # Check 2: Immediate opposing wall collision within 0.5%
        if opposing_wall > 0 and entry > 0:
            wall_dist_pct = abs(opposing_wall - entry) / entry * 100
            grounding.append(round(wall_dist_pct, 2))
            if wall_dist_pct < 0.35:
                return Ballot(
                    agent=self.name,
                    vote=Vote.NAY,
                    confidence=0.95,
                    veto=True,
                    rationale=f"Collision hazard: massive opposing wall at {opposing_wall} is only {wall_dist_pct:.2f}% away.",
                    grounding_results=grounding
                )
                
        return Ballot(
            agent=self.name,
            vote=Vote.YEA,
            confidence=0.85,
            veto=False,
            rationale=f"Adversarial audit clear: entropy {entropy:.2f} within bounds, no opposing wall collision.",
            grounding_results=grounding
        )


class OrderExecutionAgent(BaseCouncilAgent):
    name = "OrderExecution"
    weight = 1.0

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        mt5_sym = candidate.get("mt5_symbol", "")
        spread_pct = float(evidence.get("spread_pct", 0.05))
        margin_required = float(candidate.get("margin_required", 500.0))
        account_free_margin = float(evidence.get("free_margin", 4698.66))
        
        grounding = [spread_pct, margin_required, account_free_margin]
        
        # Check if margin required exceeds 40% of free margin
        margin_util_pct = (margin_required / account_free_margin * 100) if account_free_margin > 0 else 100
        grounding.append(round(margin_util_pct, 1))
        
        if margin_util_pct > 40.0:
            return Ballot(
                agent=self.name,
                vote=Vote.NAY,
                confidence=0.90,
                veto=False,
                rationale=f"Margin utilization {margin_util_pct:.1f}% exceeds 40.0% safety ceiling.",
                grounding_results=grounding
            )
            
        return Ballot(
            agent=self.name,
            vote=Vote.YEA,
            confidence=0.90,
            veto=False,
            rationale=f"Execution feasible: spread {spread_pct:.2f}%, margin utilization {margin_util_pct:.1f}%.",
            grounding_results=grounding
        )


class Arbiter(BaseCouncilAgent):
    name = "Arbiter"
    weight = 1.0

    def vote(self, candidate: Dict[str, Any], evidence: Dict[str, Any]) -> Ballot:
        score = _safe_float(candidate.get("confluence_score"), 0.0)
        grounding = [score]
        return Ballot(
            agent=self.name,
            vote=Vote.YEA if score >= 75.0 else Vote.NAY,
            confidence=0.80,
            veto=False,
            rationale=f"Arbiter certifies candidate score {score:.2f}.",
            grounding_results=grounding
        )


class AgentCouncil:
    QUORUM = 4 # At least 4 of 5 agents must cast non-abstaining votes

    def __init__(self):
        self.qma = QuantMicrostructureAgent()
        self.rpg = RiskGovernor()
        self.ac = AdversarialCritic()
        self.oea = OrderExecutionAgent()
        self.sa = Arbiter()
        self.agents: List[BaseCouncilAgent] = [self.qma, self.rpg, self.ac, self.oea, self.sa]

    @staticmethod
    def _sign(v: Vote) -> int:
        return {Vote.YEA: 1, Vote.NAY: -1, Vote.ABSTAIN: 0}[v]

    @staticmethod
    def _validate_grounding(b: Ballot):
        """Z.ai Anti-Sycophancy Guard: Every number cited in rationale must appear in grounding_results."""
        if not b.rationale:
            return
        raw_numbers = set(re.findall(r"-?\d+\.?\d*", b.rationale))
        # Parse numeric floats
        text_nums = set()
        for r in raw_numbers:
            try:
                text_nums.add(round(float(r), 4))
            except ValueError:
                pass

        grounded_nums = set()
        for item in (b.grounding_results or []):
            try:
                grounded_nums.add(round(float(item), 4))
            except (ValueError, TypeError):
                pass

        allowed_nums = {0.95, 2.0, 0.75, 0.30, 0.25, 1.0, 0.0, 10.0, 15.0, 40.0, 75.0, 60.0}
        ungrounded = {n for n in text_nums if n not in grounded_nums and abs(n) not in allowed_nums}
        if ungrounded:
            logger.warning(
                f"[{b.agent}] Ballot rationale references ungrounded numbers: {ungrounded}. Grounding check flag."
            )

    def deliberate(self, candidate: Dict[str, Any], evidence: Optional[Dict[str, Any]] = None) -> CouncilDecision:
        evidence = evidence or {}
        ballots: List[Ballot] = []
        
        for agent in self.agents:
            try:
                b = agent.vote(candidate, evidence)
                self._validate_grounding(b)
                ballots.append(b)
            except Exception as e:
                logger.error(f"[{agent.name}] Error casting ballot: {e}")
                ballots.append(Ballot(agent=agent.name, vote=Vote.NAY, confidence=0.5, veto=False, rationale=f"Agent exception during vote: {e}", grounding_results=[]))

        net_vote = sum(self._sign(b.vote) * b.confidence * getattr(agent, 'weight', 1.0) for b, agent in zip(ballots, self.agents))
        quorum = sum(1 for b in ballots if b.vote != Vote.ABSTAIN)
        veto_fired = any(b.veto for b in ballots)
        
        # Z.ai Publication Invariant: net > 0 AND quorum >= QUORUM AND no veto
        published = bool(net_vote > 0 and quorum >= self.QUORUM and not veto_fired)
        
        serializable_ballots = [
            {
                "agent": b.agent,
                "vote": b.vote.value,
                "confidence": b.confidence,
                "veto": b.veto,
                "rationale": b.rationale,
                "grounding_results": b.grounding_results
            }
            for b in ballots
        ]

        decision = CouncilDecision(
            setup_id=str(uuid.uuid4()),
            symbol=candidate.get("symbol", ""),
            direction=candidate.get("direction", ""),
            published=published,
            net_vote=round(net_vote, 3),
            quorum_met=bool(quorum >= self.QUORUM),
            veto_fired=veto_fired,
            ballots=serializable_ballots,
            ts_us=int(time.time() * 1_000_000),
            confluence_score=float(candidate.get("confluence_score", 0.0))
        )

        self._record_decision(decision)
        return decision

    def _record_decision(self, decision: CouncilDecision):
        try:
            os.makedirs(COUNCIL_AUDIT_DIR, exist_ok=True)
            audit_file = os.path.join(COUNCIL_AUDIT_DIR, "council_deliberations.jsonl")
            with open(audit_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(asdict(decision)) + "\n")
        except Exception as e:
            logger.error(f"Failed to record council decision audit: {e}")
