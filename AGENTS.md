# AGENTS.md — Institutional Quant Trading & Engineering Constitution
========================================================================================
Canonical Architecture & Behavioral Directive: Bound by Karpathy Guidelines & Superpowers
========================================================================================

## 1. Core Mandate & Behavioral Invariants
All agents operating in this repository (`_Trading_1`) must strictly adhere to the engineering principles established by **Andrej Karpathy** and the **Superpowers Agentic Execution Discipline**:

---

### Pillar I: Think Before Coding (Anti-Assumption)
- **Don't Assume. Don't Hide Confusion. Surface Tradeoffs.**
- Before touching code, state critical assumptions explicitly. If requirements or market routes are ambiguous, stop, name what is confusing, and clarify.
- If multiple interpretations exist, present them explicitly with their tradeoffs—never silently pick one.
- If a simpler, more robust architectural pattern exists, state it. Push back against unnecessary complexity.

---

### Pillar II: Simplicity First (Anti-Overengineering)
- **Minimum Code That Solves The Problem. Nothing Speculative.**
- Write zero features beyond what was requested.
- Build zero abstractions for single-use code.
- Introduce zero "flexibility" or "configurability" flags that were not asked for.
- Avoid defensive handling for mathematically or physically impossible scenarios.
- If a function or script can be written in 50 clean, cohesive lines instead of 200 bloated lines, write the 50 lines.
- **The Senior Engineer Bar**: *"Would an institutional tech lead say this is overcomplicated?"* If yes, refactor and simplify immediately.

---

### Pillar III: Surgical Changes (Anti-Contamination)
- **Touch Only What You Must. Clean Up Only Your Own Mess.**
- When modifying existing scripts or pipeline components:
  - Do not "improve" or reformat adjacent code, docstrings, or comments.
  - Do not refactor modules that are already working unless explicitly asked.
  - Strictly match existing coding conventions, indentations, and naming styles.
  - If you identify unrelated legacy dead code, note it in your report—never delete it unilaterally.
- When your code changes create orphans:
  - Remove any imports, functions, or variables that *your* changes made unused.
  - Every single changed line must trace directly to the user's explicit objective.

---

### Pillar IV: Goal-Driven Execution (Verifiable Feedback Loops)
- **Define Verifiable Success Criteria Upfront. Loop Until Empirically Proven.**
- Transform every user request into an explicit, verifiable objective:
  ```
  1. [Step / Action] -> Verify: [Deterministic Check / Parquet Audit / CLI Output]
  2. [Step / Action] -> Verify: [Deterministic Check / Parquet Audit / CLI Output]
  ```
- **Completion Guarantee**: Execute tasks via persistent verification loops (`while True:` with pending queue checks) until 100% of target assets/files exist, are uncorrupted, and match dashboard parity.
- Never declare a task complete on faith, assumptions, or silent execution. Deliver empirical proof (row counts, file lists, exit codes).

---

## 2. Superpowers Operational Protocol
1. **Systematic Debugging**:
   - Always reproduce and isolate the root cause before touching code. No speculative guesswork or random code mutations.
2. **Subagent Delegation & Parallelism**:
   - For multi-perspective verification, invoke specialized subagents (`egm-codebase-details`, `egm-design`, `code-reviewer`) with crisp, non-overlapping evaluation bounds.
3. **Decoupled Dynamic Configuration**:
   - Never hardcode asset universes or symbols in scrapers or trading algorithms.
   - Always reference the dynamic registry [`API/active_assets.json`](file:///C:/Users/SIGMA/Documents/_Trading_1/API/active_assets.json) via [`API/active_assets_manager.py`](file:///C:/Users/SIGMA/Documents/_Trading_1/API/active_assets_manager.py).
4. **Locking & Concurrency Invariants**:
   - Re-entrant file locks (`writer_lock`) using thread-local depth counters to avoid Windows 11 `WinError 32` collisions.
   - Crash-proof publication via `atomic_parquet` (`.tmp` file + `os.replace`).
   - Ephemeral CDP browser tabs with sliding window recycling and local Cloudflare WARP IP rotation.

---

## 3. Reference Files
- Complete Project Architecture & Risk Mandates: [`GEMINI.md`](file:///C:/Users/SIGMA/Documents/_Trading_1/GEMINI.md)
- Karpathy Guidelines Documentation: [`skills/karpathy-guidelines/SKILL.md`](file:///C:/Users/SIGMA/Documents/_Trading_1/skills/karpathy-guidelines/SKILL.md)
- Karpathy Anti-Pattern Examples: [`skills/karpathy-guidelines/EXAMPLES.md`](file:///C:/Users/SIGMA/Documents/_Trading_1/skills/karpathy-guidelines/EXAMPLES.md)
- Centralized Telemetry APIs: [`API/README.md`](file:///C:/Users/SIGMA/Documents/_Trading_1/API/README.md)
