# Architecture Design Specification: DeepSeek Harness (`dsh`) Antigravity Integration & RAM Optimization (EGM Approved)

- **Author**: Antigravity Quantitative Systems Architect (The Elephant)
- **Status**: Approved Post-Goldfish Review
- **Repository**: `_Trading_1`
- **Target**: `deepseek-harness` + Antigravity Agent Runtime Bridge (`Terminal/deepseek_harness_bridge.py`)

---

## 1. System Overview & Context

### 1.1 Problem Statement
The user requested:
1. **System RAM Optimization**: Terminate bloated background Python daemons (which were consuming >3.1 GB of RAM and pushing memory usage to 88%+, starving Windows swap and degrading performance).
2. **Install DeepSeek Harness**: Clone and install `https://github.com/deepseek-ai/deepseek-harness` locally in the workspace.
3. **Antigravity Intelligence Integration**: Connect the DeepSeek Harness intelligence layer cleanly into the Antigravity workspace so that agents, scripts, and workflows can harness DeepSeek reasoning (V3/R1 models, autonomous loops, and quant setup validation).
4. **Response Latency Diagnostic & Remedy**: Explain the technical causes of agent response latency and provide actionable mitigations (context window compaction, daemon decoupling, memory optimization).

### 1.2 System Invariants (Strict Preservation)
- **Trading Safety Invariant**: DeepSeek Harness operates strictly in an isolated intelligence/auditing plane. It must **never** execute live trades directly bypassing `Terminal/delta_exchange_live_trader.py` or violate Delta India live guardrails ($R:R \ge 2.0$, `MAX_OPEN_POSITIONS = 2`, `MAX_RISK_USD = $25.00`, 20% margin buffer).
- **RAM Budget Invariant**: Lean memory footprint ($\le 150 \text{ MB}$). Heavy OfficeKit (LibreOfficeKit 500MB+) and desktop Electron bundles are completely excised from the trading runtime.
- **Strict Latency SLA Invariant**: All reasoning checks in live dispatch paths enforce a hard $\le 5.0\text{s}$ timeout. Calls that exceed the deadline fail closed without stalling 15m candle execution.
- **Windows Kernel Job Object Containment**: Spawning `dsh` child processes must assign them to a native Win32 Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, with a fallback to `taskkill.exe /PID <pid> /T /F`. When the parent Python process terminates for any reason, the Windows kernel deterministically tears down all descendants.
- **Storage & Workspace Isolation**: All sessions target `.dsh_home/` with `DSH_RESOURCE_CACHE = .dsh_home/cache` outside `Database/`, preventing file lock collisions and user profile pollution.

---

## 2. Technical Architecture

```
+-------------------------------------------------------------------------------+
|                             ANTIGRAVITY HARNESS                               |
|                                                                               |
|  [Sentinel Engine / Signal Evaluator / EGM Goldfish Reviewers]                |
+---------------------------------------+---------------------------------------+
                                        |
                 5.0s Strict Latency SLA / Fail-Closed API
                                        |
+---------------------------------------v---------------------------------------+
|                Terminal/deepseek_harness_bridge.py                            |
|                                                                               |
|  * DeepSeekHarnessBridge (Win32 Job Object Managed Child Process)             |
|  * Direct API Fallback: deepseek-chat (V3) / deepseek-reasoner (R1)           |
|  * Strict Fail-Closed Provider Gate (No synthetic local mocks in live paths)   |
|  * Isolated DSH_HOME & DSH_RESOURCE_CACHE (.dsh_home/)                       |
+---------------------------------------+---------------------------------------+
                                        |
                         Cordis Plugin Microkernel
                                        |
+---------------------------------------v---------------------------------------+
|                      DEEPSEEK HARNESS (dsh) CORE                              |
|                                                                               |
|  * Core Agent Loop & Cordis Plugins (packages/core, packages/subagent)        |
|  * Direct REST / JSON-RPC Client (deepseek-official provider)                 |
|  * Context Compaction Adapter                                                 |
+-------------------------------------------------------------------------------+
```

### 2.1 The Python SDK Bridge (`Terminal/deepseek_harness_bridge.py`)
The bridge exposes two high-value quantitative capabilities:
1. **Market Confluence & Trade Critic**: Passes market confluence states (Footprint, Auction Profile, CVD, Whales, Liquidations) to DeepSeek-R1 / DeepSeek-V3 for second-opinion reasoning before live entry approval.
2. **Conversation & Context Compaction**: Provides summarization hooks to condense prompt transcripts and prevent context rot.

### 2.2 Win32 Job Object Process Lifecycle
```python
import ctypes
from ctypes import wintypes

def create_kill_on_close_job():
    job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
    # Set JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE (0x2000)
    ...
```
Every spawned child process is assigned to this job object. When the parent terminates, the OS automatically kills all child processes.

### 2.3 Fail-Closed Provider & API Key Resolution
- Primary: `DEEPSEEK_API_KEY` with endpoints `https://api.deepseek.com`.
- Missing Key / Timeout: Raises `ProviderConfigurationError` or returns `{"approved": False, "reason": "DEEPSEEK_API_UNAVAILABLE"}`. Synthetic mocks are strictly prohibited in live dispatch.

---

## 3. Implementation Verification & Deliverables
1. `deepseek-harness/`: Cloned, Python SDK installed (`deepseek-harness-sdk` and `deepseek-harness-runtime-bin`).
2. `.gitignore`: Updated with `deepseek-harness/`, `.dsh_home/`, and `Database/DSH_Home/`.
3. `Terminal/deepseek_harness_bridge.py`: Production-grade bridge with Win32 Job Object process containment, strict 5s timeout, and fail-closed safety.
4. `Terminal/test_deepseek_harness_bridge.py`: Unit test suite verifying clean lifecycle, Win32 Job Object binding, timeout enforcement, and fail-closed handling.
