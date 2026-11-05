---
name: ralph-loop
description: "Run iterative autonomous agent execution loops using externalized memory files (PRD.md and progress.txt). Executes exactly one task per iteration with fresh context, append-only progress logging, automatic git commits, and model fallback chains."
metadata:
  version: "1.0.0"
  triggers:
    - ralph-loop
    - ralph.?loop
    - autonomous.?loop
    - iterative.?execution
---

# Ralph Loop for Antigravity

Ralph Loop enables autonomous, multi-turn AI agent execution without context rot by strictly externalizing memory into files on disk.

## Core Architecture

1. **Task Ledger (`PRD.md`)**:
   Structured markdown defining user requirements, acceptance criteria, and sequentially ordered tasks with checkboxes `[ ]` and `[x]`.

2. **Progress Ledger (`progress.txt`)**:
   Strictly append-only log tracking timestamps, completed task IDs, test verification outcomes, git commit hashes, and learnings. Never truncate or delete previous entries.

3. **Single Task Execution Invariant**:
   In each iteration, the agent selects and completes **exactly ONE pending task** from `PRD.md`.

4. **Verification & Guardrails**:
   Run all regression and unit tests before marking the task complete.

5. **Commit & Iterate**:
   Commit working tree changes with conventional commit syntax, append verification to `progress.txt`, mark the task `[x]` in `PRD.md`, and proceed to the next iteration.

## Rate-Limit Fallback Strategy

1. **Primary Model**: `Gemini 3 Flash` / `Gemini 3.1 Pro (High)`
2. **Fallback Tier 1**: `Claude Opus 4.6 (Thinking)` / `Claude Sonnet 4.6`
3. **Fallback Tier 2**: `Gemini 3.1 Pro (High)`
