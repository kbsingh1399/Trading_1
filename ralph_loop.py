"""
Ralph Loop Controller for Antigravity Autonomous Iteration.

Based on https://github.com/abhishekbhakat/ralph-loop-for-antigravity.
Enforces:
1. Memory externalization to PRD.md and progress.txt.
2. Single-task iteration invariant.
3. Test suite verification before commit.
4. Append-only progress logging.
"""

import os
import sys
import re
import subprocess
import datetime
from pathlib import Path

WORKSPACE_DIR = Path(__file__).resolve().parent
PRD_PATH = WORKSPACE_DIR / "PRD.md"
PROGRESS_PATH = WORKSPACE_DIR / "progress.txt"


def read_tasks():
    if not PRD_PATH.exists():
        return []
    
    tasks = []
    task_regex = re.compile(r"^\s*-\s*\[([ xX])\]\s*(?:Task\s*(\d+):)?\s*(.+)$")
    
    with open(PRD_PATH, "r", encoding="utf-8", errors="replace") as f:
        for idx, line in enumerate(f):
            m = task_regex.match(line)
            if m:
                is_done = m.group(1).lower() == "x"
                task_num = m.group(2) or str(idx + 1)
                desc = m.group(3).strip()
                tasks.append({
                    "index": idx,
                    "task_id": task_num,
                    "description": desc,
                    "done": is_done,
                    "raw_line": line
                })
    return tasks


def show_status():
    tasks = read_tasks()
    done = [t for t in tasks if t["done"]]
    pending = [t for t in tasks if not t["done"]]
    
    print("=" * 60)
    print("RALPH LOOP FOR ANTIGRAVITY - STATUS")
    print("=" * 60)
    print(f"Total Tasks: {len(tasks)}")
    print(f"Completed:   {len(done)}")
    print(f"Pending:     {len(pending)}")
    print("-" * 60)
    
    if pending:
        print(f"NEXT TASK -> Task {pending[0]['task_id']}: {pending[0]['description']}")
    else:
        print("ALL TASKS COMPLETED! Ralph Loop execution finished.")
    print("=" * 60)


def append_progress(task_id: str, desc: str, test_result: str, commit_hash: str = ""):
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
    log_entry = (
        f"[{now_str}] Task {task_id}: {desc}\n"
        f"  - Verification: {test_result}\n"
        f"  - Commit: {commit_hash or 'N/A'}\n\n"
    )
    with open(PROGRESS_PATH, "a", encoding="utf-8") as f:
        f.write(log_entry)
    print(f"[RalphLoop] Appended progress for Task {task_id}.")


def mark_task_done(task_id: str):
    if not PRD_PATH.exists():
        return False
    
    with open(PRD_PATH, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
        
    updated = False
    task_pattern = re.compile(rf"^\s*-\s*\[ \]\s*(?:Task\s*{re.escape(task_id)}:)?\s*")
    for i, line in enumerate(lines):
        if re.search(rf"\[ \]\s*(Task\s*{re.escape(task_id)}:)", line) or (f"Task {task_id}:" in line and "[ ]" in line):
            lines[i] = line.replace("[ ]", "[x]", 1)
            updated = True
            break
            
    if updated:
        with open(PRD_PATH, "w", encoding="utf-8") as f:
            f.writelines(lines)
        print(f"[RalphLoop] Marked Task {task_id} as done in PRD.md.")
    return updated


def run_tests():
    cmd = [sys.executable, "-m", "pytest", "tests/", "-q"]
    print(f"[RalphLoop] Running test verification: {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=WORKSPACE_DIR, capture_output=True, text=True)
    if res.returncode == 0:
        print(f"[RalphLoop] Tests PASSED:\n{res.stdout.strip()}")
        return True, "100% PASSED"
    else:
        print(f"[RalphLoop] Tests FAILED:\n{res.stderr.strip() or res.stdout.strip()}")
        return False, "FAILED"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        show_status()
    elif len(sys.argv) > 1 and sys.argv[1] == "--run-tests":
        passed, res = run_tests()
        sys.exit(0 if passed else 1)
    else:
        show_status()
