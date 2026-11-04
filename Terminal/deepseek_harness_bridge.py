"""
========================================================================================
DeepSeek Harness (dsh) Antigravity Intelligence Bridge
========================================================================================
Guarantees (EGM Approved Spec Compliance):
1. Windows Kernel Job Object Containment: Assigns spawned child processes to a native
   Win32 Job Object with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE (0x2000).
   Fallback to taskkill.exe /PID <pid> /T /F. Eliminates orphan background zombies.
2. Lean RAM Budget: Strict <= 150 MB memory footprint. No heavy OfficeKit or Electron.
3. Strict 5.0s Latency SLA: Signal criticisms enforce a hard deadline to prevent stalling
   15-minute candle completion invariants (t_ms % 900_000 == 0).
4. Fail-Closed Safety: Missing DEEPSEEK_API_KEY or network failures fail-closed
   (approved=False) without synthetic mock leakage into live order execution paths.
5. Sandboxed Isolation: Targets .dsh_home/ and .dsh_home/cache, completely outside Database/.
========================================================================================
"""

import os
import sys
import time
import json
import logging
import urllib.request
import urllib.error
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger("DeepSeekHarnessBridge")
logger.setLevel(logging.INFO)
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [DSH-Bridge] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logger.addHandler(h)

ROOT_DIR = Path(__file__).resolve().parent.parent
DSH_HOME_DIR = ROOT_DIR / ".dsh_home"
DSH_HOME_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = DSH_HOME_DIR / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Win32 Kernel Job Object Constants
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
JobObjectExtendedLimitInformation = 9


def create_win32_kill_on_close_job():
    """Creates a Win32 Job Object configured to terminate all child processes upon handle close."""
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryLimit", ctypes.c_size_t),
                ("PeakJobMemoryLimit", ctypes.c_size_t),
            ]

        job = ctypes.windll.kernel32.CreateJobObjectW(None, None)
        if not job:
            return None

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x0100
        JOB_OBJECT_LIMIT_JOB_MEMORY = 0x0200
        info.BasicLimitInformation.LimitFlags = (
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE |
            JOB_OBJECT_LIMIT_PROCESS_MEMORY |
            JOB_OBJECT_LIMIT_JOB_MEMORY
        )
        # Strict 150 MB Lean RAM Budget
        max_bytes = 150 * 1024 * 1024
        info.ProcessMemoryLimit = max_bytes
        info.JobMemoryLimit = max_bytes

        res = ctypes.windll.kernel32.SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info)
        )
        if not res:
            ctypes.windll.kernel32.CloseHandle(job)
            return None
        return job
    except Exception as e:
        logger.debug(f"Could not create Win32 Job Object: {e}")
        return None


def assign_process_to_job(job_handle, pid: int) -> bool:
    """Assigns an OS process to the kill-on-close job object."""
    if not job_handle or sys.platform != "win32":
        return False
    try:
        import ctypes
        PROCESS_SET_QUOTA = 0x0100
        PROCESS_TERMINATE = 0x0001
        h_proc = ctypes.windll.kernel32.OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, False, pid)
        if not h_proc:
            return False
        success = bool(ctypes.windll.kernel32.AssignProcessToJobObject(job_handle, h_proc))
        ctypes.windll.kernel32.CloseHandle(h_proc)
        return success
    except Exception:
        return False


def terminate_process_tree(pid: int):
    """Deterministic process tree kill via taskkill.exe /PID <pid> /T /F on Windows."""
    if sys.platform == "win32":
        try:
            taskkill = os.path.join(os.environ.get("SystemRoot", r"C:\Windows"), "System32", "taskkill.exe")
            if os.path.exists(taskkill):
                subprocess.run([taskkill, "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=4)
                return
        except Exception:
            pass
    try:
        import psutil
        proc = psutil.Process(pid)
        for child in proc.children(recursive=True):
            child.kill()
        proc.kill()
    except Exception:
        pass


class DeepSeekHarnessBridge:
    """
    Antigravity bridge to DeepSeek AI Intelligence and DeepSeek Harness Runtime.
    Provides low-latency LLM reasoning, signal validation, and context compaction.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
        timeout_seconds: float = 5.0,
    ):
        self.api_key = api_key or os.environ.get("DEEPSEEK_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.model = model
        # Strict 5.0s SLA: hard-clamp to min(requested, 5.0) to prevent stalling candle invariants
        self.timeout_seconds = min(max(0.1, float(timeout_seconds)), 5.0)
        self.job_handle = create_win32_kill_on_close_job()
        self.dsh_home = DSH_HOME_DIR
        self.cache_dir = CACHE_DIR
        self._active_children = []

    def is_configured(self) -> bool:
        """Returns True if a valid API key is present."""
        return bool(self.api_key and self.api_key.strip())

    def call_deepseek_api(self, messages: list, max_tokens: int = 512, temperature: float = 0.2) -> Tuple[bool, str, float]:
        """
        Direct REST client call to DeepSeek Chat Completions API with hard latency timeout.
        Returns (success: bool, response_or_error: str, latency_ms: float).
        """
        if not self.is_configured():
            return False, "DEEPSEEK_API_KEY_NOT_CONFIGURED", 0.0

        endpoint = f"{self.base_url}/chat/completions"
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            endpoint,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": "Antigravity-DeepSeekBridge/1.0",
            },
            method="POST",
        )

        t0 = time.time()
        try:
            effective_timeout = min(self.timeout_seconds, 5.0)
            with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                latency_ms = (time.time() - t0) * 1000.0
                body = resp.read().decode("utf-8")
                res_json = json.loads(body)
                choices = res_json.get("choices", [])
                if choices:
                    content = choices[0].get("message", {}).get("content", "")
                    return True, content, latency_ms
                return False, "EMPTY_CHOICES_IN_RESPONSE", latency_ms
        except urllib.error.HTTPError as he:
            latency_ms = (time.time() - t0) * 1000.0
            return False, f"HTTP_{he.code}: {he.reason}", latency_ms
        except Exception as e:
            latency_ms = (time.time() - t0) * 1000.0
            return False, f"API_ERROR: {type(e).__name__}: {e}", latency_ms

    def evaluate_signal_confluence(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Quantitative second-opinion critic for live trade setups.
        Enforces a strict 5.0s SLA and fail-closed safety.
        """
        t0 = time.time()
        sym = signal.get("symbol", "UNKNOWN")
        direction = signal.get("direction", "UNKNOWN")
        confluence_score = float(signal.get("confluence_score", 0.0))
        headline = signal.get("signal_details", {}).get("title", "")
        reasons = signal.get("confluence_reasons", [])

        if not self.is_configured():
            logger.warning(f"[{sym}] DeepSeek critic fail-closed: DEEPSEEK_API_KEY not configured.")
            return {
                "ok": False,
                "status": "REJECTED_MISSING_API_KEY",
                "approved": False,
                "reason": "DEEPSEEK_API_KEY_NOT_CONFIGURED",
                "latency_ms": 0.0,
            }

        prompt = (
            f"You are an ultra-fast quantitative risk critic for crypto futures.\n"
            f"Asset: {sym} | Proposed Direction: {direction} | Confluence Score: {confluence_score}/100\n"
            f"Signal Headline: {headline}\n"
            f"Confluence Reasons: {', '.join(reasons[:4])}\n\n"
            f"Evaluate whether this trade is high-probability or has severe unstated risks.\n"
            f"Respond STRICTLY in JSON format:\n"
            f'{{"approved": true/false, "confidence": 0.0-1.0, "reason": "brief 10-word summary"}}'
        )

        messages = [
            {"role": "system", "content": "You are a quantitative trading risk auditor. Output strictly valid JSON."},
            {"role": "user", "content": prompt},
        ]

        success, response, latency_ms = self.call_deepseek_api(messages, max_tokens=150, temperature=0.1)

        if not success:
            logger.warning(f"[{sym}] DeepSeek critic failed ({response}) in {latency_ms:.1f}ms. Failing closed.")
            return {
                "ok": False,
                "status": "CRITIC_TIMEOUT_OR_ERROR",
                "approved": False,
                "reason": response,
                "latency_ms": latency_ms,
            }

        try:
            # Parse JSON out of markdown fences if present
            cleaned = response.strip()
            if "```" in cleaned:
                cleaned = cleaned.split("```")[1]
                if cleaned.startswith("json"):
                    cleaned = cleaned[4:]
                cleaned = cleaned.strip()

            data = json.loads(cleaned)
            return {
                "ok": True,
                "status": "CRITIQUE_COMPLETE",
                "approved": bool(data.get("approved", False)),
                "confidence": float(data.get("confidence", 0.0)),
                "reason": str(data.get("reason", "NO_REASON")),
                "latency_ms": latency_ms,
            }
        except Exception as pe:
            logger.error(f"[{sym}] Failed to parse DeepSeek response: {response} ({pe})")
            return {
                "ok": False,
                "status": "PARSE_ERROR",
                "approved": False,
                "reason": f"PARSE_ERROR: {pe}",
                "latency_ms": latency_ms,
            }

    def spawn_contained_process(self, args: list, **kwargs) -> subprocess.Popen:
        """
        Spawns child subprocess within Win32 Job Object (<= 150 MB RAM budget and kill-on-close).
        """
        proc = subprocess.Popen(args, **kwargs)
        if self.job_handle and sys.platform == "win32":
            assign_process_to_job(self.job_handle, proc.pid)
        self._active_children.append(proc)
        return proc

    def terminate_children(self):
        """Terminates all tracked child processes deterministically."""
        for proc in getattr(self, "_active_children", []):
            try:
                if proc.poll() is None:
                    terminate_process_tree(proc.pid)
            except Exception:
                pass
        self._active_children = []

    def close(self):
        """Cleans up child processes and Win32 Job Object handle."""
        self.terminate_children()
        if self.job_handle and sys.platform == "win32":
            try:
                import ctypes
                ctypes.windll.kernel32.CloseHandle(self.job_handle)
                self.job_handle = None
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
