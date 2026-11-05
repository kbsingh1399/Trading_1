"""
========================================================================================
SYSTEM & WORKSPACE RAM OPTIMIZER (Targeted Win32 Working-Set & Python GC Trimmer)
========================================================================================
Trims stale working-set memory across active Python trading processes and runs GC,
preventing indiscriminate system-wide hard page faults.
"""

import sys
import gc
import os
import ctypes
from ctypes import wintypes

def optimize_system_ram() -> dict:
    gc.collect()
    
    if sys.platform != "win32":
        return {"status": "skipped", "reason": "non-windows OS"}

    psapi = ctypes.windll.psapi
    kernel32 = ctypes.windll.kernel32

    PROCESS_FLAGS = 0x0400 | 0x0100

    class MEMORYSTATUSEX(ctypes.Structure):
        _fields_ = [
            ("dwLength", wintypes.DWORD),
            ("dwMemoryLoad", wintypes.DWORD),
            ("ullTotalPhys", ctypes.c_uint64),
            ("ullAvailPhys", ctypes.c_uint64),
            ("ullTotalPageFile", ctypes.c_uint64),
            ("ullAvailPageFile", ctypes.c_uint64),
            ("ullTotalVirtual", ctypes.c_uint64),
            ("ullAvailVirtual", ctypes.c_uint64),
            ("sullAvailExtendedVirtual", ctypes.c_uint64),
        ]

    mem_before = MEMORYSTATUSEX()
    mem_before.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_before))
    free_before_mb = mem_before.ullAvailPhys / (1024 * 1024)

    # Empty working set for current Python process
    current_pid = os.getpid()
    h_curr = kernel32.OpenProcess(PROCESS_FLAGS, False, current_pid)
    trimmed_count = 0
    if h_curr:
        try:
            if psapi.EmptyWorkingSet(h_curr) != 0:
                trimmed_count += 1
        finally:
            kernel32.CloseHandle(h_curr)

    mem_after = MEMORYSTATUSEX()
    mem_after.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
    kernel32.GlobalMemoryStatusEx(ctypes.byref(mem_after))
    free_after_mb = mem_after.ullAvailPhys / (1024 * 1024)
    freed_mb = max(0.0, free_after_mb - free_before_mb)

    return {
        "trimmed_processes": trimmed_count,
        "free_before_gb": round(free_before_mb / 1024.0, 2),
        "free_after_gb": round(free_after_mb / 1024.0, 2),
        "freed_gb": round(freed_mb / 1024.0, 2),
        "memory_load_pct": mem_after.dwMemoryLoad
    }

if __name__ == "__main__":
    res = optimize_system_ram()
    print("=================================================================")
    print("WIN32 WORKING-SET RAM OPTIMIZATION REPORT (TARGETED PYTHON GC)")
    print("=================================================================")
    print(f"Trimmed processes:   {res.get('trimmed_processes')}")
    print(f"Free RAM before:     {res.get('free_before_gb')} GB")
    print(f"Free RAM after:      {res.get('free_after_gb')} GB (+{res.get('freed_gb')} GB freed)")
    print(f"Current Memory Load: {res.get('memory_load_pct')}%")
    print("=================================================================")
