import os, sys
from pathlib import Path
ROOT = Path(os.environ.get("AUDIT_REPO_ROOT", Path(__file__).resolve().parents[2]))
sys.path[:0] = [str(ROOT / "API"), str(ROOT)]
