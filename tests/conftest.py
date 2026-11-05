"""
Pytest configuration and compatibility hook for socket isolation and offline test execution.
"""
import sys
from pathlib import Path

# Guarantee repository root and archived legacy scrapers are on sys.path for all tests
ROOT_DIR = Path(__file__).resolve().parent.parent
LEGACY_DIR = ROOT_DIR / "legacy_archive" / "cdp_scrapers"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(LEGACY_DIR) not in sys.path:
    sys.path.insert(0, str(LEGACY_DIR))


def pytest_addoption(parser):
    """
    Registers --disable-socket and --allow-unix-socket options gracefully
    when pytest-socket plugin is not installed in the local environment.
    """
    group = parser.getgroup("socket_compat", "Socket Compatibility Hooks")
    try:
        group.addoption(
            "--disable-socket",
            action="store_true",
            default=False,
            help="Disable socket calls (compatibility stub when pytest-socket is not installed)"
        )
    except (ValueError, Exception):
        pass

    try:
        group.addoption(
            "--allow-unix-socket",
            action="store_true",
            default=False,
            help="Allow unix sockets (compatibility stub when pytest-socket is not installed)"
        )
    except (ValueError, Exception):
        pass
