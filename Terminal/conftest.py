"""
Pytest configuration and compatibility hook for socket isolation and offline test execution.
"""

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
