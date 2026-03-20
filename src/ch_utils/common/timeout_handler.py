"""Handle timeout occurances."""

import signal
from typing import Any


class TimeoutException(Exception):
    """Raised when timeout is reached."""

    pass


def _timeout_handler(signum, frame):
    raise TimeoutException


def run_with_timeout(func, args=(), kwargs={}, timeout=5) -> Any:
    """
    Run a python function with a timeout.

    This function sets a signal alarm that will raise a TimeoutException
    if the function does not complete within the specified timeout period.

    Note:
        This utility is for Unix-like systems only, as it relies on the
        `signal.SIGALRM`, which is not available on Windows.

    Args:
        func (Callable): The function to execute.
        args (tuple): Positional arguments to pass to the function.
        kwargs (dict): Keyword arguments to pass to the function.
        timeout (int): The timeout duration in seconds.

    Returns:
        The result of the function if it completes in time.

    Raises:
        TimeoutException: If the function execution exceeds the timeout.
    """
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)
    try:
        result = func(*args, **kwargs)
    finally:
        # Always cancel the alarm to prevent it from firing later
        signal.alarm(0)
    return result
