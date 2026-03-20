"""Define functions that can be used during jinja rendering."""

import datetime
from typing import Any, Literal, Optional

from jinja2.runtime import Undefined

from ch_utils.time.date_utils import (
    first_day_previous_month,
    first_day_this_month,
    get_monday_n_weeks_ago,
    get_previous_week_monday,
    get_previous_week_sunday,
    last_day_previous_month,
    nth_day_this_month,
)

__all__ = [
    "default_previous_week_monday",
    "default_previous_week_sunday",
    "get_bfs_zeitraum_start",
    "get_bfs_zeitraum_ende",
    "default_two_weeks_ago_monday",
    "default_four_weeks_ago_monday",
    "get_kolli_sort_start_date",
]


def _is_missing(value: Any) -> bool:
    """Return True if value is Jinja Undefined, None, or empty string."""
    return isinstance(value, Undefined) or value is None or (isinstance(value, str) and value.strip() == "")


def default_previous_week_monday(input_date: Optional[str]) -> str:
    """Return previous Monday if input is missing; otherwise return input."""
    return get_previous_week_monday() if _is_missing(input_date) else input_date


def default_two_weeks_ago_monday(input_date: Optional[str]) -> str:
    """Return previous Monday if input is missing; otherwise return input."""
    return get_monday_n_weeks_ago(n=2) if _is_missing(input_date) else input_date


def default_four_weeks_ago_monday(input_date: Optional[str]) -> str:
    """Return previous Monday if input is missing; otherwise return input."""
    return get_monday_n_weeks_ago(n=4) if _is_missing(input_date) else input_date


def default_previous_week_sunday(input_date: Optional[str]) -> str:
    """Return previous Sunday if input is missing; otherwise return input."""
    return get_previous_week_sunday() if _is_missing(input_date) else input_date


def get_bfs_zeitraum_start(zeitraum: Literal["EZ", "GM"]) -> str:
    """Return start of BFS time period."""
    if zeitraum == "GM":
        return first_day_previous_month().strftime("%Y-%m-%d")
    if zeitraum == "EZ":
        return first_day_this_month().strftime("%Y-%m-%d")
    raise ValueError(f"Invalid value for zeitraum: {zeitraum}")


def get_bfs_zeitraum_ende(zeitraum: Literal["EZ", "GM"]) -> str:
    """Return end of BFS time period."""
    if zeitraum == "GM":
        return last_day_previous_month().strftime("%Y-%m-%d")
    if zeitraum == "EZ":
        return nth_day_this_month(day=14).strftime("%Y-%m-%d")
    raise ValueError(f"Invalid value for zeitraum: {zeitraum}")


def get_kolli_sort_start_date(max_number_lookback_weeks: int, end_date_str: str) -> str:
    """
    Calculates the start date based on an end date and a week lookback.

    Args:
        max_number_lookback_weeks: The number of weeks (the value piped in).
        end_date_str: The end date, passed as an argument.

    Returns:
        The calculated start date as an ISO date string (YYYY-MM-DD).
    """
    # 1. Get the end_date from the argument.
    if not end_date_str:
        raise ValueError("end_date_str was not provided to filter.")

    # 2. Convert to a date object (handling both string and date)
    if isinstance(end_date_str, (datetime.date, datetime.datetime)):
        end_date = end_date_str
    else:
        try:
            end_date = datetime.date.fromisoformat(str(end_date_str))
        except ValueError:
            raise ValueError(f"Invalid end_date format: {end_date_str}")

    # 3. Calculate the lookback days
    lookback_days = (int(max_number_lookback_weeks) * 7) - 1

    # 4. Calculate the start date
    start_date = end_date - datetime.timedelta(days=lookback_days)

    # 5. Return as an ISO string for the SQL query
    return start_date.isoformat()
