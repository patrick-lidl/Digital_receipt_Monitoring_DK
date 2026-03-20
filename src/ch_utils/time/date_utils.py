"""Utility functions for handling dates."""

from datetime import date, datetime, timedelta
from typing import List

ISO_DATE_FORMAT = "%Y-%m-%d"


def get_year_cw(input_date: str) -> str:
    """
    Convert a date string to a string representing the ISO year and calendar week.

    Args:
        input_date (str): A date string in ISO format (e.g., '2025-05-14').

    Returns:
        str: A string in the format 'YYYY_WW', where 'YYYY' is the year and 'WW' is the
             zero-padded ISO calendar week number corresponding to the input date.

    Example:
        >>> get_year_cw("2025-05-14")
        '2025_20'
    """
    date = datetime.strptime(input_date, ISO_DATE_FORMAT)
    return f"{date.year}_{str(date.isocalendar().week).zfill(2)}"


def get_year_cw_list(mindate: str, maxdate: str) -> List[str]:
    """
    Return year-calendarweek list within range.

    For a given date range, computes all the year + calendar week in between in the following format:
    YYYYWW --> e.g., 201904 is the 4th calendar week in 2019.

    Args:
        mindate (str): Start date as a string in the format 'YYYY-MM-DD'.
        maxdate (str): End date as a string in the format 'YYYY-MM-DD'.

    Returns:
        List of calendar week strings in the format YYYYWW.
    """
    date_min = date.fromisoformat(mindate)
    date_max = date.fromisoformat(maxdate)
    year_cw = [
        str((date_min + timedelta(days=x)).isocalendar()[0] * 100 + (date_min + timedelta(days=x)).isocalendar()[1])
        for x in range((date_max - date_min).days + 1)
    ]
    return list(dict.fromkeys(year_cw))


def days_in_range(start_date, end_date) -> int:
    """
    Calculate the number of days between two dates, inclusive.

    Args:
        start_date (str): The start date in 'yyyy-mm-dd' format.
        end_date (str): The end date in 'yyyy-mm-dd' format.

    Returns:
        int: The number of days between the start and end dates, inclusive.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return (end - start).days + 1


def weeks_in_range(start_date, end_date) -> int:
    """
    Calculate the number of calendar weeks between two dates, inclusive.

    Args:
        start_date (str): The start date in 'yyyyww' (e.g. 202105) format.
        end_date (str): The end date in 'yyyyww' (e.g. 202105) format.

    Returns:
        int: The number of calendar weeks between the start and end dates, inclusive.
    """
    start = datetime.strptime(start_date + "-1", "%G%V-%w")
    end = datetime.strptime(end_date + "-1", "%G%V-%w")
    # Ensure the start date is the beginning of the week (Monday)
    start -= timedelta(days=start.weekday())

    # Ensure the end date is the end of the week (Sunday)
    end += timedelta(days=(6 - end.weekday()))

    return (end - start).days // 7 + 1


def months_in_range(start_date, end_date) -> int:
    """
    Calculate the number of calendar months between two dates, inclusive.

    Args:
        start_date (str): The start date in 'yyyy-mm' format.
        end_date (str): The end date in 'yyyy-mm' format.

    Returns:
        int: The number of calendar months between the start and end dates, inclusive.
    """
    start = datetime.strptime(start_date + "-1", "%Y-%m-%d")
    end = datetime.strptime(end_date + "-1", "%Y-%m-%d")

    # Total months difference
    total_months = (end.year - start.year) * 12 + (end.month - start.month)

    # Include the start month
    return total_months + 1


def get_previous_month() -> str:
    """
    Returns the previous month in 'YYYYMM' format.

    This function calculates the previous calendar month relative to the current date.
    It correctly handles the edge case where the current month is January by rolling
    back to December of the previous year.

    Returns:
        str: A string representing the previous month in 'YYYYMM' format.
    """
    now = datetime.now()
    year: int = now.year
    month: int = now.month

    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1

    return f"{year}{month:02d}"


def get_monday_n_weeks_ago(n: int) -> str:
    """Return date of monday N weeks ago in format YYYY-MM-DD."""
    today = date.today()
    monday_n_weeks_ago = today - timedelta(days=today.weekday() + (7 * n))
    return monday_n_weeks_ago.strftime("%Y-%m-%d")


def get_previous_week_monday() -> str:
    """Return date of previous monday in format YYYY-MM-DD."""
    return get_monday_n_weeks_ago(n=1)


def get_previous_week_sunday() -> str:
    """Return date of previous sunday in format YYYY-MM-DD."""
    previous_week_monday = get_previous_week_monday()
    previous_week_sunday = date.fromisoformat(previous_week_monday) + timedelta(days=6)
    return previous_week_sunday.strftime("%Y-%m-%d")


def nth_day_this_month(day: int) -> date:
    """
    Returns a date object representing the specified day of the current month.

    Args:
        day (int): The day of the month to return.

    Returns:
        date: A date object set to the specified day in the current month.
    """
    return date.today().replace(day=day)


def first_day_this_month() -> date:
    """
    Returns a date object representing the first day of the current month.

    Returns:
        date: A date object set to the first day of the current month.
    """
    return nth_day_this_month(day=1)


def first_day_previous_month() -> date:
    """
    Returns a date object representing the first day of the previous month.

    Returns:
        date: A date object set to the first day of the previous month.
    """
    current_date = date.today()
    year = current_date.year
    month = current_date.month - 1
    if month == 0:
        month = 12
        year -= 1
    return date(year, month, 1)


def last_day_previous_month() -> date:
    """
    Returns a date object representing the last day of the previous month.

    Returns:
        date: A date object set to the last day of the previous month.
    """
    return date.today().replace(day=1) - timedelta(days=1)


def subtract_weeks_from_date(to_date_str: str, look_back_weeks: int) -> str:
    """
    Calculates the start date for an inclusive lookback window.

    This function determines the correct `from_date` for data loading.
    It subtracts `(N * 7) - 1` days from the provided end date to
    create a date range that is fully inclusive of N weeks.

    This logic ensures consistency with the heuristic window calculation
    in `BuildCalendarDimension`. For example, a 1-week lookback
    (look_back_weeks=1) from a Sunday will return the prior Monday
    (6 days back), not the prior Sunday (7 days back).

    Args:
        to_date_str: The end date of the lookback period
            (e.g., "2025-11-09").
        look_back_weeks: The number of weeks to look back (e.g., 52).

    Returns:
        The calculated start date as a string in 'YYYY-MM-DD' format.
    """
    # 1. Convert the end date string to a date object
    to_date = datetime.strptime(to_date_str, "%Y-%m-%d").date()

    # 2. Calculate the days to subtract for an inclusive window
    days_to_subtract = (look_back_weeks * 7) - 1

    # 3. Subtract the days to find the start date
    from_date = to_date - timedelta(days=days_to_subtract)

    # 4. Return as a string
    return from_date.strftime("%Y-%m-%d")
