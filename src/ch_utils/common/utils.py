"""General Utility functions."""

from typing import Any, List

__client_mapping = {
    "AT": 1,
    "BE": 2,
    "BG": 3,
    "CH": 4,
    "CY": 5,
    "CZ": 6,
    "INT": 7,
    "DK": 8,
    "EE": 9,
    "ES": 10,
    "FI": 11,
    "FR": 12,
    "GB": 13,
    "GR": 14,
    "HR": 15,
    "HU": 16,
    "IE": 17,
    "IT": 18,
    "LT": 19,
    "LV": 20,
    "NI": 21,
    "NL": 22,
    "DE": 23,
    "PL": 24,
    "PT": 25,
    "RO": 26,
    "SE": 27,
    "SI": 28,
    "SK": 29,
    "CS": 31,
    "MT": 32,
    "US": 33,
    "RS": 34,
}


def get_client_id_from_code(country_code: str) -> int:
    """Return lidl client id from client code."""
    output = __client_mapping.get(country_code.upper())
    if output is None:
        raise ValueError
    return output


def list_wo_param(arr: List[Any], var: Any) -> List[Any]:
    """Filter a value from a list."""
    return [x for x in arr if x != var]


def try_round(number, ndigits: int | None = None):
    """Try to round, else return None."""
    try:
        return round(number, ndigits)
    except TypeError:
        return None


def is_subset(list1, list2) -> bool:
    """Check if one list is a subset of another list."""
    if not list1 or not list2:
        return False
    return set(list1).issubset(set(list2))


def convert_to_percentages(values: List[float]) -> List[float]:
    """Convert a list of numeric values to percentages."""
    total = sum(values)
    if total > 0:
        return [100 * x / total for x in values]
    else:
        return [0 for _ in values]
