"""Utilities for handling strings."""

import re


def camel_to_snake(camel_case: str) -> str:
    """
    Convert camel case string to snake case.

    Args:
        camel_case (str): The camel case string to be converted

    Returns:
        str: The snake case representation of the input string
    """
    # Insert underscores before capital letters
    return re.sub(r"(?<!^)(?=[A-Z])", "_", camel_case).lower()


def snake_to_camel(snake_case: str, is_class: bool = False) -> str:
    """
    Convert snake case string to camel case.

    Args:
        snake_case (str): The snake case string to be converted
        is_class (bool): If class, first letter should be capitalized.

    Returns:
        str: The camel case representation of the input string
    """
    components = snake_case.split("_")
    if is_class:
        return "".join(x.title() for x in components)
    else:
        return components[0] + "".join(x.title() for x in components[1:])
