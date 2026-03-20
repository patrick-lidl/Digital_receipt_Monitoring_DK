"""Utility functions for saving and loading files."""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Union


def get_unique_filename(filename: str, output_dir: Union[str, Path], max_attempts: int = 1000) -> str:
    """
    Generate a unique filename in the specified output directory.

    This function checks if a file with the given filename already exists in the
    output directory. If it does, it appends a counter to the base name of the
    file to create a unique filename.

    Args:
        filename (str): The name of the file.
        output_dir (Union[str, Path]): The directory where the file will be saved.
        max_attempts (int): The maximum number of attempts to find a unique filename.

    Returns:
        str: A unique filename that does not conflict with existing files in the
        output directory.

    Raises:
        Exception: If a unique filename cannot be found within the maximum number of attempts.
    """
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    base, ext = os.path.splitext(filename)
    counter = 1
    unique_filename = filename
    while (output_dir / unique_filename).exists():
        unique_filename = f"{base} ({counter}){ext}"
        counter += 1
        if counter > max_attempts:
            raise RuntimeError(f"Could not find a unique filename for {filename} after {max_attempts} attempts.")
    return str(output_dir / unique_filename)


def save_json_to_path(output_dict: Dict, output_path: Union[str, Path], print_pretty: bool) -> None:
    """
    Save a dictionary as a JSON file to the specified path.

    This function writes the contents of a dictionary to a JSON file. It can
    optionally format the JSON output to be more readable.

    Args:
        output_dict (Dict): The dictionary to be saved as a JSON file.
        output_path (Union[str, Path]): The file path where the JSON file will be saved.
        print_pretty (bool): If True, the JSON file will be formatted with
                             indentation for readability. If False, the JSON
                             file will be compact.

    Returns:
        None

    Raises:
        OSError: If there is an error opening or writing to the file.

    """
    if isinstance(output_path, str):
        output_path = Path(output_path)
    try:
        with output_path.open("w", encoding="utf-8") as file:
            if print_pretty:
                json.dump(output_dict, file, indent=2)
            else:
                json.dump(output_dict, file)
        logging.info(f"Wrote dictionary to {output_path}")
    except OSError as e:
        logging.error(f"Error writing to {output_path}: {e}")
        raise
