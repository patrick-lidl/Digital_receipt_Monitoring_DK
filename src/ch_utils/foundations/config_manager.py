import logging
import os
from pathlib import Path
from typing import List, Union

from box import Box
from box.exceptions import BoxKeyError, BoxValueError


class Config(Box):
    """
    A dictionary-like object that allows for attribute-style access.

    This class is an alias for `box.Box`, providing a convenient way to work
    with configuration data using dot notation (e.g., `config.database.host`)
    instead of dictionary-style access (e.g., `config['database']['host']`).
    """

    pass


def load_configs(file_name: Union[List[str], str]) -> Config:
    """
    Loads and merges one or more YAML configuration files.

    This function constructs file paths from a base directory and loads the
    specified YAML files. The base path is determined by the `BASE_PATH`
    environment variable, defaulting to the current working directory. All
    configs are expected to be in a `config/` subdirectory.

    If multiple files are provided, their contents are merged sequentially.
    Keys from later files in the list will override keys from earlier ones.

    Example:
        `load_configs("logging")` will load `config/logging.yml`.
        `load_configs("clusters/train")` will load `config/clusters/train.yml`.
        `load_configs(["common", "production"])` will load `config/common.yml`
        and then merge `config/production.yml` on top of it.

    Args:
        file_name: The name(s) of the configuration file(s) to load,
            without the `.yml` extension. The path is relative to the `config/`
            directory.

    Returns:
        A single `Config` object containing the merged configurations.
    """
    import yaml

    config = Config()
    base_path = Path(os.environ.get("BASE_PATH") or ".")
    all_files = file_name if isinstance(file_name, list) else [file_name]
    for file in all_files:
        current_path = base_path / f"config/{file}.yml"
        if current_path.exists():
            try:
                with open(current_path, encoding="utf8") as f:
                    config |= Config(yaml.safe_load(f))
            except BoxValueError:
                logging.warning(f"The config file '{current_path}' was empty.")
                continue

    return config


def _distribute_base_config(raw_config: Config, base_key: str) -> Config:
    """
    Merges a base configuration into all other top-level keys.

    Args:
        raw_config: The base configuration.
        base_key: The top-level key to use as the base configuration
                  (e.g., "base_config", "base_cluster").

    Returns:
        A Config object with the base settings applied to all other items.
    """
    try:
        base_config = raw_config.pop(base_key)
    except BoxKeyError:
        # If no base key is found, just return the original config
        return raw_config

    # Merge the base config into every other top-level key
    processed_config = {}
    for k, v in raw_config.items():
        if isinstance(v, dict):
            # Merge only if v is a dictionary
            processed_config[k] = base_config | v
        else:
            processed_config[k] = v

    return Config(processed_config)


def load_config_with_base(file_name: Union[List[str], str], base_key: str) -> Config:
    """
    Loads a YAML config and merges a base configuration into all other top-level keys.

    Args:
        file_name: The name(s) of the configuration file(s) to load.
        base_key: The top-level key to use as the base configuration
                  (e.g., "base_config", "base_cluster").

    Returns:
        A Config object with the base settings applied to all other items.
    """
    raw_config = load_configs(file_name)
    return _distribute_base_config(raw_config, base_key)
