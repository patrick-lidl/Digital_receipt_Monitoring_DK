from __future__ import annotations

import importlib.util
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

if TYPE_CHECKING:
    from jinja2 import Environment
    from markupsafe import Markup


logger = logging.getLogger(__name__)


# ============================================================================
# Basic filters (kept in this module so jinja_filters.py can remain “custom only”)
# ============================================================================


def required(value: Any, name: Optional[str] = None) -> Any:
    """
    Ensure that a template value is present and non-empty.

    Treats ``None``, empty strings, and whitespace-only strings as missing.

    Args:
        value: The value to validate.
        name: Optional parameter name for clearer error messages.

    Returns:
        The original value if it is present and non-empty.

    Raises:
        ValueError: If the value is missing or empty.
    """
    missing = value is None or (isinstance(value, str) and value.strip() == "")
    if missing:
        label = f" '{name}'" if name else ""
        raise ValueError(f"Required parameter{label} is missing or empty.")
    return value


def ensure_date(value: Any, fmt: str = "%Y-%m-%d") -> Optional[str]:
    """
    Validate a date string.

    Validates that ``value`` is a string matching the supplied ``fmt``. If the
    value is ``None`` or an empty string, it is returned unchanged so that a
    preceding or subsequent ``required`` filter can enforce presence.

    Args:
        value: The date string to validate (e.g., '2025-08-11').
        fmt: The expected format (default: '%Y-%m-%d').

    Returns:
        The original date string if valid, or ``None``/empty string unchanged.

    Raises:
        ValueError: If ``value`` is not a string or does not match ``fmt``.
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return value  # Let `required` enforce presence if needed.
    if not isinstance(value, str):
        raise ValueError(f"Expected date string in format {fmt}, got {type(value).__name__}")
    try:
        datetime.strptime(value, fmt)
        return value
    except ValueError as e:
        raise ValueError(f"Invalid date '{value}': {e}") from e


def as_sql_in_list(value: Any) -> "Markup":
    """
    Render a Python value as a SQL ``IN (...)`` list payload.

    Accepts a list/tuple/set, a comma-separated string, a single scalar,
    or ``None``. Numbers are emitted unquoted; strings are single-quoted
    with internal quotes escaped.

    Examples:
        - ``[1, 2, 3]`` -> ``1, 2, 3``
        - ``"A, B , C"`` -> ``'A', 'B', 'C'``
        - ``None`` -> ```` (empty string)

    Args:
        value: The source value(s) to format.

    Returns:
        A ``Markup`` object containing the comma-separated list ready to place
        inside ``IN (...)``. ``Markup`` is used to prevent HTML escaping when
        autoescape is enabled (although for SQL you should keep autoescape off).
    """
    # Normalize to a flat list of values
    items: list[Any] = []
    if value is None:
        items = []
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    elif isinstance(value, str):
        # Split comma-separated strings and trim
        parts = [p.strip() for p in value.split(",")]
        items = [p for p in parts if p]  # drop empties
    else:
        items = [value]

    def _fmt(x: Any) -> str:
        # Integers (including negative string integers)
        try:
            if isinstance(x, int):
                return str(x)
            if isinstance(x, str) and x.strip().lstrip("-").isdigit():
                return str(int(x))
        except Exception:
            pass

        # Floats (only if it's already a float; avoid parsing arbitrary strings)
        if isinstance(x, float):
            return str(x)

        # Fallback to quoted string, escape single quotes
        s = str(x).replace("'", "''")
        return f"'{s}'"

    rendered = ", ".join(_fmt(x) for x in items)

    from markupsafe import Markup  # marks string as "safe" under autoescape

    return Markup(rendered)


# ============================================================================
# SqlRenderer
# ============================================================================


class SqlRenderer:
    """
    Render Jinja-based SQL templates with built-in and custom filters.

    This class encapsulates a Jinja2 environment configured for SQL rendering:
    - Autoescape is disabled (recommended for SQL).
    - ``StrictUndefined`` is enabled, so missing variables raise unless guarded
      by filters (e.g., ``|default``, ``|required``).
    - Basic filters (``required``, ``ensure_date``, ``as_sql_in_list``) are
      always registered.
    - Optional custom filters are loaded from ``$SQL_PATH/jinja_filters.py`` if
      present. Only names declared in ``__all__`` are imported.

    Typical usage:
        renderer = SqlRenderer()
        sql = renderer.render_query(Path("path/to/query.sql.j2"), params)

    """

    def render_query(self, template_path: Path, params: Dict[str, Any]) -> str:
        """
        Render a single Jinja template file to a SQL string.

        Creates a fresh Jinja environment for the template directory, registers
        the basic filters, optionally loads custom filters, and renders with the
        provided parameters. Also performs an advisory preflight check to log
        undeclared variables.

        Args:
            template_path: Path to the Jinja template file (``.sql``, ``.j2``, etc.).
            params: Mapping of template variables to values.

        Returns:
            The rendered SQL string.

        Raises:
            ValueError: If a template variable is missing/undefined at render time.
            SyntaxError: If there is a Jinja syntax error in the template.
            RuntimeError: If the rendered SQL still contains unresolved Jinja tokens.
        """
        from jinja2 import (
            Environment,
            FileSystemLoader,
            StrictUndefined,
            TemplateSyntaxError,
            UndefinedError,
        )

        template_dir = template_path.parent
        template_file = template_path.name

        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=False,  # SQL: do not HTML-escape
            undefined=StrictUndefined,  # fail fast if not guarded by filters
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Register built-in (basic) filters first
        self._register_basic_filters(env)

        # Load custom filters (if provided) from $SQL_PATH/jinja_filters.py
        sql_path = Path(os.environ.get("SQL_PATH") or "sql")
        module_path = os.path.join(sql_path, "jinja_filters.py")
        self._load_custom_filters(env, module_path)

        # Optional: advisory preflight (undeclared vars). Not authoritative—the
        # template may guard vars with filters like |default.
        self._preflight_missing_vars(env, template_file, params)

        try:
            template = env.get_template(template_file)
            sql = template.render(**params)
        except UndefinedError as e:
            # Raised if a variable is missing AND not handled by your filters
            raise ValueError(f"Template variable undefined/missing in '{template_file}': {e}") from e
        except TemplateSyntaxError as e:
            raise SyntaxError(f"Jinja syntax error in '{template_file}' at line {e.lineno}: {e.message}") from e

        # Post-render sanity: no stray Jinja tokens should remain
        if any(tok in sql for tok in ("{{", "}}", "{%", "%}")):
            raise RuntimeError(f"Rendered SQL for '{template_file}' still contains unresolved Jinja delimiters.")

        return sql

    # -------------------------- Internals --------------------------

    @staticmethod
    def _register_basic_filters(env: "Environment") -> None:
        """
        Register basic filters that are always available in SQL templates.

        Filters:
            - required: Enforce presence of a value.
            - ensure_date: Validate 'YYYY-MM-DD' (by default).
            - as_sql_in_list: Format values for SQL IN (...) payloads.

        Args:
            env: The Jinja environment to mutate.
        """
        env.filters.setdefault("required", required)
        env.filters.setdefault("ensure_date", ensure_date)
        env.filters.setdefault("as_sql_in_list", as_sql_in_list)

    @staticmethod
    def _load_filters_from_module(
        module_path: str,
        module_name: str,
        filter_type: str,
        registry: Dict[str, Callable],
    ) -> None:
        """
        Generic helper to load callables from a Python module into a registry.

        The module should export callable filter functions via its ``__all__``
        list. Each function named in ``__all__`` is registered.

        Args:
            module_path: Absolute or relative path to the filters module.
            module_name: The "name" of the module (e.g., "jinja_filters").
            filter_type: A string for logging (e.g., "custom" or "context").
            registry: The dictionary to add the filters to (e.g., env.filters).
        """
        if not os.path.exists(module_path):
            logger.debug("No %s filters module found at %s", filter_type, module_path)
            return

        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if not spec or not spec.loader:
            logger.warning("Could not load module from path: %s", module_path)
            return

        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
        except Exception as e:
            logger.exception("Failed to import %s filters from %s: %s", filter_type, module_path, e)
            return

        exported = getattr(module, "__all__", [])
        if not isinstance(exported, (list, tuple)):
            logger.warning("Custom %s filters module %s has no valid __all__; skipping.", filter_type, module_path)
            return

        for name in exported:
            func = getattr(module, name, None)
            if callable(func):
                # Allow custom filters to override basic filters intentionally.
                registry[name] = func
                logger.debug("Registered %s filter: %s", filter_type, name)
            else:
                logger.warning("Skipping non-callable filter '%s' in %s", name, module_path)

    @staticmethod
    def _load_custom_filters(env: "Environment", module_path: str) -> None:
        """
         Load custom filters from a Python module if present.

        The module should export callable filter functions via its ``__all__``
        list. Each function named in ``__all__`` is registered into the Jinja
        environment as a filter with the same name.

        Args:
            env: The Jinja environment to mutate.
            module_path: Absolute or relative path to the custom filters module
                (typically ``$SQL_PATH/jinja_filters.py``).

        Notes:
            - If the module or ``__all__`` is missing, no error is raised.
            - Custom filters are added after basic filters. If a custom filter
              shares a name with a basic one, it will overwrite it.
        """
        SqlRenderer._load_filters_from_module(
            module_path=module_path,
            module_name="jinja_filters",
            filter_type="custom",
            registry=env.filters,
        )

    @staticmethod
    def _load_context_filters(env: "Environment", module_path: str) -> None:
        """
        Load custom *context* filters from a Python module if present.

        This is identical to _load_custom_filters, but it registers
        the functions to the `env.contextfilters` dictionary, allowing
        them to receive the full Jinja context as the first argument.

        Args:
            env: The Jinja environment to mutate.
            module_path: Absolute or relative path to the custom context
                         filters module (typically `$SQL_PATH/context_filters.py`).

        Raises:
            NotImplementedError: The previous implementation did not work.
        """
        raise NotImplementedError

    @staticmethod
    def _preflight_missing_vars(env: "Environment", template_file: str, params: Dict[str, Any]) -> None:
        """
        Log undeclared template variables not found in provided params.

        This is an *advisory* check only. It cannot detect whether the template
        guards a variable with filters like ``|default`` or ``|required``. The
        authoritative check remains the render-time behavior with ``StrictUndefined``.

        Args:
            env: The Jinja environment.
            template_file: The template filename relative to the environment loader root.
            params: Mapping of parameters intended for rendering.
        """
        from jinja2 import meta

        try:
            source, _, _ = env.loader.get_source(env, template_file)  # type: ignore[arg-type]
            parsed = env.parse(source)
            referenced = meta.find_undeclared_variables(parsed)
            builtins = {"loop", "cycler", "namespace", "True", "False", "None", "true", "false", "none"}
            missing = sorted(v for v in referenced if v not in params and v not in builtins)
            if missing:
                logger.info(
                    "[Preflight] Template references variables not in params: %s "
                    "(they may still be supplied via filters like |default)",
                    missing,
                )
        except Exception as e:
            logger.debug("Preflight check skipped due to: %s", e)
