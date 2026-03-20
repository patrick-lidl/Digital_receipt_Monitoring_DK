import ast
import re
import sys
import tomllib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Set, Tuple

# --- Configuration Constants ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
TOML_FILE = PROJECT_ROOT / "pyproject.toml"
SRC_DIR = PROJECT_ROOT / "src"

# Modules provided by the runtime (e.g., Databricks) that should never be listed in TOML.
RUNTIME_ALLOWLIST = {
    "dbruntime",
    "databricks",
}

# Dependencies that are considered "used" if their parent package is used.
IMPLICITLY_USED = {
    "numpy": "pandas",
    "openpyxl": "pandas",
    "xlsxwriter": "pandas",
    "kaleido": "plotly",
    "utilsforecast": "statsforecast",
    "colorlog": "logging",
}

# Specific dependencies to ignore for specific modules.
# Example: deployment imports core for type checking, but we don't want it in the TOML.
IGNORED_DEPENDENCIES = {
    "ch_utils": {"pyspark"},
    "deployment": {"core"},
    "forecasting": {"neuralprophet"},  # deprecated
}

# Mapping from Python import names to PyPI package names.
PACKAGE_MAPPING = {
    "yaml": "pyyaml",
    "dotenv": "python-dotenv",
    "box": "python-box",
    "bs4": "beautifulsoup4",
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "google": "google-maps-places",
    "causalimpact": "tfcausalimpact",
    "dateutil": "python-dateutil",
    "PIL": "pillow",
    "markupsafe": "markupsafe",
    "concave_hull": "concave-hull",
    "mlflow": "mlflow-skinny",
}


class DependencyChecker:
    """
    Parses source code and pyproject.toml to audit Python dependencies.

    Attributes:
        internal_modules (Set[str]): Set of first-party module names defined in TOML.
        dependency_map (Dict): Resolved dependencies for each internal module.
        raw_deps (Dict): The raw dependency lists from pyproject.toml.
        all_opts (Dict): The full optional-dependencies dictionary for recursion.
        missing_report (Dict): Dependencies found in code but missing from TOML.
        unused_report (Dict): Dependencies in TOML but not found in code.
        used_packages_map (Dict): Map of actual imports found per module.
    """

    def __init__(self):
        """Initializes the checker by loading configuration and parsing the TOML file."""
        self.internal_modules: Set[str] = set()
        self.dependency_map: Dict[str, Dict[str, Any]] = {}
        self.raw_deps: Dict[str, Any] = {}
        self.all_opts: Dict[str, Any] = {}

        # State containers for results
        self.missing_report = defaultdict(lambda: defaultdict(list))
        self.unused_report = defaultdict(list)
        self.used_packages_map = defaultdict(set)

        self._load_toml()

    def run(self):
        """
        Executes the full dependency check workflow.

        1. Scans codebase for imports.
        2. checks for unused dependencies.
        3. Prints a report.
        4. Generates a clean TOML block.
        """
        print(f"Scanning {len(self.internal_modules)} internal modules...")
        self.scan_codebase()

        print("Checking for unused dependencies...")
        self.check_unused()

        self.print_report()
        self.generate_clean_toml()

    def scan_codebase(self):
        """
        Scans all Python files in src/ for internal modules.

        Populates self.used_packages_map and self.missing_report.
        """
        stdlib = sys.stdlib_module_names

        for module_name in self.internal_modules:
            module_path = SRC_DIR / module_name
            if not module_path.exists():
                continue

            deps_info = self.dependency_map[module_name]
            allowed_resolved = deps_info["resolved"]
            ignored_for_this_module = IGNORED_DEPENDENCIES.get(module_name, set())

            for py_file in module_path.rglob("*.py"):
                if "archive" in py_file.parts:
                    continue

                for imp_name, lineno in self._get_imports(py_file):
                    # 1. Filter Runtime/Stdlib
                    if imp_name in stdlib or imp_name in RUNTIME_ALLOWLIST:
                        continue

                    # 2. Filter Explicitly Ignored (e.g. deployment -> core)
                    if imp_name in ignored_for_this_module:
                        continue

                    # 3. Normalize
                    pkg_name = PACKAGE_MAPPING.get(imp_name, imp_name).lower()
                    self.used_packages_map[module_name].add(pkg_name)

                    # 4. Check for Missing
                    # It is MISSING if:
                    #   - Not in the resolved allowed list
                    #   - AND it is not the module itself (self-import)
                    if pkg_name not in allowed_resolved and pkg_name != module_name:
                        try:
                            rel_path = py_file.relative_to(PROJECT_ROOT)
                        except ValueError:
                            rel_path = py_file
                        self.missing_report[module_name][pkg_name].append(f"{rel_path}:{lineno}")

    def check_unused(self):
        """
        Identifies unused dependencies by comparing imports against TOML definitions.

        Populates self.unused_report.
        """
        cache = {}

        for module in self.internal_modules:
            current_list = self.raw_deps.get(module, [])
            used_in_this_module = self.used_packages_map.get(module, set())

            for dep in current_list:
                clean = self._normalize_name(dep)
                is_used = False

                # 1. Direct Usage
                if clean in used_in_this_module:
                    is_used = True

                # 2. Implicit Usage (e.g. openpyxl is used if pandas is used)
                elif clean in IMPLICITLY_USED:
                    required_parent = IMPLICITLY_USED[clean]
                    if required_parent in used_in_this_module:
                        is_used = True

                # 3. Group Usage (e.g. cdspch[core])
                # It is used if ANY package provided by 'core' is used in this module.
                elif clean in self.all_opts:
                    # Expand the group to see what packages it contains
                    group_contents = self._resolve_group(clean, self.all_opts, cache)
                    # If overlap exists between what group provides and what module uses -> It's Used.
                    if not group_contents.isdisjoint(used_in_this_module):
                        is_used = True

                if not is_used:
                    self.unused_report[module].append(dep)

    def print_report(self):
        """Prints the missing and unused dependencies to stdout."""
        if self.missing_report:
            print(f"\n{'MISSING DEPENDENCIES':^80}")
            print("=" * 80)
            for module, pkgs in sorted(self.missing_report.items()):
                print(f"\n[MODULE] {module}")
                for pkg, locs in sorted(pkgs.items()):
                    print(f"  X Missing: {pkg}")
                    for loc in locs[:2]:
                        print(f"      at {loc}")
        else:
            print("\nSUCCESS: No missing dependencies found! 🚀")

        if self.unused_report:
            print(f"\n{'UNUSED DEPENDENCIES':^80}")
            print("=" * 80)
            for module, pkgs in sorted(self.unused_report.items()):
                print(f"\n[MODULE] {module}")
                for pkg in sorted(pkgs):
                    print(f"  ? Unused:  {pkg}")

    def generate_clean_toml(self):
        """
        Generates and prints a clean [project.optional-dependencies] block.

        Features:
        - Removes unused dependencies.
        - Adds missing dependencies.
        - Removes redundant transitive dependencies (e.g. if A includes B, don't list B).
        """
        print(f"\n{' PROPOSED CLEAN TOML BLOCK ':^80}")
        print("=" * 80)
        print("[project.optional-dependencies]")

        cache = {}

        for group in sorted(self.internal_modules):
            current_list = set(self.raw_deps.get(group, []))
            unused_list = set(self.unused_report.get(group, []))

            # 1. Start with current, remove unused
            kept_list = current_list - unused_list

            # 2. Add missing
            missing_pkgs = self.missing_report.get(group, {})
            for pkg in missing_pkgs:
                if pkg in self.internal_modules:
                    kept_list.add(f"cdspch[{pkg}]")
                else:
                    kept_list.add(pkg)

            # 3. Remove Redundant Transitive Dependencies
            final_list = set(kept_list)
            sorted_candidates = sorted(list(kept_list))

            for parent in sorted_candidates:
                # Check if this item is a group (e.g. cdspch[core])
                match = re.match(r"cdspch\[(.*?)\]", parent)
                if match:
                    parent_group = match.group(1)
                    # Get everything this parent group provides
                    provided_by_parent = self._resolve_group(parent_group, self.all_opts, cache)

                    # Check if any OTHER item in our list is provided by this parent
                    for child in list(final_list):
                        if child == parent:
                            continue

                        child_clean = self._normalize_name(child)
                        if child_clean in provided_by_parent:
                            final_list.discard(child)

            if not final_list:
                continue

            print(f"{group} = [")
            for item in sorted(final_list):
                print(f'    "{item}",')
            print("]\n")

    def _load_toml(self):
        """Loads and parses the pyproject.toml file."""
        if not TOML_FILE.exists():
            sys.exit(f"Error: {TOML_FILE} not found.")

        with open(TOML_FILE, "rb") as f:
            data = tomllib.load(f)

        # 1. Source of Truth: known-first-party
        try:
            first_party = data["tool"]["ruff"]["lint"]["isort"]["known-first-party"]
            self.internal_modules = set(first_party)
        except KeyError:
            sys.exit("Error: [tool.ruff.lint.isort] known-first-party not found in pyproject.toml")

        # 2. Global Deps
        global_raw = data.get("project", {}).get("dependencies", [])
        global_deps = {self._normalize_name(d) for d in global_raw}

        # 3. Optional Deps
        self.all_opts = data.get("project", {}).get("optional-dependencies", {})

        # Filter only to internal modules + calculate resolved deps
        cache = {}
        for module in self.internal_modules:
            if not (SRC_DIR / module).exists():
                print(f"Warning: Module '{module}' listed in known-first-party but not found in src/")

            raw_list = self.all_opts.get(module, [])
            self.raw_deps[module] = raw_list

            resolved = self._resolve_group(module, self.all_opts, cache)

            self.dependency_map[module] = {"raw": raw_list, "resolved": resolved | global_deps}

    def _resolve_group(self, group_name: str, raw_deps_map: Dict, recursion_cache: Dict = None) -> Set[str]:
        """
        Recursively resolves all allowed packages for a given group.

        Args:
            group_name: The name of the group to resolve (e.g., 'core').
            raw_deps_map: The dictionary of all groups and their raw dependencies.
            recursion_cache: Cache to prevent infinite loops and re-computation.

        Returns:
            A set of all package names provided by this group and its children.
        """
        if recursion_cache is None:
            recursion_cache = {}
        if group_name in recursion_cache:
            return recursion_cache[group_name]

        allowed = set()
        raw_list = raw_deps_map.get(group_name, [])

        for dep in raw_list:
            clean_name = self._normalize_name(dep)
            allowed.add(clean_name)
            # If dependency is another group, recurse
            if clean_name in raw_deps_map:
                allowed.update(self._resolve_group(clean_name, raw_deps_map, recursion_cache))

        recursion_cache[group_name] = allowed
        return allowed

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalizes a dependency string. 'pandas>=2.0' -> 'pandas', 'cdspch[core]' -> 'core'."""
        match = re.match(r"cdspch\[(.*?)\]", name)
        if match:
            return match.group(1)
        return re.split(r"[<>=\[\s]", name)[0].lower()

    @staticmethod
    def _get_imports(filepath: Path) -> list[Tuple[str, int]]:
        """Parses a Python file using AST to find all top-level and function imports."""
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                root = ast.parse(f.read(), str(filepath))
            except SyntaxError:
                return []

        imports = []
        for node in ast.walk(root):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.append((alias.name.split(".")[0], node.lineno))
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.level == 0:
                    imports.append((node.module.split(".")[0], node.lineno))
        return imports


if __name__ == "__main__":
    checker = DependencyChecker()
    # checker.run()

    print(f"Scanning {len(checker.internal_modules)} internal modules...")
    checker.scan_codebase()

    print("Checking for unused dependencies...")
    checker.check_unused()

    checker.print_report()
    # checker.generate_clean_toml()
