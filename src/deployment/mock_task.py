import ast
import logging
from typing import Dict, List, Optional, Type, Union

from ch_utils.common.string_utils import snake_to_camel


class TaskAnalyzer(ast.NodeVisitor):
    """Visits the Syntax Tree of a Task's source code to extract dependency metadata without executing the file."""

    def __init__(self, function_of_interest: str):
        """
        Initializes the analyzer.

        Args:
            function_of_interest: The name of the method to analyze for
                inputs and outputs (e.g., 'run_content').
        """
        super().__init__()
        self._function_of_interest = function_of_interest
        # Attributes are initialized here as instance variables to prevent
        # state leakage between different analyses.
        self.function_params: Dict[str, List[str]] = {}
        self.function_returns: Dict[str, List[Dict[str, str]]] = {}
        self.dependencies: List[str] = []

    def visit_ClassDef(self, node: ast.ClassDef):
        """Visit class definitions to find forced dependencies and function definitions."""
        for item in node.body:
            if isinstance(item, ast.AnnAssign):  # Annotated with type hint
                if (
                    isinstance(item.target, ast.Name)
                    and item.target.id == "forced_dependencies"
                    and item.value is not None  # AnnAssign value can be None
                    and isinstance(item.value, ast.List)
                ):
                    self.dependencies = [
                        elt.value
                        for elt in item.value.elts
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                    ]
            elif isinstance(item, ast.Assign):
                if (
                    len(item.targets) == 1  # Ensure single assignment target
                    and isinstance(item.targets[0], ast.Name)
                    and item.targets[0].id == "forced_dependencies"
                    and isinstance(item.value, ast.List)
                ):
                    self.dependencies = [
                        elt.value
                        for elt in item.value.elts
                        if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                    ]
            elif isinstance(item, ast.FunctionDef) and item.name == self._function_of_interest:
                self.function_params[item.name] = [arg.arg for arg in item.args.args]
                self.function_returns[item.name] = []
                for n in ast.walk(item):
                    if isinstance(n, ast.Return) and n.value is not None:
                        parsed_return = self.parse_return(n.value)
                        if isinstance(parsed_return, dict):
                            self.function_returns[item.name].append(parsed_return)
        self.generic_visit(node)

    def parse_return(self, node: ast.AST) -> Union[Dict[str, str], str]:
        """Parse the return statement to extract keys and values."""
        if isinstance(node, ast.Dict):
            keys = [self.parse_node(k) for k in node.keys if k is not None]
            values = [self.parse_node(v) for v in node.values if v is not None]
            return dict(zip(keys, values))
        return ast.dump(node)

    def parse_node(self, node: ast.AST) -> str:
        """Parse individual AST nodes to extract relevant information."""
        if isinstance(node, ast.Constant):
            return str(node.value)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Call):
            func_name = self.parse_node(node.func)
            args = [self.parse_node(arg) for arg in node.args]
            return f"{func_name}({', '.join(args)})"
        elif isinstance(node, ast.Attribute):
            value = self.parse_node(node.value)
            return f"{value}.{node.attr}"
        return ast.dump(node)

    def get_task_inputs(self) -> List[str]:
        """Get the input parameters of the function of interest."""
        if self._function_of_interest in self.function_params:
            output_list = self.function_params[self._function_of_interest]
            if "self" in output_list:
                output_list.remove("self")
            return output_list
        return []

    def get_task_outputs(self) -> List[str]:
        """Get the output keys of the function of interest."""
        if self._function_of_interest in self.function_returns and self.function_returns[self._function_of_interest]:
            # Assumes the first return statement's keys are representative
            return list(self.function_returns[self._function_of_interest][0].keys())
        return []


def analyze_task(file_path: str, function_of_interest: str) -> Dict[str, List[str]]:
    """
    Analyzes a Python file to extract task parameters, returns, and dependencies.

    This function reads the source code of a task, parses it into an
    Abstract Syntax Tree, and uses TaskAnalyzer to extract metadata without
    executing the code.

    Args:
        file_path: The path to the Python file to be analyzed.
        function_of_interest: The specific function name to look for inside the task class.

    Returns:
        A dictionary containing the task's inputs, outputs, and forced dependencies.
    """
    with open(file_path, "r") as file:
        tree = ast.parse(file.read(), filename=file_path)

    analyzer = TaskAnalyzer(function_of_interest=function_of_interest)
    analyzer.visit(tree)

    # Handle special cases where task outputs are derived by convention,
    # not from the return statement.
    if function_of_interest == "_calculate_metric":
        task_returns = [f"dbp_kpi_{file_path.removesuffix('.py')[-2:]}"]
    else:
        task_returns = analyzer.get_task_outputs()

    return {
        "task_params": analyzer.get_task_inputs(),
        "task_returns": task_returns,
        "forced_dependencies": analyzer.dependencies,
    }


class MockTask:
    """
    A lightweight, data-only representation of a Task.

    It holds the dependency metadata (inputs, outputs, etc.) needed to build
    a Job's dependency graph without requiring the original Task's heavy
    library dependencies to be installed.
    """

    task_name: str
    task_type = "standard"
    task_returns: List[str] = []
    task_params: Dict[str, Optional[Type]] = {}
    forced_dependencies: List[str] = []
    table_name: Optional[str] = None
    is_mocked: bool = True


def mock_task(module_name: str, analysis_function: str = "run_content") -> Type[MockTask]:
    """
    Creates a new MockTask class by analyzing a task's source code.

    If the source file cannot be found, it creates a blank mock with no
    dependency information to prevent the deployment from crashing.

    Args:
        module_name: The Python module name of the task (e.g., 'wka.wka').
        analysis_function: The function name to analyze for I/O, configurable
            for non-standard tasks.

    Returns:
        A new MockTask class populated with dependency metadata.
    """
    file_path = f"src/{module_name.replace('.', '/')}.py"
    try:
        task_dict = analyze_task(file_path, analysis_function)
    except FileNotFoundError:
        logging.error(f"Could not find source file for task: {file_path}. Creating a blank mock.")
        task_dict = {"task_params": [], "task_returns": [], "forced_dependencies": []}

    class NewMockTask(MockTask):
        pass

    NewMockTask.task_name = snake_to_camel(module_name.split(".")[-1], is_class=True)
    NewMockTask.task_params = {task_param: None for task_param in task_dict["task_params"]}
    NewMockTask.task_returns = task_dict["task_returns"]
    NewMockTask.forced_dependencies = task_dict["forced_dependencies"]
    return NewMockTask


def mock_data_loader_task(table_name: str) -> Type[MockTask]:
    """
    Creates a new MockTask class that simulates a DataLoader task.

    Args:
        table_name: The name of the table this mock task "loads".

    Returns:
        A new MockTask class configured as a data loader.
    """

    class NewMockTask(MockTask):
        pass

    NewMockTask.task_name = "Load" + snake_to_camel(table_name, is_class=True)
    NewMockTask.task_type = "data_loader"
    NewMockTask.task_returns = [table_name]
    NewMockTask.table_name = table_name
    return NewMockTask
