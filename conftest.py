"""conftest.py — pytest session setup.

Databricks Serverless does not support importing workspace .py files via
sys.path. This conftest pre-loads any local module using importlib and
registers it in sys.modules so test files can use normal import syntax.
"""
import sys
import importlib.util
from pathlib import Path

try:
    HERE = Path(__file__).parent  # /Workspace/Users/bkvaria13@gmail.com
except NameError:
    # __file__ is not defined when executed directly in a Databricks notebook cell
    HERE = Path("/Workspace/Users/bkvaria13@gmail.com")


def _register(module_name: str) -> None:
    """Load a .py file from src/ and register it in sys.modules."""
    path = HERE / "src" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules[module_name] = mod


_register("transformations")
