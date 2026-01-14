# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Utility to extract docstrings from pyarrow and update
# docstrings in stubfiles.
#
# Usage
# =====
#
# python ../dev/update_stub_docstrings.py pyarrow-stubs


import argparse
import importlib
import inspect
from pathlib import Path
from textwrap import indent

import libcst
from libcst import matchers as m


def _resolve_object(module, path):
    """
    Resolve an object by dotted path from a base module.

    Parameters
    ----------
    module : module
        The base module (e.g., pyarrow)
    path : str
        Dotted path like "lib.Array" or "lib.concat_arrays"

    Returns
    -------
    tuple
        (obj, parent, obj_name) or (None, None, None) if not found
    """
    if not path:
        return module, None, module.__name__

    parts = path.split(".")
    parent = None
    obj = module

    for part in parts:
        parent = obj
        try:
            obj = getattr(obj, part)
        except AttributeError:
            # Fallback: try __dict__ access for special methods like __init__
            # that may not be directly accessible via getattr
            if hasattr(parent, "__dict__"):
                obj = parent.__dict__.get(part)
                if obj is not None:
                    continue
            # Try vars() as another fallback
            try:
                obj = vars(parent).get(part)
                if obj is not None:
                    continue
            except TypeError:
                pass
            return None, None, None

    # Get the object's simple name
    obj_name = getattr(obj, "__name__", parts[-1])
    return obj, parent, obj_name


def _get_docstring(name, module, indentation):
    """
    Extract and format docstring for a symbol.

    Parameters
    ----------
    name : str
        Dotted name like "lib.Array" or "lib.concat_arrays"
    module : module
        The pyarrow module
    indentation : int
        Number of indentation levels (4 spaces each)

    Returns
    -------
    str or None
        Formatted docstring ready for insertion, or None if not found
    """
    obj, parent, obj_name = _resolve_object(module, name)

    if obj is None:
        print(f"{name} not found in {module.__name__}, it's probably ok.")
        return None

    # Get docstring using inspect.getdoc for cleaner formatting
    docstring = inspect.getdoc(obj)
    if not docstring:
        return None

    # Get parent name for signature detection
    parent_name = getattr(parent, "__name__", None) if parent else None

    # Remove signature if present in docstring
    # Cython/pybind11 often include signatures like "func_name(...)\n\n..."
    if docstring.startswith(obj_name) or (
        parent_name is not None and docstring.startswith(f"{parent_name}.{obj_name}")
    ):
        docstring = "\n".join(docstring.splitlines()[2:])

    # Skip empty docstrings
    if not docstring.strip():
        return None

    # Format as docstring with proper indentation
    indentation_prefix = indentation * "    "
    docstring = indent(docstring + '\n"""', indentation_prefix)
    docstring = '"""\n' + docstring

    return docstring


class ReplaceEllipsis(libcst.CSTTransformer):
    def __init__(self, module, namespace):
        self.module = module
        self.base_namespace = namespace
        self.stack = []
        self.indentation = 0

    # Insert module level docstring if _clone_signature is used
    def leave_Module(self, original_node, updated_node):
        new_body = []
        clone_matcher = m.SimpleStatementLine(
            body=[m.Assign(
                value=m.Call(func=m.Name(value="_clone_signature"))
            ), m.ZeroOrMore()]
        )
        for statement in updated_node.body:
            new_body.append(statement)
            if m.matches(statement, clone_matcher):
                name = statement.body[0].targets[0].target.value
                if self.base_namespace:
                    name = f"{self.base_namespace}.{name}"
                docstring = _get_docstring(name, self.module, 0)
                if docstring is not None:
                    new_expr = libcst.Expr(value=libcst.SimpleString(docstring))
                    new_line = libcst.SimpleStatementLine(body=[new_expr])
                    new_body.append(new_line)

        return updated_node.with_changes(body=new_body)

    def visit_ClassDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_ClassDef(self, original_node, updated_node):
        name = ".".join(self.stack)
        if self.base_namespace:
            name = self.base_namespace + "." + name

        class_matcher_1 = m.ClassDef(
            name=m.Name(),
            body=m.IndentedBlock(
                body=[m.SimpleStatementLine(
                    body=[m.Expr(m.Ellipsis()), m.ZeroOrMore()]
                ), m.ZeroOrMore()]
            )
        )
        class_matcher_2 = m.ClassDef(
            name=m.Name(),
            body=m.IndentedBlock(
                body=[m.FunctionDef(), m.ZeroOrMore()]
            )
        )

        if m.matches(updated_node, class_matcher_1):
            docstring = _get_docstring(name, self.module, self.indentation)
            if docstring is not None:
                new_node = libcst.SimpleString(value=docstring)
                updated_node = updated_node.deep_replace(
                    updated_node.body.body[0].body[0].value, new_node)

        if m.matches(updated_node, class_matcher_2):
            docstring = _get_docstring(name, self.module, self.indentation)
            if docstring is not None:
                new_docstring = libcst.SimpleString(value=docstring)
                new_docstring_stmt = libcst.SimpleStatementLine(
                    body=[libcst.Expr(value=new_docstring)]
                )
                new_body = [new_docstring_stmt] + list(updated_node.body.body)
                updated_node = updated_node.with_changes(
                    body=updated_node.body.with_changes(body=new_body)
                )

        self.stack.pop()
        self.indentation -= 1
        return updated_node

    def visit_FunctionDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_FunctionDef(self, original_node, updated_node):
        name = ".".join(self.stack)
        if self.base_namespace:
            name = self.base_namespace + "." + name

        function_matcher = m.FunctionDef(
            name=m.Name(),
            body=m.SimpleStatementSuite(
                body=[m.Expr(
                    m.Ellipsis()
                )]))
        if m.matches(original_node, function_matcher):
            docstring = _get_docstring(name, self.module, self.indentation)
            if docstring is not None:
                new_docstring = libcst.SimpleString(value=docstring)
                new_docstring_stmt = libcst.SimpleStatementLine(
                    body=[libcst.Expr(value=new_docstring)]
                )
                new_body = libcst.IndentedBlock(body=[new_docstring_stmt])
                updated_node = updated_node.with_changes(body=new_body)

        self.stack.pop()
        self.indentation -= 1
        return updated_node


def add_docs_to_stub_files(pyarrow_folder):
    """
    Update stub files with docstrings extracted from pyarrow runtime.

    Parameters
    ----------
    pyarrow_folder : Path
        Path to the pyarrow-stubs folder
    """
    print("Updating docstrings of stub files in:", pyarrow_folder)

    # Load pyarrow using importlib
    pyarrow_module = importlib.import_module("pyarrow")

    lib_modules = ["array", "builder", "compat", "config", "device", "error", "io",
                   "_ipc", "memory", "pandas_shim", "scalar", "table", "tensor",
                   "_types"]

    for stub_file in pyarrow_folder.rglob('*.pyi'):
        if stub_file.name == "_stubs_typing.pyi":
            continue
        module = stub_file.with_suffix('').name
        print(f"[{stub_file} {module}]")

        with open(stub_file, 'r') as f:
            tree = libcst.parse_module(f.read())

        if module in lib_modules:
            module = "lib"
        elif stub_file.parent.name in ["parquet", "interchange"]:
            module = f"{stub_file.parent.name}.{module}"
        elif module == "__init__":
            module = ""

        modified_tree = tree.visit(ReplaceEllipsis(pyarrow_module, module))
        with open(stub_file, "w") as f:
            f.write(modified_tree.code)
        print("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract docstrings from pyarrow and update stub files."
    )
    parser.add_argument(
        "pyarrow_folder",
        type=Path,
        help="Path to the pyarrow-stubs folder"
    )
    args = parser.parse_args()

    add_docs_to_stub_files(args.pyarrow_folder.resolve())
