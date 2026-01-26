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

"""
Extract docstrings from pyarrow runtime and insert them into stub files.

Usage (from python/ directory with pyarrow built):
    python ../dev/update_stub_docstrings.py pyarrow-stubs
"""

import argparse
import importlib
import inspect
import shutil
import sys
from pathlib import Path
from textwrap import indent

import libcst
from libcst import matchers as m


def _resolve_object(module, path):
    """Resolve an object by dotted path from a module."""
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
            try:
                obj = vars(parent).get(part)
                if obj is not None:
                    continue
            except TypeError:
                pass
            return None, None, None

    return obj, parent, getattr(obj, "__name__", parts[-1])


def _get_docstring(name, module, indentation):
    """Extract and format a docstring for insertion into a stub file."""
    obj, parent, obj_name = _resolve_object(module, name)
    if obj is None:
        print(f"{name} not found in {module.__name__}")
        return None

    docstring = inspect.getdoc(obj)
    if not docstring:
        return None

    # Remove signature prefix
    parent_name = getattr(parent, "__name__", None) if parent else None
    if docstring.startswith(obj_name) or (
        parent_name and docstring.startswith(f"{parent_name}.{obj_name}")
    ):
        docstring = "\n".join(docstring.splitlines()[2:])

    # Skip empty docstrings
    if not docstring.strip():
        return None

    prefix = "    " * indentation
    return '"""\n' + indent(docstring + '\n"""', prefix)


class DocstringInserter(libcst.CSTTransformer):
    """CST transformer that inserts docstrings into stub file nodes."""

    def __init__(self, module, namespace):
        self.module = module
        self.base_namespace = namespace
        self.stack = []
        self.indentation = 0

    def _full_name(self):
        name = ".".join(self.stack)
        return f"{self.base_namespace}.{name}" if self.base_namespace else name

    def leave_Module(self, original_node, updated_node):
        new_body = []
        clone_matcher = m.SimpleStatementLine(
            body=[m.Assign(value=m.Call(func=m.Name(value="_clone_signature"))),
                  m.ZeroOrMore()]
        )
        for stmt in updated_node.body:
            new_body.append(stmt)
            if m.matches(stmt, clone_matcher):
                name = stmt.body[0].targets[0].target.value
                if self.base_namespace:
                    name = f"{self.base_namespace}.{name}"
                docstring = _get_docstring(name, self.module, 0)
                if docstring:
                    new_body.append(libcst.SimpleStatementLine(
                        body=[libcst.Expr(value=libcst.SimpleString(docstring))]))
        return updated_node.with_changes(body=new_body)

    def visit_ClassDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_ClassDef(self, original_node, updated_node):
        name = self._full_name()
        docstring = _get_docstring(name, self.module, self.indentation)

        if docstring:
            ellipsis_class = m.ClassDef(body=m.IndentedBlock(
                body=[m.SimpleStatementLine(body=[m.Expr(m.Ellipsis()), m.ZeroOrMore()]),
                      m.ZeroOrMore()]))
            func_class = m.ClassDef(body=m.IndentedBlock(body=[m.FunctionDef(), m.ZeroOrMore()]))

            if m.matches(updated_node, ellipsis_class):
                updated_node = updated_node.deep_replace(
                    updated_node.body.body[0].body[0].value,
                    libcst.SimpleString(value=docstring))
            elif m.matches(updated_node, func_class):
                docstring_stmt = libcst.SimpleStatementLine(
                    body=[libcst.Expr(value=libcst.SimpleString(value=docstring))])
                updated_node = updated_node.with_changes(
                    body=updated_node.body.with_changes(
                        body=[docstring_stmt] + list(updated_node.body.body)))

        self.stack.pop()
        self.indentation -= 1
        return updated_node

    def visit_FunctionDef(self, node):
        self.stack.append(node.name.value)
        self.indentation += 1

    def leave_FunctionDef(self, original_node, updated_node):
        name = self._full_name()
        ellipsis_func = m.FunctionDef(body=m.SimpleStatementSuite(body=[m.Expr(m.Ellipsis())]))

        if m.matches(original_node, ellipsis_func):
            docstring = _get_docstring(name, self.module, self.indentation)
            if docstring:
                docstring_stmt = libcst.SimpleStatementLine(
                    body=[libcst.Expr(value=libcst.SimpleString(value=docstring))])
                updated_node = updated_node.with_changes(
                    body=libcst.IndentedBlock(body=[docstring_stmt]))

        self.stack.pop()
        self.indentation -= 1
        return updated_node


LIB_MODULES = {"array", "builder", "compat", "config", "device", "error", "io",
               "_ipc", "memory", "pandas_shim", "scalar", "table", "tensor", "_types"}


def add_docstrings_to_stubs(stubs_dir):
    """Update all stub files in stubs_dir with docstrings from pyarrow runtime."""
    stubs_dir = Path(stubs_dir)
    print(f"Updating stub docstrings in: {stubs_dir}")

    pyarrow = importlib.import_module("pyarrow")

    for stub_file in stubs_dir.rglob('*.pyi'):
        if stub_file.name == "_stubs_typing.pyi":
            continue

        module_name = stub_file.stem
        if module_name in LIB_MODULES:
            namespace = "lib"
        elif stub_file.parent.name in ("parquet", "interchange"):
            namespace = f"{stub_file.parent.name}.{module_name}"
        elif module_name == "__init__":
            namespace = ""
        else:
            namespace = module_name

        print(f"  {stub_file.name} -> {namespace or '(root)'}")
        tree = libcst.parse_module(stub_file.read_text())
        modified = tree.visit(DocstringInserter(pyarrow, namespace))
        stub_file.write_text(modified.code)


def copy_stubs(src_dir, dest_dir):
    """Copy .pyi files from src_dir to dest_dir."""
    src_dir, dest_dir = Path(src_dir), Path(dest_dir)
    if not src_dir.exists():
        return

    print(f"Copying stubs: {src_dir} -> {dest_dir}")
    for src in src_dir.rglob('*.pyi'):
        dest = dest_dir / src.relative_to(src_dir)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)


def update_stubs_for_build(stubs_dir, build_lib):
    """Entry point for setup.py: update docstrings and copy stubs to build dir."""
    stubs_dir, build_lib = Path(stubs_dir), Path(build_lib)

    sys.path.insert(0, str(build_lib))
    try:
        add_docstrings_to_stubs(stubs_dir)
        copy_stubs(stubs_dir / "pyarrow", build_lib / "pyarrow")
    finally:
        sys.path.pop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stubs_dir", type=Path, help="Path to pyarrow-stubs folder")
    args = parser.parse_args()

    sys.path.insert(0, ".")
    add_docstrings_to_stubs(args.stubs_dir.resolve())
