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
# python ./dev/update_stub_docstrings.py -f ./python/pyarrow-stubs


from pathlib import Path
from textwrap import indent

import click
# TODO: perhaps replace griffe with importlib
import griffe
from griffe import AliasResolutionError
import libcst
from libcst import matchers as m


def _get_docstring(name, package, indentation):
    # print("extract_docstrings", name)
    try:
        obj = package.get_member(name)
    except (KeyError, ValueError, AliasResolutionError):
        # Some cython __init__ symbols can't be found
        # e.g. pyarrow.lib.OSFile.__init__
        stack = name.split(".")
        parent_name = ".".join(stack[:-1])

        try:
            obj = package.get_member(parent_name).all_members[stack[-1]]
        except (KeyError, ValueError, AliasResolutionError):
            print(f"{name} not found in {package.name}, it's probably ok.")
            return None

    if obj.has_docstring:
        docstring = obj.docstring.value
        # Remove signature if present in docstring
        if docstring.startswith(obj.name) or (
            (hasattr(obj.parent, "name") and
                docstring.startswith(f"{obj.parent.name}.{obj.name}"))):
            docstring = "\n".join(docstring.splitlines()[2:])
        # Skip empty docstrings
        if docstring.strip() == "":
            return None
        # Indent docstring
        indentation_prefix = indentation * "    "
        docstring = indent(docstring + '\n"""', indentation_prefix)
        docstring = '"""\n' + docstring
        return docstring
    return None


class ReplaceEllipsis(libcst.CSTTransformer):
    def __init__(self, package, namespace):
        self.package = package
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
                docstring = _get_docstring(name, self.package, 0)
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
            docstring = _get_docstring(name, self.package, self.indentation)
            if docstring is not None:
                new_node = libcst.SimpleString(value=docstring)
                updated_node = updated_node.deep_replace(
                    updated_node.body.body[0].body[0].value, new_node)

        if m.matches(updated_node, class_matcher_2):
            docstring = _get_docstring(name, self.package, self.indentation)
            if docstring is not None:
                new_docstring = libcst.SimpleString(value=docstring)
                new_body = [
                    libcst.SimpleWhitespace(self.indentation * "    "),
                    libcst.Expr(value=new_docstring),
                    libcst.Newline()
                ] + list(updated_node.body.body)
                new_body = libcst.IndentedBlock(body=new_body)
                updated_node = updated_node.with_changes(body=new_body)

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
            docstring = _get_docstring(name, self.package, self.indentation)
            if docstring is not None:
                new_docstring = libcst.SimpleString(value=docstring)
                new_body = [
                    libcst.SimpleWhitespace(self.indentation * "    "),
                    libcst.Expr(value=new_docstring),
                    libcst.Newline()
                ]
                new_body = libcst.IndentedBlock(body=new_body)
                updated_node = updated_node.with_changes(body=new_body)

        self.stack.pop()
        self.indentation -= 1
        return updated_node


@click.command()
@click.option('--pyarrow_folder', '-f', type=click.Path(resolve_path=True))
def add_docs_to_stub_files(pyarrow_folder):
    print("Updating docstrings of stub files in:", pyarrow_folder)
    package = griffe.load("pyarrow", try_relative_path=True,
                          force_inspection=True, resolve_aliases=True)
    lib_modules = ["array", "builder", "compat", "config", "device", "error", "io",
                   "_ipc", "memory", "pandas_shim", "scalar", "table", "tensor",
                   "_types"]

    for stub_file in Path(pyarrow_folder).rglob('*.pyi'):
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

        modified_tree = tree.visit(ReplaceEllipsis(package, module))
        with open(stub_file, "w") as f:
            f.write(modified_tree.code)
        print("\n")


if __name__ == "__main__":
    docstrings_map = {}
    add_docs_to_stub_files(obj={})
