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
Add the docstrings of the pyarrow runtime to the installed type stubs with
docstring-adder (https://github.com/astral-sh/docstring-adder).

Usage:
    python scripts/update_stub_docstrings.py <install_prefix> <source_dir>
"""

import argparse
import ast
import os
import shutil
import sys
import tempfile
from pathlib import Path


def _link_or_copy(source, destination):
    # Prefer symlinks (faster, no disk use) but fall back to copying when the
    # filesystem doesn't support them (e.g. Docker volumes, network mounts).
    if sys.platform != "win32":
        try:
            os.symlink(source, destination)
            return
        except OSError:
            pass

    if source.is_dir():
        shutil.copytree(source, destination, symlinks=(sys.platform != "win32"))
    else:
        shutil.copy2(source, destination)


def _create_importable_pyarrow(pyarrow_pkg, source_dir, install_pyarrow_dir):
    """
    Assemble an importable pyarrow package inside a temporary directory.

    During wheel builds the .py sources and compiled binary artifacts live in
    separate trees (source checkout vs CMake install prefix). This function
    symlinks (or copies) both into pyarrow_pkg folder so that a plain
    ``import pyarrow`` works and docstrings can be extracted at build time.
    """
    source_pyarrow = source_dir / "pyarrow"
    if not source_pyarrow.exists():
        raise FileNotFoundError(f"PyArrow source package not found: {source_pyarrow}")

    for source_path in sorted(source_pyarrow.iterdir()):
        if source_path.suffix == ".py":
            _link_or_copy(source_path, pyarrow_pkg / source_path.name)
        elif source_path.is_dir() and not source_path.name.startswith((".", "__")):
            _link_or_copy(source_path, pyarrow_pkg / source_path.name)

    for artifact in sorted(install_pyarrow_dir.iterdir()):
        if not artifact.is_file() or artifact.suffix == ".pyi":
            continue

        destination = pyarrow_pkg / artifact.name
        if not destination.exists():
            _link_or_copy(artifact, destination)


def _has_class_docstring(stub_file, class_name):
    tree = ast.parse(stub_file.read_text(encoding="utf-8"))
    return any(isinstance(node, ast.ClassDef) and node.name == class_name
               and ast.get_docstring(node) for node in tree.body)


def add_docstrings_to_stubs(stubs_dir):
    """Add the docstrings of the pyarrow runtime to the stubs in stubs_dir."""
    import add_docstrings

    # docstring-adder logs the modules it can't import and skips them, so
    # fail early if pyarrow.lib can't be imported.
    import pyarrow.lib
    print(f"Adding docstrings to the stubs in {stubs_dir} "
          f"from {pyarrow.lib.__file__}")

    try:
        add_docstrings._main(["--packages", str(stubs_dir)])
    except SystemExit as e:
        if e.code:
            raise

    if not _has_class_docstring(stubs_dir / "lib.pyi", "Array"):
        raise RuntimeError("docstring-adder didn't add docstrings to lib.pyi")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("install_prefix", type=Path,
                        help="CMAKE_INSTALL_PREFIX used by wheel build")
    parser.add_argument("source_dir", type=Path,
                        help="PyArrow source directory")
    args = parser.parse_args()

    install_prefix = args.install_prefix.resolve()
    source_dir = args.source_dir.resolve()
    install_pyarrow_dir = install_prefix / "pyarrow"
    if not install_pyarrow_dir.exists():
        install_pyarrow_dir = install_prefix
    # docstring-adder derives the module names of the stubs from their paths.
    if install_pyarrow_dir.name != "pyarrow":
        sys.exit(f"The stubs must be in a pyarrow directory: {install_pyarrow_dir}")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        pyarrow_pkg = Path(tmpdir) / "pyarrow"
        pyarrow_pkg.mkdir()
        _create_importable_pyarrow(pyarrow_pkg, source_dir, install_pyarrow_dir)

        sys.path.insert(0, tmpdir)
        try:
            add_docstrings_to_stubs(install_pyarrow_dir)
        finally:
            sys.path.pop(0)
