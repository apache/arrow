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
Check the type stubs against the PyArrow runtime with mypy's stubtest.

Usage:
    python scripts/run_stubtest.py [--wheel WHEEL] [stubtest options]

By default, checks the stubs in pyarrow-stubs/ against the installed PyArrow.
With --wheel, checks the stubs in a built wheel against the modules in the
same wheel, without installing it; the wheel builds do this.

Stubs that aren't written yet are skipped (--ignore-missing-stub), and the
known mismatches are listed in pyarrow-stubs-tests/stubtest_allowlist.txt.
Pass --generate-allowlist to print the entries for the current mismatches.
"""

import argparse
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

from mypy import stubtest

PYTHON_DIR = Path(__file__).resolve().parents[1]
STUBS_DIR = PYTHON_DIR / "pyarrow-stubs"
ALLOWLIST = PYTHON_DIR / "pyarrow-stubs-tests" / "stubtest_allowlist.txt"


def _import_pyarrow_from(path):
    sys.path.insert(0, str(path))
    import pyarrow
    if Path(pyarrow.__file__).parent != path / "pyarrow":
        raise RuntimeError(f"Imported pyarrow from {pyarrow.__file__} "
                           f"instead of {path}")


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument("--wheel", type=Path,
                        help="check the stubs and modules of this wheel")
    args, stubtest_args = parser.parse_known_args(argv)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        tmpdir = Path(tmpdir)
        stubs_dir = STUBS_DIR
        if args.wheel:
            runtime_dir = tmpdir / "runtime"
            with zipfile.ZipFile(args.wheel) as wheel:
                wheel.extractall(runtime_dir)
            _import_pyarrow_from(runtime_dir)
            stubs_dir = runtime_dir / "pyarrow"

        # mypy doesn't look for "-stubs" packages in its search path, so give
        # it the stubs under the name of the package, without the sources of
        # the modules that don't have stubs, e.g. pyarrow/fs.py: stubtest would
        # check those as if they were stubs. Run it from the directory with
        # the stubs, as mypy searches the working directory too.
        mypy_dir = tmpdir / "stubs"
        for stub in stubs_dir.rglob("*.pyi"):
            destination = mypy_dir / "pyarrow" / stub.relative_to(stubs_dir)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(stub, destination)

        options = stubtest.parse_options([
            "pyarrow",
            "--ignore-missing-stub",
            # Every Cython extension type is a disjoint base (PEP 800).
            "--ignore-disjoint-bases",
            "--allowlist", str(ALLOWLIST),
            "--mypy-config-file", str(PYTHON_DIR / "pyproject.toml"),
            *stubtest_args,
        ])
        cwd = os.getcwd()
        os.chdir(mypy_dir)
        try:
            return stubtest.test_stubs(options)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
