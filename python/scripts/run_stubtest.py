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
Check the type stubs in pyarrow-stubs/ against the installed PyArrow with
mypy's stubtest.

Usage:
    python scripts/run_stubtest.py [stubtest options]

Stubs that aren't written yet are skipped (--ignore-missing-stub), and the
known mismatches are listed in pyarrow-stubs-tests/stubtest_allowlist.txt.
Pass --generate-allowlist to print the entries for the current mismatches.
"""

import os
import sys
import tempfile
from pathlib import Path

from mypy import stubtest

import pyarrow.lib

PYTHON_DIR = Path(__file__).resolve().parents[1]
STUBS_DIR = PYTHON_DIR / "pyarrow-stubs"
ALLOWLIST = PYTHON_DIR / "pyarrow-stubs-tests" / "stubtest_allowlist.txt"


def main(argv):
    # The stubs of pyarrow.lib are split into the stub-only pyarrow.__lib
    # package, one module per .pxi file. Alias those modules to pyarrow.lib,
    # otherwise stubtest can't import them and skips everything they define.
    for stub in (STUBS_DIR / "__lib").glob("*.pyi"):
        name = "pyarrow.__lib"
        if stub.stem != "__init__":
            name += f".{stub.stem}"
        sys.modules[name] = pyarrow.lib

    with tempfile.TemporaryDirectory() as tmpdir:
        # mypy doesn't look for "-stubs" packages on MYPYPATH, so give it the
        # stubs under the name of the package.
        os.symlink(STUBS_DIR, Path(tmpdir) / "pyarrow")
        os.environ["MYPYPATH"] = tmpdir
        options = stubtest.parse_options([
            "pyarrow",
            "--ignore-missing-stub",
            # Every Cython extension type is a disjoint base (PEP 800).
            "--ignore-disjoint-bases",
            "--allowlist", str(ALLOWLIST),
            "--mypy-config-file", str(PYTHON_DIR / "pyproject.toml"),
            *argv,
        ])
        return stubtest.test_stubs(options)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
