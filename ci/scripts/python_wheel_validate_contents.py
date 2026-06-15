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

import argparse
# TODO(GH-48970): Uncomment when wheel stub validation is re-enabled.
# import ast
from pathlib import Path
import re
import zipfile


# TODO(GH-48970): Uncomment when wheel stub validation is re-enabled.
# def _count_docstrings(source):
#     """Count docstrings in module, function, and class bodies."""
#     tree = ast.parse(source)
#     count = 0
#     for node in ast.walk(tree):
#         if isinstance(node, (ast.Module, ast.FunctionDef,
#                              ast.AsyncFunctionDef, ast.ClassDef)):
#             if (node.body
#                     and isinstance(node.body[0], ast.Expr)
#                     and isinstance(node.body[0].value, ast.Constant)
#                     and isinstance(node.body[0].value.value, str)):
#                 count += 1
#     return count


def validate_wheel(path):
    p = Path(path)
    wheels = list(p.glob('*.whl'))
    error_msg = f"{len(wheels)} wheels found but only 1 expected ({wheels})"
    assert len(wheels) == 1, error_msg
    with zipfile.ZipFile(wheels[0]) as wheel_zip:
        outliers = [
            info.filename for info in wheel_zip.filelist if not re.match(
                r'(pyarrow/|pyarrow-[-.\w\d]+\.dist-info/|pyarrow\.libs/)', info.filename
            )
        ]
        assert not outliers, f"Unexpected contents in wheel: {sorted(outliers)}"
        for filename in ('LICENSE.txt', 'NOTICE.txt'):
            assert any(
                info.filename.split("/")[-1] == filename for info in wheel_zip.filelist
            ), f"{filename} is missing from the wheel."

        # TODO(GH-48970): Invert these checks when stubfiles are complete and
        # pyarrow-stubs are intentionally shipped in wheels again.
        assert not any(
            info.filename == "pyarrow/py.typed" for info in wheel_zip.filelist
        ), "pyarrow/py.typed must not be present in the wheel."

        wheel_stub_files = sorted(
            info.filename
            for info in wheel_zip.filelist
            if info.filename.startswith("pyarrow/") and info.filename.endswith(".pyi")
        )
        assert not wheel_stub_files, (
            "pyarrow .pyi files must not be present in the wheel: "
            f"{wheel_stub_files}"
        )

    print(f"The wheel: {wheels[0]} seems valid.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, required=True,
                        help="Directory where wheel is located")
    args = parser.parse_args()
    validate_wheel(args.path)


if __name__ == '__main__':
    main()
