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
from pathlib import Path
import re
import zipfile


def validate_wheel(path):
    p = Path(path)
    wheels = list(p.glob('*.whl'))
    error_msg = f"{len(wheels)} wheels found but only 1 expected ({wheels})"
    assert len(wheels) == 1, error_msg
    f = zipfile.ZipFile(wheels[0])

    outliers = [
        info.filename for info in f.filelist if not re.match(
            r'(pyarrow/|pyarrow-[-.\w\d]+\.dist-info/|pyarrow\.libs/)', info.filename
        )
    ]
    assert not outliers, f"Unexpected contents in wheel: {sorted(outliers)}"
    for filename in ('LICENSE.txt', 'NOTICE.txt'):
        assert any(info.filename.split("/")[-1] == filename
                   for info in f.filelist), \
            f"{filename} is missing from the wheel."

    assert any(info.filename == "pyarrow/py.typed" for info in f.filelist), \
        "pyarrow/py.typed is missing from the wheel."

    source_root = Path(__file__).resolve().parents[2]
    stubs_dir = source_root / "python" / "pyarrow-stubs" / "pyarrow"
    assert stubs_dir.exists(), f"Stub source directory not found: {stubs_dir}"

    expected_stub_files = {
        f"pyarrow/{stub_file.relative_to(stubs_dir).as_posix()}"
        for stub_file in stubs_dir.rglob("*.pyi")
    }

    wheel_stub_files = {
        info.filename
        for info in f.filelist
        if info.filename.startswith("pyarrow/") and info.filename.endswith(".pyi")
    }

    assert wheel_stub_files == expected_stub_files, (
        "Wheel .pyi files differ from python/pyarrow-stubs/pyarrow.\n"
        f"Missing in wheel: {sorted(expected_stub_files - wheel_stub_files)}\n"
        f"Unexpected in wheel: {sorted(wheel_stub_files - expected_stub_files)}"
    )

    docstring_injected_stub_files = []
    for wheel_stub_file in wheel_stub_files:
        stub_relpath = Path(wheel_stub_file).relative_to("pyarrow")
        source_stub_file = stubs_dir / stub_relpath
        source_content = source_stub_file.read_text(encoding="utf-8")
        wheel_content = f.read(wheel_stub_file).decode("utf-8")
        if wheel_content.count('"""') > source_content.count('"""'):
            docstring_injected_stub_files.append(wheel_stub_file)

    assert docstring_injected_stub_files, (
        "No injected docstrings were detected in wheel stub files. "
        "Expected at least one .pyi file in the wheel to contain more "
        "triple-quoted docstrings than its source stub counterpart."
    )

    print(f"The wheel: {wheels[0]} seems valid.")
    # TODO(GH-32609): Validate some docstrings were generated and added.

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, required=True,
                        help="Directory where wheel is located")
    args = parser.parse_args()
    validate_wheel(args.path)


if __name__ == '__main__':
    main()
