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
Build backend wrapper that resolves license symlinks before delegating
to scikit-build-core.

Arrow's LICENSE.txt and NOTICE.txt live at the repository root, one level
above python/. They are symlinked into python/ so that license-files in
pyproject.toml can reference them otherwise project metadata fails validation.
This is done before any build backend is invoked that's why symlinks are necessary.
But when building sdist tarballs symlinks are not copied and we end up with
broken LICENSE.txt and NOTICE.txt.

This custom build backend replaces the symlinks with actual file copies before
scikit_build_core.build.build_sdist so that the sdist contains the real file content.
The symlinks are restored afterwards to keep the git working tree clean.
"""

import base64
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import shutil
import sys
import tempfile
import zipfile

from scikit_build_core.build import *  # noqa: F401,F403
from scikit_build_core.build import build_sdist as scikit_build_sdist
from scikit_build_core.build import build_wheel as scikit_build_wheel

LICENSE_FILES = ("LICENSE.txt", "NOTICE.txt")
PYTHON_DIR = Path(__file__).resolve().parent.parent
PYTHON_STUBS_DIR = PYTHON_DIR / "pyarrow-stubs" / "pyarrow"


@contextmanager
def prepare_licenses():
    # Temporarily copy the files so they are included on sdist.
    for name in LICENSE_FILES:
        parent_license = PYTHON_DIR.parent / name
        pyarrow_license = PYTHON_DIR / name
        pyarrow_license.unlink(missing_ok=True)
        shutil.copy2(parent_license, pyarrow_license)
    try:
        yield
    finally:
        if sys.platform != "win32":
            # Copy back the original symlinks so git status is clean.
            for name in LICENSE_FILES:
                filepath = PYTHON_DIR / name
                os.unlink(filepath)
                os.symlink(f"../{name}", filepath)


def build_sdist(sdist_directory, config_settings=None):
    with prepare_licenses():
        return scikit_build_sdist(sdist_directory, config_settings)


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    wheel_name = scikit_build_wheel(
        wheel_directory, config_settings, metadata_directory
    )
    wheel_path = Path(wheel_directory) / wheel_name
    _inject_stub_docstrings(wheel_path)
    return wheel_name


def _inject_stub_docstrings(wheel_path):
    """Extract wheel, copy stubs, inject docstrings from runtime, repack."""
    if not PYTHON_STUBS_DIR.exists():
        return

    if os.environ.get("PYARROW_SKIP_STUB_DOCSTRINGS", "0") == "1":
        print("-- Skipping stub docstring injection (PYARROW_SKIP_STUB_DOCSTRINGS=1)")
        return

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Extract wheel into temp dir
        with zipfile.ZipFile(wheel_path, "r") as whl:
            whl.extractall(tmp_path)

        # Copy .pyi stubs alongside the built pyarrow package
        pyarrow_dir = tmp_path / "pyarrow"
        for stub_file in PYTHON_STUBS_DIR.rglob("*.pyi"):
            rel = stub_file.relative_to(PYTHON_STUBS_DIR)
            dest = pyarrow_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(stub_file, dest)

        # Inject docstrings extracted from the built pyarrow runtime
        try:
            from pyarrow._build_utils.update_stub_docstrings import (
                add_docstrings_from_build,
            )
            add_docstrings_from_build(pyarrow_dir, tmp_path)
        except ImportError as e:
            print(f"-- Skipping stub docstring injection ({e})")

        # Repack the modified contents back into the wheel file
        _repack_wheel(wheel_path, tmp_path)


def _repack_wheel(wheel_path, extracted_dir):
    """Repack a wheel from an extracted directory, regenerating RECORD checksums."""
    dist_info_dirs = list(extracted_dir.glob("*.dist-info"))
    if not dist_info_dirs:
        raise RuntimeError("No .dist-info directory found in extracted wheel")
    record_path = dist_info_dirs[0] / "RECORD"
    record_rel = record_path.relative_to(extracted_dir)

    # Compute hashes for all files except RECORD itself
    all_files = sorted(
        f for f in extracted_dir.rglob("*")
        if f.is_file() and f != record_path
    )
    record_lines = []
    for f in all_files:
        rel = f.relative_to(extracted_dir)
        data = f.read_bytes()
        digest = base64.urlsafe_b64encode(
            hashlib.sha256(data).digest()
        ).rstrip(b"=").decode()
        record_lines.append(f"{rel},sha256={digest},{len(data)}")
    record_lines.append(f"{record_rel},,")
    record_path.write_text("\n".join(record_lines) + "\n")

    # Overwrite the original wheel file
    tmp = wheel_path.with_suffix(".tmp")
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as whl:
        for f in sorted(extracted_dir.rglob("*")):
            if f.is_file():
                whl.write(f, f.relative_to(extracted_dir))
    tmp.replace(wheel_path)
