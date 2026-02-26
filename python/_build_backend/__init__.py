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

This custom build backend only replace the symlinks with hardlinks
before scikit_build_core.build.build_sdist so
that sdist contains the actual file content. The symlinks are restored
afterwards so the git working tree stays clean.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import shutil

from scikit_build_core.build import *  # noqa: F401,F403
from scikit_build_core.build import build_sdist as scikit_build_sdist

LICENSE_FILES = ("LICENSE.txt", "NOTICE.txt")
PYTHON_DIR = Path(__file__).resolve().parent.parent


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
        # Copy back the original symlinks so git status is clean.
        for name in LICENSE_FILES:
            filepath = PYTHON_DIR / name
            os.unlink(filepath)
            os.symlink(f"../{name}", filepath)


def build_sdist(sdist_directory, config_settings=None):
    with prepare_licenses():
        return scikit_build_sdist(sdist_directory, config_settings)
