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
Build backend wrapper that copies license files from the repo root
before delegating to scikit-build-core.

Arrow's LICENSE.txt and NOTICE.txt live at the repository root, one level above
python/.  The pyproject-metadata validator requires that files listed in
``project.license-files`` exist inside the project directory, so we copy them
in before anything else happens.
"""

import shutil
from pathlib import Path

from scikit_build_core.build import *  # noqa: F401,F403

_PYTHON_DIR = Path(__file__).resolve().parent.parent
_REPO_ROOT = _PYTHON_DIR.parent

for _name in ("LICENSE.txt", "NOTICE.txt"):
    _src = _REPO_ROOT / _name
    _dst = _PYTHON_DIR / _name
    # If file doesn't exist, example on an sdist, this is just no-op.
    if _src.exists() and not _dst.exists():
        shutil.copy2(_src, _dst)
