#!/usr/bin/env bash
#
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

set -ex
pyarrow_dir=${1}

if [ -n "${ARROW_PYTHON_VENV:-}" ]; then
  # shellcheck source=/dev/null
  . "${ARROW_PYTHON_VENV}/bin/activate"
fi

cd "${pyarrow_dir}"

# Install type checkers and library stubs. Note some libraries contain their
# own type hints so they need to be installed.
pip install -r requirements-stubs.txt

# Run type checkers
mypy
pyright --stats
ty check --verbose --output-format concise

# The type annotation tests are type checked above; also run them to verify
# the expected annotated types against the runtime types.
pytest -r s pyarrow-stubs-tests

# Check that the stubs match the signatures of the installed PyArrow.
python scripts/run_stubtest.py
