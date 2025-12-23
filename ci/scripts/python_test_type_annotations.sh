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

if [ "${PYARROW_TEST_ANNOTATIONS}" == "ON" ]; then
  # Install library stubs
  pip install pandas-stubs scipy-stubs types-cffi types-psutil types-requests types-python-dateutil

  # Install type checkers
  pip install mypy pyright ty

  # Install other dependencies for type checking
  pip install fsspec

  # Run type checkers
  pushd ${pyarrow_dir}
  mypy
  pyright
  ty check;
else
  echo "Skipping type annotation tests";
fi
