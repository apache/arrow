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

set -e

if [ "$#" -ne 1 ] && [ "$#" -ne 2 ]; then
  echo "Usage: $0 <numba version> [numba-cuda version]"
  exit 1
fi

numba=$1

if [ -n "${ARROW_PYTHON_VENV:-}" ]; then
  # We don't need to follow this external file.
  # See also: https://www.shellcheck.net/wiki/SC1091
  #
  # shellcheck source=/dev/null
  . "${ARROW_PYTHON_VENV}/bin/activate"
fi

if [ "${numba}" = "master" ]; then
  pip install https://github.com/numba/numba/archive/main.tar.gz#egg=numba
elif [ "${numba}" = "latest" ]; then
  pip install numba
else
  pip install "numba==${numba}"
fi

if [ "$#" -eq 1 ]; then
  exit 0
fi

numba_cuda=$2

if [ "${numba_cuda}" = "master" ]; then
  pip install https://github.com/NVIDIA/numba-cuda/archive/main.tar.gz#egg=numba-cuda
elif [ "${numba_cuda}" = "latest" ]; then
  pip install numba-cuda
else
  pip install "numba-cuda==${numba_cuda}"
fi
