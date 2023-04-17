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

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <pandas version> <optional numpy version = latest>"
  exit 1
fi

pandas=$1
numpy=${2:-"latest"}

if [ "${numpy}" = "nightly" ]; then
  pip install --extra-index-url https://pypi.anaconda.org/scipy-wheels-nightly/simple --pre numpy
elif [ "${numpy}" = "latest" ]; then
  pip install numpy
else
  pip install numpy==${numpy}
fi

if [ "${pandas}" = "upstream_devel" ]; then
  pip install git+https://github.com/pandas-dev/pandas.git
elif [ "${pandas}" = "nightly" ]; then
  pip install --extra-index-url https://pypi.anaconda.org/scipy-wheels-nightly/simple --pre pandas
elif [ "${pandas}" = "latest" ]; then
  pip install pandas
else
  pip install pandas==${pandas}
fi
