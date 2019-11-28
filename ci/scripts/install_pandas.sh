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

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <pandas version>"
  exit 1
fi

pandas=$1

if [ "${pandas}" = "master" ]; then
  conda install -q numpy
  pip install git+https://github.com/pandas-dev/pandas.git --no-build-isolation
elif [ "${pandas}" = "latest" ]; then
  conda install -q pandas
else
  conda install -q pandas=${pandas}
fi

conda clean --all
