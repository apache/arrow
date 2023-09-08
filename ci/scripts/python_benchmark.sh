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

# Check the ASV benchmarking setup.
# Unfortunately this won't ensure that all benchmarks succeed
# (see https://github.com/airspeed-velocity/asv/issues/449)
source deactivate
conda create -y -q -n pyarrow_asv python=$PYTHON_VERSION
conda activate pyarrow_asv
pip install -q git+https://github.com/pitrou/asv.git@customize_commands

export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_ORC=0
export PYARROW_WITH_GANDIVA=0

pushd $ARROW_PYTHON_DIR
# Workaround for https://github.com/airspeed-velocity/asv/issues/631
DEFAULT_BRANCH=$(git rev-parse --abbrev-ref origin/HEAD | sed s@origin/@@)
git fetch --depth=100 origin $DEFAULT_BRANCH:$DEFAULT_BRANCH
# Generate machine information (mandatory)
asv machine --yes
# Run benchmarks on the changeset being tested
asv run --no-pull --show-stderr --quick HEAD^!
popd  # $ARROW_PYTHON_DIR
