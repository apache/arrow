#!/usr/bin/env bash

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

pushd python/manylinux1
docker run --shm-size=2g --rm -e PYARROW_PARALLEL=3 -v $PWD:/io -v $PWD/../../:/arrow quay.io/xhochy/arrow_manylinux1_x86_64_base:llvm-7-manylinux1 /io/build_arrow.sh

# Testing for https://issues.apache.org/jira/browse/ARROW-2657
# These tests cannot be run inside of the docker container, since TensorFlow
# does not run on manylinux1

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

PYTHON_VERSION=3.6
CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION

conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION
conda activate $CONDA_ENV_DIR

pip install -q tensorflow
pip install "dist/`ls dist/ | grep cp36`"
python -c "import pyarrow; import tensorflow"
