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

# Testing for https://issues.apache.org/jira/browse/ARROW-2657
# These tests cannot be run inside of the docker container, since TensorFlow
# does not run on manylinux1

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh
source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

pushd python/manylinux1

cat << EOF > check_imports.py
import sys
import pyarrow
import pyarrow.orc
import pyarrow.parquet
import pyarrow.plasma

if sys.version_info.major > 2:
    import pyarrow.gandiva
EOF

for PYTHON_TUPLE in ${PYTHON_VERSIONS}; do
  IFS="," read PYTHON_VERSION UNICODE_WIDTH <<< $PYTHON_TUPLE

  # cleanup the artifact directory, docker writes it as root
  sudo rm -rf dist

  # build the wheels
  docker run --shm-size=2g --rm \
    -e PYARROW_PARALLEL=3 \
    -e PYTHON_VERSION=$PYTHON_VERSION \
    -e UNICODE_WIDTH=$UNICODE_WIDTH \
    -v $PWD:/io \
    -v $PWD/../../:/arrow \
    quay.io/pravindra/arrow_manylinux1_x86_64_base:latest \
    /io/build_arrow.sh

  # create a testing conda environment
  CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION
  conda create -y -q -p $CONDA_ENV_DIR python=$PYTHON_VERSION
  conda activate $CONDA_ENV_DIR

  # install the produced wheels
  pip install tensorflow
  pip install dist/*.whl

  # Test optional dependencies and the presence of tensorflow
  python check_imports.py

  # Install test dependencies and run pyarrow tests
  pip install -r ../requirements-test.txt
  pytest --pyargs pyarrow
done

popd
