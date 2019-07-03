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

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set up testing data
pushd arrow
git submodule update --init
popd
export ARROW_TEST_DATA=/arrow/testing/data

## Set up huge pages for plasma test
#sudo sysctl -w vm.nr_hugepages=2048
#sudo mkdir -p /mnt/hugepages
#sudo mount -t hugetlbfs -o uid=`id -u` -o gid=`id -g` none /mnt/hugepages
#sudo bash -c "echo `id -g` > /proc/sys/vm/hugetlb_shm_group"
#sudo bash -c "echo 2048 > /proc/sys/vm/nr_hugepages"

python --version

pytest -v --pyargs pyarrow

# Basic sanity checks
python -c "import pyarrow.parquet"
python -c "import pyarrow.plasma"
python -c "import pyarrow.orc"

# Ensure we do eagerly import pandas (or other expensive imports)
python < /arrow/python/scripts/test_imports.py

echo "PLASMA_VALGRIND: $PLASMA_VALGRIND"

# Check Cython coverage was correctly captured in $COVERAGE_FILE
coverage report -i --include="*/lib.pyx"
coverage report -i --include="*/memory.pxi"
coverage report -i --include="*/_parquet.pyx"

# Generate XML file for CodeCov
coverage xml -i -o $SOURCE_DIR/coverage.xml

# Capture C++ coverage info
pushd $SOURCE_DIR
lcov --directory . --capture --no-external --output-file coverage-python-tests.info \
    2>&1 | grep -v "ignoring data for external file"
lcov --add-tracefile coverage-python-tests.info \
    --output-file $SOURCE_DIR/coverage.info
rm coverage-python-tests.info
popd

# Build docs
pushd ../cpp/apidoc
doxygen
popd
cd ../docs
sphinx-build -q -b html -d _build/doctrees -W source _build/html

popd  # $ARROW_PYTHON_DIR

# Check the ASV benchmarking setup.
# Unfortunately this won't ensure that all benchmarks succeed
# (see https://github.com/airspeed-velocity/asv/issues/449)
source deactivate
conda create -y -q -n pyarrow_asv python=$PYTHON_VERSION
conda activate pyarrow_asv
pip install -q git+https://github.com/pitrou/asv.git@customize_commands

pushd $SOURCE_DIR
# Workaround for https://github.com/airspeed-velocity/asv/issues/631
git fetch --depth=100 origin master:master
# Generate machine information (mandatory)
asv machine --yes
# Run benchmarks on the changeset being tested
asv run --no-pull --show-stderr --quick HEAD^!
popd  # $ARROW_PYTHON_DIR

#- $TRAVIS_BUILD_DIR/ci/travis_upload_cpp_coverage.sh
