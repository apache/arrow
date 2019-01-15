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

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh

export ARROW_HOME=$ARROW_CPP_INSTALL
export PARQUET_HOME=$ARROW_CPP_INSTALL
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH
export PYARROW_CXXFLAGS="-Werror"

PYARROW_PYTEST_FLAGS=" -r sxX --durations=15 --parquet"

PYTHON_VERSION=$1
CONDA_ENV_DIR=$TRAVIS_BUILD_DIR/pyarrow-test-$PYTHON_VERSION

# We should use zlib in the target Python directory to avoid loading
# wrong libpython on macOS at run-time. If we use zlib in
# $ARROW_BUILD_TOOLCHAIN and libpython3.6m.dylib exists in both
# $ARROW_BUILD_TOOLCHAIN and $CONDA_ENV_DIR, arrow-python-test uses
# libpython3.6m.dylib on $ARROW_BUILD_TOOLCHAIN not $CONDA_ENV_DIR.
# libpython3.6m.dylib on $ARROW_BUILD_TOOLCHAIN doesn't have NumPy. So
# python-test fails.
export ZLIB_HOME=$CONDA_ENV_DIR

if [ $ARROW_TRAVIS_PYTHON_JVM == "1" ]; then
  CONDA_JVM_DEPS="jpype1"
fi

conda create -y -q -p $CONDA_ENV_DIR -c conda-forge/label/cf201901 \
      --file $TRAVIS_BUILD_DIR/ci/conda_env_python.yml \
      nomkl \
      cmake \
      pip \
      numpy=1.14 \
      python=${PYTHON_VERSION} \
      ${CONDA_JVM_DEPS}

conda activate $CONDA_ENV_DIR

python --version
which python

if [ "$ARROW_TRAVIS_PYTHON_DOCS" == "1" ] && [ "$PYTHON_VERSION" == "3.6" ]; then
  # Install documentation dependencies
  conda install -y -c conda-forge/label/cf201901 --file ci/conda_env_sphinx.yml
fi

# ARROW-2093: PyTorch increases the size of our conda dependency stack
# significantly, and so we have disabled these tests in Travis CI for now

# if [ "$PYTHON_VERSION" != "2.7" ] || [ $TRAVIS_OS_NAME != "osx" ]; then
#   # Install pytorch for torch tensor conversion tests
#   # PyTorch seems to be broken on Python 2.7 on macOS so we skip it
#   conda install -y -q pytorch torchvision -c soumith
# fi

if [ $TRAVIS_OS_NAME != "osx" ]; then
  conda install -y -c conda-forge/label/cf201901 tensorflow
  PYARROW_PYTEST_FLAGS="$PYARROW_PYTEST_FLAGS --tensorflow"
fi

# Re-build C++ libraries with the right Python setup
mkdir -p $ARROW_CPP_BUILD_DIR
pushd $ARROW_CPP_BUILD_DIR

# Clear out prior build files
rm -rf *

# XXX Can we simply reuse CMAKE_COMMON_FLAGS from travis_before_script_cpp.sh?
CMAKE_COMMON_FLAGS="-DARROW_EXTRA_ERROR_CONTEXT=ON"

PYTHON_CPP_BUILD_TARGETS="arrow_python-all plasma parquet"

if [ $ARROW_TRAVIS_COVERAGE == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GENERATE_COVERAGE=ON"
fi

if [ $ARROW_TRAVIS_PYTHON_GANDIVA == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DARROW_GANDIVA=ON"
  PYTHON_CPP_BUILD_TARGETS="$PYTHON_CPP_BUILD_TARGETS gandiva"
fi

cmake -GNinja \
      $CMAKE_COMMON_FLAGS \
      -DARROW_BUILD_TESTS=ON \
      -DARROW_BUILD_UTILITIES=OFF \
      -DARROW_OPTIONAL_INSTALL=ON \
      -DARROW_PARQUET=on \
      -DARROW_PLASMA=on \
      -DARROW_TENSORFLOW=on \
      -DARROW_PYTHON=on \
      -DARROW_ORC=on \
      -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
      $ARROW_CPP_DIR

ninja $PYTHON_CPP_BUILD_TARGETS
ninja install

popd

# python-test isn't run by travis_script_cpp.sh, exercise it here
$ARROW_CPP_BUILD_DIR/$ARROW_BUILD_TYPE/arrow-python-test

pushd $ARROW_PYTHON_DIR

# Other stuff pip install
pip install -r requirements.txt

# FIXME(kszucs): disabled in https://github.com/apache/arrow/pull/3406
if [ "$PYTHON_VERSION" == "3.6" ]; then
    pip install -vvv pickle5 || true
fi
if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    export PYARROW_GENERATE_COVERAGE=1
    pip install -q coverage
fi

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig

export PYARROW_BUILD_TYPE=$ARROW_BUILD_TYPE
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_PLASMA=1
export PYARROW_WITH_ORC=1
if [ $ARROW_TRAVIS_PYTHON_GANDIVA == "1" ]; then
  export PYARROW_WITH_GANDIVA=1
fi

python setup.py develop

# Basic sanity checks
python -c "import pyarrow.parquet"
python -c "import pyarrow.plasma"
python -c "import pyarrow.orc"

echo "PLASMA_VALGRIND: $PLASMA_VALGRIND"

# Set up huge pages for plasma test
if [ $TRAVIS_OS_NAME == "linux" ]; then
    sudo sysctl -w vm.nr_hugepages=2048
    sudo mkdir -p /mnt/hugepages
    sudo mount -t hugetlbfs -o uid=`id -u` -o gid=`id -g` none /mnt/hugepages
    sudo bash -c "echo `id -g` > /proc/sys/vm/hugetlb_shm_group"
    sudo bash -c "echo 2048 > /proc/sys/vm/nr_hugepages"
fi

# Need to run tests from the source tree for Cython coverage and conftest.py
if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    # Output Python coverage data in a persistent place
    export COVERAGE_FILE=$ARROW_PYTHON_COVERAGE_FILE
    coverage run --append -m pytest $PYARROW_PYTEST_FLAGS pyarrow/tests
else
    python -m pytest $PYARROW_PYTEST_FLAGS pyarrow/tests
fi

if [ "$ARROW_TRAVIS_COVERAGE" == "1" ]; then
    # Check Cython coverage was correctly captured in $COVERAGE_FILE
    coverage report -i --include="*/lib.pyx"
    coverage report -i --include="*/memory.pxi"
    coverage report -i --include="*/_parquet.pyx"
    # Generate XML file for CodeCov
    coverage xml -i -o $TRAVIS_BUILD_DIR/coverage.xml
    # Capture C++ coverage info
    pushd $TRAVIS_BUILD_DIR
    lcov --quiet --directory . --capture --no-external --output-file coverage-python-tests.info \
        2>&1 | grep -v "WARNING: no data found for /usr/include"
    lcov --add-tracefile coverage-python-tests.info \
        --output-file $ARROW_CPP_COVERAGE_FILE
    rm coverage-python-tests.info
    popd   # $TRAVIS_BUILD_DIR
fi

if [ "$ARROW_TRAVIS_PYTHON_DOCS" == "1" ] && [ "$PYTHON_VERSION" == "3.6" ]; then
  pushd ../cpp/apidoc
  doxygen
  popd
  cd ../docs
  sphinx-build -q -b html -d _build/doctrees -W source _build/html
fi

popd  # $ARROW_PYTHON_DIR

if [ "$ARROW_TRAVIS_PYTHON_BENCHMARKS" == "1" ] && [ "$PYTHON_VERSION" == "3.6" ]; then
  # Check the ASV benchmarking setup.
  # Unfortunately this won't ensure that all benchmarks succeed
  # (see https://github.com/airspeed-velocity/asv/issues/449)
  source deactivate
  conda create -y -q -n pyarrow_asv python=$PYTHON_VERSION -c conda-forge/label/cf201901
  conda activate pyarrow_asv
  pip install -q git+https://github.com/pitrou/asv.git@customize_commands

  export PYARROW_WITH_PARQUET=1
  export PYARROW_WITH_PLASMA=1
  export PYARROW_WITH_ORC=0
  export PYARROW_WITH_GANDIVA=0

  pushd $ARROW_PYTHON_DIR
  # Workaround for https://github.com/airspeed-velocity/asv/issues/631
  git fetch --depth=100 origin master:master
  # Generate machine information (mandatory)
  asv machine --yes
  # Run benchmarks on the changeset being tested
  asv run --no-pull --show-stderr --quick HEAD^!
  popd  # $ARROW_PYTHON_DIR
fi
