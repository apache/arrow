#!/bin/bash
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.
#
# Usage:
#   docker run --rm -v $PWD:/io arrow-base-x86_64 /io/build_arrow.sh
# or with Parquet support
#   docker run --rm -v $PWD:/io parquet_arrow-base-x86_64 /io/build_arrow.sh

# Build upon the scripts in https://github.com/matthew-brett/manylinux-builds
# * Copyright (c) 2013-2016, Matt Terry and Matthew Brett (BSD 2-clause)

PYTHON_VERSIONS="${PYTHON_VERSIONS:-2.7 3.4 3.5 3.6}"

# Package index with only manylinux1 builds
MANYLINUX_URL=https://nipy.bic.berkeley.edu/manylinux

source /multibuild/manylinux_utils.sh

cd /arrow/python

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
# PyArrow build configuration
export PYARROW_BUILD_TYPE='release'
export PYARROW_CMAKE_OPTIONS='-DPYARROW_BUILD_TESTS=ON'
# Need as otherwise arrow_io is sometimes not linked
export LDFLAGS="-Wl,--no-as-needed"
export ARROW_HOME="/usr"
export PARQUET_HOME="/usr"

# Ensure the target directory exists
mkdir -p /io/dist
# Temporary directory to store the wheels that should be sent through auditwheel
rm_mkdir unfixed_wheels

PY35_BIN=/opt/python/cp35-cp35m/bin
$PY35_BIN/pip install 'pyelftools<0.24'
$PY35_BIN/pip install 'git+https://github.com/xhochy/auditwheel.git@pyarrow-fixes'

# Override repair_wheelhouse function
function repair_wheelhouse {
    local in_dir=$1
    local out_dir=$2
    for whl in $in_dir/*.whl; do
        if [[ $whl == *none-any.whl ]]; then
            cp $whl $out_dir
        else
            # Store libraries directly in . not .libs to fix problems with libpyarrow.so linkage.
            $PY35_BIN/auditwheel -v repair -L . $whl -w $out_dir/
        fi
    done
    chmod -R a+rwX $out_dir
}

for PYTHON in ${PYTHON_VERSIONS}; do
    PYTHON_INTERPRETER="$(cpython_path $PYTHON)/bin/python"
    PIP="$(cpython_path $PYTHON)/bin/pip"
    PIPI_IO="$PIP install -f $MANYLINUX_URL"
    PATH="$PATH:$(cpython_path $PYTHON)"

    $PIPI_IO "numpy==1.9.0"
    $PIPI_IO "cython==0.24"

    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER setup.py build_ext --inplace --with-parquet --with-jemalloc
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER setup.py bdist_wheel

    # Test for optional modules
    $PIPI_IO -r requirements.txt
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER -c "import pyarrow.parquet"
    PATH="$PATH:$(cpython_path $PYTHON)/bin" $PYTHON_INTERPRETER -c "import pyarrow.jemalloc"

    repair_wheelhouse dist /io/dist
done

