#!/bin/sh

set -e
set -x

# Build dependencies
export ARROW_HOME=$PREFIX
export PARQUET_HOME=$PREFIX
export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION
export PYARROW_BUILD_TYPE=release
export PYARROW_WITH_FLIGHT=1
export PYARROW_WITH_PLASMA=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_ORC=1
export PYARROW_WITH_GANDIVA=1

cd python

$PYTHON setup.py \
        build_ext \
        install --single-version-externally-managed \
                --record=record.txt
