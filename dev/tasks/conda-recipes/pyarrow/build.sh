#!/bin/sh

set -e
set -x

# Build dependencies
export ARROW_HOME=$PREFIX
export PARQUET_HOME=$PREFIX
export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION

cd python

$PYTHON setup.py \
        build_ext --build-type=release \
                  --with-flight \
                  --with-orc \
                  --with-plasma \
                  --with-parquet \
                  --with-gandiva \
        install --single-version-externally-managed \
                --record=record.txt
