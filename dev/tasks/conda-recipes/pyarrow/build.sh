#!/bin/bash

set -e
set -x

# Build dependencies
export ARROW_HOME=$PREFIX
export PARQUET_HOME=$PREFIX


cd python
$PYTHON setup.py \
        build_ext --build-type=release --with-parquet --with-plasma --with-orc \
        install --single-version-externally-managed --record=record.txt
