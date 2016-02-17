#!/bin/bash

set -e

SOURCE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh

export GTEST_HOME=$SOURCE_DIR/thirdparty/$GTEST_BASEDIR

echo "Build env initialized"
