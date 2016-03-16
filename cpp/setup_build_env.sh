#!/bin/bash

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh
source thirdparty/versions.sh

export GTEST_HOME=$SOURCE_DIR/thirdparty/$GTEST_BASEDIR

echo "Build env initialized"
