#!/bin/bash

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

./thirdparty/download_thirdparty.sh || { echo "download_thirdparty.sh failed" ; return; }
./thirdparty/build_thirdparty.sh || { echo "build_thirdparty.sh failed" ; return; }
source thirdparty/versions.sh

export GTEST_HOME=$SOURCE_DIR/thirdparty/$GTEST_BASEDIR
export GBENCHMARK_HOME=$SOURCE_DIR/thirdparty/installed
export FLATBUFFERS_HOME=$SOURCE_DIR/thirdparty/installed

echo "Build env initialized"
