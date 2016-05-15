#!/bin/bash

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

./thirdparty/download_thirdparty.sh || { echo "download_thirdparty.sh failed" ; return; }
./thirdparty/build_thirdparty.sh || { echo "build_thirdparty.sh failed" ; return; }
source ./thirdparty/set_thirdparty_env.sh || { echo "source set_thirdparty_env.sh failed" ; return; }

echo "Build env initialized"
