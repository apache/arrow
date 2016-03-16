#!/bin/bash

SOURCE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
check_run() {
rc=eval $1
echo $rc
if [[ $rc ]]; then echo $1 failed with exit code $rc!; kill -INT $$; fi
}

check_run ./thirdparty/download_thirdparty.sh
check_run ./thirdparty/build_thirdparty.sh
source thirdparty/versions.sh

export GTEST_HOME=$SOURCE_DIR/thirdparty/$GTEST_BASEDIR

echo "Build env initialized"
