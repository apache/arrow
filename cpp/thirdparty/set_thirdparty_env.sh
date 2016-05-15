#!/usr/bash

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
source $SOURCE_DIR/versions.sh

if [ -z "$THIRDPARTY_DIR" ]; then
	THIRDPARTY_DIR=$SOURCE_DIR
fi

export GTEST_HOME=$THIRDPARTY_DIR/$GTEST_BASEDIR
export GBENCHMARK_HOME=$THIRDPARTY_DIR/installed
export FLATBUFFERS_HOME=$THIRDPARTY_DIR/installed
