#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

source $TP_DIR/versions.sh

download_extract_and_cleanup() {
	type curl >/dev/null 2>&1 || { echo >&2 "curl not installed.  Aborting."; exit 1; }
	filename=$TP_DIR/$(basename "$1")
	curl -#LC - "$1" -o $filename
	tar xzf $filename -C $TP_DIR
	rm $filename
}

if [ ! -d ${GTEST_BASEDIR} ]; then
  echo "Fetching gtest"
  download_extract_and_cleanup $GTEST_URL
fi

echo ${GBENCHMARK_BASEDIR}
if [ ! -d ${GBENCHMARK_BASEDIR} ]; then
  echo "Fetching google benchmark"
  download_extract_and_cleanup $GBENCHMARK_URL
fi
