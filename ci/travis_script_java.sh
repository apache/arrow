#!/usr/bin/env bash

set -e

JAVA_DIR=${TRAVIS_BUILD_DIR}/java

pushd $JAVA_DIR

mvn -B test

popd
