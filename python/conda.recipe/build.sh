#!/bin/bash
set -ex

# Build dependency
export ARROW_HOME=$PREFIX

cd $RECIPE_DIR

echo Setting the compiler...
if [ `uname` == Linux ]; then
  EXTRA_CMAKE_ARGS=-DCMAKE_SHARED_LINKER_FLAGS=-static-libstdc++
elif [ `uname` == Darwin ]; then
  EXTRA_CMAKE_ARGS=
fi

cd ..
$PYTHON setup.py build_ext --extra-cmake-args=$EXTRA_CMAKE_ARGS || exit 1
$PYTHON setup.py install || exit 1
