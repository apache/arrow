#!/usr/bin/env bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.


set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

if [ $TRAVIS_OS_NAME == "osx" ]; then
    brew install gtk-doc autoconf-archive gobject-introspection
fi

gem install gobject-introspection

pushd $ARROW_C_GLIB_DIR

: ${ARROW_C_GLIB_INSTALL=$TRAVIS_BUILD_DIR/c-glib-install}

./autogen.sh

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib

./configure --prefix=${ARROW_C_GLIB_INSTALL} --enable-gtk-doc

make -j4
make install

popd
