#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

source $TRAVIS_BUILD_DIR/ci/travis_env_common.sh

if [ $TRAVIS_OS_NAME = "osx" ]; then
  export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/opt/libffi/lib/pkgconfig
  export XML_CATALOG_FILES=/usr/local/etc/xml/catalog
else
  source $TRAVIS_BUILD_DIR/ci/travis_install_conda.sh
  conda create -n meson -y -q python=3.6
  conda activate meson
  pip install meson
  sudo apt-get install -y -q \
    autoconf-archive \
    gtk-doc-tools \
    libgirepository1.0-dev
  conda install -q -y ninja
fi

gem install test-unit gobject-introspection

if [ $TRAVIS_OS_NAME = "osx" ]; then
  sudo env PKG_CONFIG_PATH=$PKG_CONFIG_PATH luarocks install lgi
else
  sudo apt install -y -qq luarocks
  sudo luarocks install lgi
fi

pushd $ARROW_C_GLIB_DIR

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib

# Build with GNU Autotools
./autogen.sh
CONFIGURE_OPTIONS="--prefix=$ARROW_C_GLIB_INSTALL_AUTOTOOLS"
# TODO(ARROW-5307): Enable this
# CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS --enable-gtk-doc"
CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS CFLAGS=-DARROW_NO_DEPRECATED_API"
CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS CXXFLAGS=-DARROW_NO_DEPRECATED_API"
mkdir -p build
pushd build
../configure $CONFIGURE_OPTIONS
make -j4
make install
popd
rm -rf build

# Build with Meson
MESON_OPTIONS="--prefix=$ARROW_C_GLIB_INSTALL_MESON"
# TODO(ARROW-5307): Enable this
# MESON_OPTIONS="$MESON_OPTIONS -Dgtk_doc=true"
mkdir -p build
env \
  CFLAGS="-DARROW_NO_DEPRECATED_API" \
  CXXFLAGS="-DARROW_NO_DEPRECATED_API" \
  meson build $MESON_OPTIONS
pushd build
ninja
ninja install
popd
rm -rf build

popd
