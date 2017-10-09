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

if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update && brew bundle --file=c_glib/Brewfile

  export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/opt/libffi/lib/pkgconfig
fi

gem install test-unit gobject-introspection

if [ $TRAVIS_OS_NAME == "osx" ]; then
  sudo env PKG_CONFIG_PATH=$PKG_CONFIG_PATH luarocks install lgi
else
  git clone \
    --quiet \
    --depth 1 \
    --recursive \
    https://github.com/torch/distro.git ~/torch
  pushd ~/torch
  ./install-deps > /dev/null
  echo "yes" | ./install.sh > /dev/null
  . ~/torch/install/bin/torch-activate
  popd
  luarocks install lgi
fi

go get github.com/linuxdeepin/go-gir-generator || :
pushd $GOPATH/src/github.com/linuxdeepin/go-gir-generator
rm lib.in/gio-2.0/gdk_workaround.go
mv lib.in/gio-2.0/config.json{,.orig}
sed \
  -e 's/\("Settings",\)/\/\/ \1/g' \
  -e 's/\("SettingsBackend",\)/\/\/ \1/g' \
  lib.in/gio-2.0/config.json.orig > lib.in/gio-2.0/config.json
mv Makefile{,.orig}
sed -e 's/ gudev-1.0//' Makefile.orig > Makefile
mkdir -p out/src/gir/gudev-1.0
make build copyfile
mkdir -p $GOPATH/bin/
cp -a out/gir-generator $GOPATH/bin/
cp -a out/src/gir/ $GOPATH/src/gir/
popd

pushd $ARROW_C_GLIB_DIR

./autogen.sh

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib

CONFIGURE_OPTIONS="--prefix=$ARROW_C_GLIB_INSTALL"
if [ $TRAVIS_OS_NAME != "osx" ]; then
  CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS --enable-gtk-doc"
fi

CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS CFLAGS=-DARROW_NO_DEPRECATED_API"
CONFIGURE_OPTIONS="$CONFIGURE_OPTIONS CXXFLAGS=-DARROW_NO_DEPRECATED_API"

./configure $CONFIGURE_OPTIONS

make -j4
make install

popd
