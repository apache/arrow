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

go get github.com/linuxdeepin/go-gir-generator || :
pushd $GOPATH/src/github.com/linuxdeepin/go-gir-generator
make build copyfile
mkdir -p $GOPATH/bin/
cp -a out/gir-generator $GOPATH/bin/
cp -a out/src/gir/ $GOPATH/src/
popd

pushd $ARROW_C_GLIB_DIR

./autogen.sh

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$ARROW_CPP_INSTALL/lib/pkgconfig
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ARROW_CPP_INSTALL/lib

./configure --prefix=${ARROW_C_GLIB_INSTALL} --enable-gtk-doc

make -j4
make install

popd
