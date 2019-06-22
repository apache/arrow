#!/bin/bash
#
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

set -e

archive_name=$1
dist_c_glib_tar_gz=$2

tar xf /host/${archive_name}.tar

# build Apache Arrow C++ before building Apache Arrow GLib because
# Apache Arrow GLib requires Apache Arrow C++.
mkdir -p ${archive_name}/cpp/build
cpp_install_dir=${PWD}/${archive_name}/cpp/install
cd ${archive_name}/cpp/build
cmake .. \
  -DCMAKE_INSTALL_PREFIX=${cpp_install_dir} \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DARROW_PLASMA=yes \
  -DARROW_GANDIVA=yes \
  -DARROW_PARQUET=yes
make -j8
make install
cd -

# build source archive for Apache Arrow GLib by "make dist".
cd ${archive_name}/c_glib
./autogen.sh
./configure \
  PKG_CONFIG_PATH=${cpp_install_dir}/lib/pkgconfig \
  --enable-gtk-doc
LD_LIBRARY_PATH=${cpp_install_dir}/lib make -j8
make dist
tar xzf *.tar.gz
rm *.tar.gz
cd -
mv ${archive_name}/c_glib/apache-arrow-glib-* c_glib/
tar czf /host/${dist_c_glib_tar_gz} c_glib
