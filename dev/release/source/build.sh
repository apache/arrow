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
c_glib_including_configure_tar_gz=$2

tar xf /arrow/${archive_name}.tar

# Run autogen.sh to create c_glib/ source archive containing the configure script
cd ${archive_name}/c_glib
./autogen.sh
rm -rf autom4te.cache
cd -
mv ${archive_name}/c_glib/ c_glib/
tar czf /arrow/${c_glib_including_configure_tar_gz} c_glib
