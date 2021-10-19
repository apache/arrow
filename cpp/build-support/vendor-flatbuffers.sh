#!/usr/bin/env bash
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
#

set -eu

# Run this from cpp/ directory with $FLATBUFFERS_HOME set to location of your
# Flatbuffers installation
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

VENDOR_LOCATION=$SOURCE_DIR/../thirdparty/flatbuffers/include/flatbuffers
mkdir -p $VENDOR_LOCATION
cp -f $FLATBUFFERS_HOME/include/flatbuffers/base.h $VENDOR_LOCATION
cp -f $FLATBUFFERS_HOME/include/flatbuffers/flatbuffers.h $VENDOR_LOCATION
cp -f $FLATBUFFERS_HOME/include/flatbuffers/stl_emulation.h $VENDOR_LOCATION
