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

set -ex

# Download database
curl https://data.iana.org/time-zones/releases/tzdata2024b.tar.gz --output ~/Downloads/tzdata.tar.gz

# Extract
mkdir -p ~/Downloads/tzdata
tar --extract --file ~/Downloads/tzdata.tar.gz --directory ~/Downloads/tzdata

# Download Windows timezone mapping
curl https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml --output ~/Downloads/tzdata/windowsZones.xml
