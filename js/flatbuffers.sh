#!/bin/bash

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

echo "Compiling flatbuffer schemas..."
mkdir -p lib lib-esm
DIR=`mktemp -d`
flatc -o $DIR --js ../format/*.fbs
cat $DIR/*_generated.js > src/Arrow_generated.js

# Duplicate in the tsc-generated outputs - we can't make tsc pull in .js files
# and still prooduce declaration files
cat $DIR/*_generated.js > lib/Arrow_generated.js
cat $DIR/*_generated.js > lib-esm/Arrow_generated.js
rm -rf $DIR
