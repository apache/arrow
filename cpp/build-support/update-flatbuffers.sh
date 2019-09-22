#!/bin/sh
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

# Run this from cpp/ directory. flatc is expected to be in your path

OUTPUT_LOCATION=src/generated
flatc -c -o $OUTPUT_LOCATION \
      ../format/Message.fbs \
      ../format/File.fbs \
      ../format/Schema.fbs \
      ../format/Tensor.fbs \
      ../format/SparseTensor.fbs \
      src/arrow/ipc/feather.fbs

flatc -c -o src/plasma \
      --gen-object-api \
      --scoped-enums \
      src/plasma/common.fbs \
      src/plasma/plasma.fbs
