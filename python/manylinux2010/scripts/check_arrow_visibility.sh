#!/bin/bash -ex
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

nm --demangle --dynamic /arrow-dist/lib/libarrow.so > nm_arrow.log

# Filter out Arrow symbols and see if anything remains.
# '_init' and '_fini' symbols may or not be present, we don't care.
# (note we must ignore the grep exit status when no match is found)
grep ' T ' nm_arrow.log | grep -v -E '(arrow|\b_init\b|\b_fini\b)' | cat - > visible_symbols.log

if [[ -f visible_symbols.log && `cat visible_symbols.log | wc -l` -eq 0 ]]
then
    exit 0
fi

echo "== Unexpected symbols exported by libarrow.so =="
cat visible_symbols.log
echo "================================================"

exit 1
