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

s|WORD $0x54[0-9a-f]\+[[:space:]]\+//[[:space:]]\+b.\([leqgtnso]\+\)[[:space:]]\+.\(LBB0_[0-9]\+\)|B\U\1 \2|
s|WORD $0x14000000[[:space:]]\+//[[:space:]]\+b[[:space:]]\+.\(LBB0_[0-9]\+\)|JMP \1|
s|\(WORD $0x9[0-9a-f]\+ // adrp.*\)|// \1|
s|WORD $0x[0-9a-f]\+ // ldr[[:space:]]\+d\([0-9]\+\), \[x[0-9]\+, :lo[0-9]\+:.\(LCPI0_[0-9]\+\)\]|VMOVD \2, V\1|
s|WORD $0x[0-9a-f]\+ // ldr[[:space:]]\+q\([0-9]\+\), \[x[0-9]\+, :lo[0-9]\+:.\(LCPI0_[0-9]\+\)\]|VMOVQ \2L, \2H, V\1|
