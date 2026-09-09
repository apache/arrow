// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Portable sequential (non-interleaved) bit-pack/unpack reference, arm 1 of
// the three-arm kernel sweep. Compiled once into seq_unpack.o at a fixed
// optimization level and linked unchanged into every driver build, so this
// arm's machine code never varies between the -O2 and -O3 rows.
#pragma once
#include <cstdint>

using SeqPackFn = void (*)(const uint32_t* in, uint32_t* out, int n);
using SeqUnpackFn = void (*)(const uint32_t* packed, uint32_t* out, int n);

// Indexed by bit width 1..32 (index 0 unused).
extern SeqPackFn kSeqPack[33];
extern SeqUnpackFn kSeqUnpack[33];
