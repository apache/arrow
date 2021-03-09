// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdint.h>
#include <x86intrin.h>

void extract_bits(uint64_t bitmap, uint64_t select_bitmap, uint64_t* res) {
   *res = _pext_u64(bitmap, select_bitmap);
}

void popcount64(uint64_t bitmap, uint64_t* res) {
  *res = __builtin_popcountll(bitmap);
}

void popcount32(uint32_t bitmap, uint32_t* res) {
  *res = __builtin_popcount(bitmap);
}

void levels_to_bitmap(const int16_t* levels, const int num_levels, const int16_t rhs, uint64_t* res) {
  uint64_t mask = 0;
  for (int x = 0; x < num_levels; x++) {
    mask |= (uint64_t)(levels[x] > rhs ? 1 : 0) << x;
  }
  *res = mask;
}
