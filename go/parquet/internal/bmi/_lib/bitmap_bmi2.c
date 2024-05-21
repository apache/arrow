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

#include <arch.h>
#include <stdint.h>

#if !defined(__ARM_NEON) && !defined(__ARM_NEON__)
// don't compile this for ARM, the pure go lookup table version
// is more performant anyways since ARM doesn't have a BMI2/pext_u64
// instruction we can call directly.
uint64_t FULL_NAME(extract_bits)(uint64_t bitmap, uint64_t select_bitmap) {
#if defined(__BMI2__)
   return (uint64_t)(_pext_u64(bitmap, select_bitmap));
#else
  uint64_t res = 0;
  for (uint64_t bp = 1; select_bitmap != 0; bp += bp) {
    if (bitmap & select_bitmap & -select_bitmap) {
      res |= bp;
    }
    select_bitmap &= (select_bitmap - 1);
  }
  return res;
#endif
}

#endif

uint64_t FULL_NAME(levels_to_bitmap)(const int16_t* levels, const int num_levels, const int16_t rhs) {
  uint64_t mask = 0;
  for (int x = 0; x < num_levels; x++) {
    mask |= (uint64_t)(levels[x] > rhs ? 1 : 0) << x;
  }
  return mask;
}
