/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern "C" {

#include "./types.h"

// Dummy function to test NULL_INTERNAL (most valid ones need varchar).

// If input is valid and a multiple of 2, return half the value. else, null.
int half_or_null_int32(int32 val, bool in_valid, bool *out_valid) {
  if (in_valid && (val % 2 == 0)) {
    // output is valid.
    *out_valid = true;
    return val / 2;
  } else {
    // output is invalid.
    *out_valid = false;
    return 0;
  }
}

} // extern "C"
