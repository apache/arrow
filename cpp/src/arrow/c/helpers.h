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

#pragma once

#include <assert.h>
#include <string.h>

#include "arrow/c/abi.h"

#ifdef __cplusplus
extern "C" {
#endif

inline int ArrowIsReleased(const struct ArrowArray* array) {
  return array->format == NULL;
}

inline void ArrowMoveArray(struct ArrowArray* src, struct ArrowArray* dest) {
  assert(dest != src);
  assert(!ArrowIsReleased(src));
  memcpy(dest, src, sizeof(struct ArrowArray));
  src->format = NULL;
  src->release = NULL;
}

inline void ArrowReleaseArray(struct ArrowArray* array) {
  if (array->format != NULL) {
    if (array->release != NULL) {
      array->release(array);
      assert(ArrowIsReleased(array));
    } else {
      array->format = NULL;
    }
  }
}

#ifdef __cplusplus
}
#endif
