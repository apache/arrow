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
#include <stdbool.h>
#include <string.h>

#include "arrow/dataset/c/api.h"

#ifdef __cplusplus
extern "C" {
#endif

// Macro for easily generating IsReleased, MarkReleased, and Release functions
// for the various C struct types in arrow/dataset/c/api.h.
#define DEFINE_DATASET_CFUNCS(_name, _typestruct)                   \
  inline bool Arrow##_name##IsReleased(struct _typestruct* obj) {   \
    return obj->release == NULL;                                    \
  }                                                                 \
  inline void Arrow##_name##MarkReleased(struct _typestruct* obj) { \
    obj->release = NULL;                                            \
  }                                                                 \
  inline void Arrow##_name##Release(struct _typestruct* obj) {      \
    if (!Arrow##_name##IsReleased(obj)) {                           \
      obj->release(obj);                                            \
      assert(Arrow##_name##IsReleased(obj));                        \
    }                                                               \
  }

DEFINE_DATASET_CFUNCS(DatasetFactory, DatasetFactory)
DEFINE_DATASET_CFUNCS(Dataset, Dataset)
DEFINE_DATASET_CFUNCS(Scanner, Scanner)

#ifdef __cplusplus
}
#endif
