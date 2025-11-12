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

#include <cstdint>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

// The default rss_limit_mb on OSS-Fuzz is 2560 MB and we want to fail allocations
// before that limit is reached, otherwise the fuzz target gets killed (GH-48105).
constexpr int64_t kFuzzingMemoryLimit = 2200LL * 1000 * 1000;

/// Return a memory pool that will not allocate more than kFuzzingMemoryLimit bytes.
ARROW_EXPORT MemoryPool* fuzzing_memory_pool();

// Optionally log the outcome of fuzzing an input
ARROW_EXPORT void NoteFuzzStatus(const Status&, const uint8_t* data, int64_t size);

}  // namespace arrow::internal
