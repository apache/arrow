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
#include <variant>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

// A tag used to signal the fuzzing engine that this input should not be saved
// into the corpus.
struct SkipFuzzInput {
  Status reason;
};

// The Status alternative holds a regular fuzzing success or failure,
// while the SkipFuzzInput alternative holds a setup failure (e.g. invalid
// fuzzing parameters encoded in the payload).
using FuzzStatus = std::variant<Status, SkipFuzzInput>;

inline const Status& FuzzReason(const FuzzStatus& st) {
  struct Visitor {
    const Status& operator()(const SkipFuzzInput& v) { return v.reason; }
    const Status& operator()(const Status& v) { return v; }
  };
  return std::visit(Visitor{}, st);
}

// The default rss_limit_mb on OSS-Fuzz is 2560 MB and we want to fail allocations
// before that limit is reached, otherwise the fuzz target gets killed (GH-48105).
constexpr int64_t kFuzzingMemoryLimit = 2200LL * 1000 * 1000;

/// Return a memory pool that will not allocate more than kFuzzingMemoryLimit bytes.
ARROW_EXPORT MemoryPool* fuzzing_memory_pool();

/// Optionally log the outcome of fuzzing an input
///
/// Returns the integer code to return from LLVMFuzzerTestOneInput.
ARROW_EXPORT int LogFuzzStatus(const FuzzStatus&, const uint8_t* data, int64_t size);

inline int LogFuzzStatus(const Status& status, const uint8_t* data, int64_t size) {
  return LogFuzzStatus(FuzzStatus{status}, data, size);
}

}  // namespace arrow::internal
