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

#include "arrow/util/fuzz_internal.h"

#include <cstdint>
#include <utility>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow::internal {

MemoryPool* fuzzing_memory_pool() {
  static auto pool = std::make_shared<::arrow::CappedMemoryPool>(
      ::arrow::default_memory_pool(), /*bytes_allocated_limit=*/kFuzzingMemoryLimit);
  return pool.get();
}

void LogFuzzStatus(const Status& st, const uint8_t* data, int64_t size) {
  static const int kVerbosity = []() {
    auto maybe_env_value = GetEnvVar("ARROW_FUZZING_VERBOSITY");
    if (maybe_env_value.status().IsKeyError()) {
      return 0;
    }
    auto env_value = std::move(maybe_env_value).ValueOrDie();
    int32_t value;
    if (!ParseValue<Int32Type>(env_value.data(), env_value.length(), &value)) {
      Status::Invalid("Invalid value for ARROW_FUZZING_VERBOSITY: '", env_value, "'")
          .Abort();
    }
    return value;
  }();

  if (!st.ok() && kVerbosity >= 1) {
    ARROW_LOG(WARNING) << "Fuzzing input with size=" << size
                       << " failed: " << st.ToString();
  } else if (st.IsOutOfMemory()) {
    ARROW_LOG(WARNING) << "Fuzzing input with size=" << size
                       << " hit allocation failure: " << st.ToString();
  }
}

}  // namespace arrow::internal
