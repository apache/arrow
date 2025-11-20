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

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging_internal.h"

namespace arrow::internal {

MemoryPool* fuzzing_memory_pool() {
  static auto pool = std::make_shared<::arrow::CappedMemoryPool>(
      ::arrow::default_memory_pool(), /*bytes_allocated_limit=*/kFuzzingMemoryLimit);
  return pool.get();
}

void LogFuzzStatus(const Status& st, const uint8_t* data, int64_t size) {
  // Most fuzz inputs will be invalid and generate errors, only log potential OOMs
  if (st.IsOutOfMemory()) {
    ARROW_LOG(WARNING) << "Fuzzing input with size=" << size
                       << " hit allocation failure: " << st.ToString();
  }
}

}  // namespace arrow::internal
