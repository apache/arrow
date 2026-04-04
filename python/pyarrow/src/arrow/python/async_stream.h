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

#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"

namespace arrow::py {

/// \brief Call an AsyncGenerator<RecordBatchWithMetadata> and return the Future.
///
/// This is needed because Cython cannot invoke std::function objects directly.
inline Future<RecordBatchWithMetadata> CallAsyncGenerator(
    AsyncGenerator<RecordBatchWithMetadata>& generator) {
  return generator();
}

/// \brief Create a roundtrip async producer+consumer pair for testing.
///
/// Allocates an ArrowAsyncDeviceStreamHandler on the heap, calls
/// CreateAsyncDeviceStreamHandler (consumer side), then submits
/// ExportAsyncRecordBatchReader (producer side) on the given executor.
/// Returns a Future that resolves to the AsyncRecordBatchGenerator once
/// the schema is available.
inline Future<AsyncRecordBatchGenerator> RoundtripAsyncBatches(
    std::shared_ptr<Schema> schema,
    std::vector<std::shared_ptr<RecordBatch>> batches,
    ::arrow::internal::Executor* executor, uint64_t queue_size = 5) {
  // Heap-allocate the handler so it outlives this function.
  auto* handler = new ArrowAsyncDeviceStreamHandler;
  std::memset(handler, 0, sizeof(ArrowAsyncDeviceStreamHandler));

  auto fut_gen = CreateAsyncDeviceStreamHandler(handler, executor, queue_size);

  // Submit the export to the executor so it runs concurrently with the consumer.
  auto submit_result = executor->Submit(
      [schema = std::move(schema), batches = std::move(batches), handler]() mutable {
        auto generator = MakeVectorGenerator(std::move(batches));
        return ExportAsyncRecordBatchReader(std::move(schema), std::move(generator),
                                            DeviceAllocationType::kCPU, handler);
      });

  if (!submit_result.ok()) {
    return Future<AsyncRecordBatchGenerator>::MakeFinished(submit_result.status());
  }

  return fut_gen;
}

}  // namespace arrow::py
