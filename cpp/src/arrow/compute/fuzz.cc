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

// A fuzzer for the compute kernels: read an IPC stream, then evaluate
// compute functions selected by a packed header on the decoded batches.

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/initialize.h"
#include "arrow/datum.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/options.h"
#include "arrow/ipc/reader.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/util/fuzz_internal.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace compute {

namespace {

constexpr uint8_t kFuzzHeaderMarker = 0xFA;
constexpr int kFuzzHeaderSize = 8;

// Function names indexable by the header selector byte.  All of these can be
// called with default options; unsupported (function, argument type)
// combinations simply return a non-OK Status, which is fine for fuzzing.
const std::vector<std::string>& FuzzFunctions() {
  static const std::vector<std::string> functions = {
      // unary scalar
      "abs",      "negate", "floor", "ceil", "sqrt",
      // vector
      "unique",   "value_counts", "dictionary_encode",
      // aggregate
      "sum",      "mean",   "min_max", "approximate_median", "count",
      // binary scalar
      "add",      "subtract", "multiply", "divide", "power", "max", "min",
  };
  return functions;
}

}  // namespace

// Fuzz input layout (same convention as the CSV fuzzer):
//   [0] marker 0xFA, [1] function selector, [2] argument mode,
//   [3] scalar argument byte, [4..7] reserved.
// Inputs smaller than 8 bytes or without the marker are treated as plain
// IPC streams and only validated (legacy behavior).
Status FuzzCompute(const uint8_t* data, int64_t size) {
  ARROW_UNUSED(Initialize());

  MemoryPool* pool = ::arrow::internal::fuzzing_memory_pool();

  ipc::IpcReadOptions read_options;
  read_options.use_threads = false;
  read_options.memory_pool = pool;

  auto buffer = std::make_shared<Buffer>(data, size);
  io::BufferReader buffer_reader(buffer);
  ARROW_ASSIGN_OR_RAISE(auto batch_reader,
                        ipc::RecordBatchStreamReader::Open(&buffer_reader,
                                                           read_options));

  uint8_t function_selector = 0;
  uint8_t arg_mode = 0;
  uint8_t scalar_byte = 0;
  bool run_kernels = false;
  if (size >= kFuzzHeaderSize && data[0] == kFuzzHeaderMarker) {
    run_kernels = true;
    function_selector = data[1];
    arg_mode = data[2];
    scalar_byte = data[3];
  }

  ExecContext exec_ctx(pool);

  Status st;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->ReadNext());
    if (!batch.batch && !batch.custom_metadata) {
      // EOS
      break;
    }
    if (batch.batch) {
      st &= batch.batch->Validate();
      if (run_kernels) {
        const auto& functions = FuzzFunctions();
        const auto& function_name = functions[function_selector % functions.size()];
        const auto& columns = batch.batch->columns();
        std::vector<Datum> args;
        switch (arg_mode % 3) {
          case 0:
            args = {Datum(columns[0])};
            break;
          case 1:
            args = {Datum(columns[0]),
                    Datum(Int64Scalar(static_cast<int64_t>(scalar_byte)))};
            break;
          default:
            args = {Datum(columns[0]),
                    Datum(columns[columns.size() > 1 ? 1 : 0])};
            break;
        }
        // Kernels are allowed to reject inputs; only crashes are interesting.
        ARROW_UNUSED(CallFunction(function_name, args, /*options=*/nullptr, &exec_ctx)
                         .status());
      }
    }
  }

  return st;
}

}  // namespace compute
}  // namespace arrow

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::compute::FuzzCompute(data, static_cast<int64_t>(size));
  return arrow::internal::LogFuzzStatus(status, data, static_cast<int64_t>(size));
}