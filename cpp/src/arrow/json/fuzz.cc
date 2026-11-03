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

#include <cstdint>
#include <memory>

#include "arrow/buffer.h"
#include "arrow/io/memory.h"
#include "arrow/json/options.h"
#include "arrow/json/reader.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/fuzz_internal.h"
#include "arrow/util/macros.h"
#include "arrow/util/thread_pool.h"

namespace arrow::json {

using ::arrow::internal::GetCpuThreadPool;
using ::arrow::io::InputStream;

namespace {

// Packed header prefixing the fuzz payload, used to explore the JSON reader's
// option space (see https://github.com/google/oss-fuzz/issues/14437 for why
// fuzz target parameters have to be encoded inside the payload itself).
//
// All option values are total functions of the header bytes, so every possible
// input leads to a well-defined option set. Inputs without the marker byte are
// read with the default options, which keeps plain-JSON corpora useful.
constexpr int64_t kFuzzHeaderSize = 8;
constexpr uint8_t kFuzzMarker = 0xFA;

// Explicit-schema variants: none, full int64/double/utf8, all-utf8 coercion,
// mismatched field name, and a single-column int/uint selector.
std::shared_ptr<::arrow::Schema> MakeExplicitSchema(uint8_t selector) {
  switch (selector % 5) {
    case 0:
      return nullptr;
    case 1:
      return ::arrow::schema({::arrow::field("a", ::arrow::int64()),
                              ::arrow::field("b", ::arrow::float64()),
                              ::arrow::field("c", ::arrow::utf8())});
    case 2:
      return ::arrow::schema({::arrow::field("a", ::arrow::utf8()),
                              ::arrow::field("b", ::arrow::utf8()),
                              ::arrow::field("c", ::arrow::utf8())});
    case 3:
      return ::arrow::schema({::arrow::field("x", ::arrow::int64())});
    default:
      return ::arrow::schema({::arrow::field(
          "a", (selector & 4) ? ::arrow::uint64() : ::arrow::int64())});
  }
}

void MakeFuzzJsonOptions(const uint8_t* header, ReadOptions* read_options,
                         ParseOptions* parse_options) {
  *read_options = ReadOptions::Defaults();
  *parse_options = ParseOptions::Defaults();

  // --- ParseOptions ---
  switch (header[1] % 3) {
    case 0:
      parse_options->unexpected_field_behavior = UnexpectedFieldBehavior::InferType;
      break;
    case 1:
      parse_options->unexpected_field_behavior = UnexpectedFieldBehavior::Ignore;
      break;
    default:
      parse_options->unexpected_field_behavior = UnexpectedFieldBehavior::Error;
      break;
  }
  parse_options->newlines_in_values = (header[1] & 4) != 0;
  parse_options->explicit_schema = MakeExplicitSchema(header[2]);

  // --- ReadOptions ---
  const int32_t kBlockSizes[] = {1024, 4096, 65536, 1 << 20};
  read_options->block_size = kBlockSizes[(header[3] >> 1) % 4];
  read_options->use_threads = (header[3] & 1) != 0;
}

// Merge a Result's status into the aggregate without aborting the other
// reader paths, so a failure in one path cannot mask coverage of the rest.
void MergeStatus(Status st, Status* out) { *out &= std::move(st); }

Status ReadAllWays(const ReadOptions& read_options_in,
                   const ParseOptions& parse_options,
                   const std::shared_ptr<Buffer>& json) {
  Status st;

  // 1 & 2. Full table reads, single- and multi-threaded. Each path gets a
  // fresh BufferReader over the same bytes since readers consume the stream.
  for (const bool use_threads : {false, true}) {
    auto read_options = read_options_in;
    read_options.use_threads = use_threads;
    auto input = std::make_shared<::arrow::io::BufferReader>(json);
    auto maybe_reader =
        TableReader::Make(default_memory_pool(), input, read_options, parse_options);
    if (!maybe_reader.ok()) {
      MergeStatus(maybe_reader.status(), &st);
      continue;
    }
    auto maybe_table = (*maybe_reader)->Read();
    if (maybe_table.ok()) {
      MergeStatus((*maybe_table)->ValidateFull(), &st);
    } else {
      MergeStatus(maybe_table.status(), &st);
    }
  }

  // 3. Incremental (streaming) read, multi-threaded.
  {
    auto read_options = read_options_in;
    read_options.use_threads = true;
    auto input = std::make_shared<::arrow::io::BufferReader>(json);
    auto maybe_reader = StreamingReader::Make(input, read_options, parse_options);
    if (!maybe_reader.ok()) {
      MergeStatus(maybe_reader.status(), &st);
    } else {
      auto maybe_table = (*maybe_reader)->ToTable();
      if (maybe_table.ok()) {
        MergeStatus((*maybe_table)->ValidateFull(), &st);
      } else {
        MergeStatus(maybe_table.status(), &st);
      }
    }
  }

  // 4. Single-object parse.
  {
    auto maybe_batch = ParseOne(parse_options, json);
    if (maybe_batch.ok()) {
      MergeStatus((*maybe_batch)->ValidateFull(), &st);
    } else {
      MergeStatus(maybe_batch.status(), &st);
    }
  }

  return st;
}

}  // namespace

Status FuzzJsonReader(const uint8_t* data, int64_t size) {
  // Since the Fuzz-allocated data is not owned, any task that outlives the
  // readers may try to read memory that has been deallocated. Hence we wait
  // for all pending tasks to end before leaving.
  struct TaskGuard {
    ~TaskGuard() { GetCpuThreadPool()->WaitForIdle(); }
  } task_guard;

  ReadOptions read_options;
  ParseOptions parse_options;
  const uint8_t* payload = data;
  int64_t payload_size = size;
  if (size >= kFuzzHeaderSize && data[0] == kFuzzMarker) {
    MakeFuzzJsonOptions(data, &read_options, &parse_options);
    payload += kFuzzHeaderSize;
    payload_size -= kFuzzHeaderSize;
  } else {
    // Default options for inputs without a full header. A small block size
    // makes chunking more likely to be exercised.
    read_options = ReadOptions::Defaults();
    read_options.use_threads = false;
    read_options.block_size = 1024;
    parse_options = ParseOptions::Defaults();
  }

  auto json = std::make_shared<Buffer>(payload, payload_size);
  return ReadAllWays(read_options, parse_options, json);
}

}  // namespace arrow::json

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto status = arrow::json::FuzzJsonReader(data, static_cast<int64_t>(size));
  return arrow::internal::LogFuzzStatus(status, data, size);
}