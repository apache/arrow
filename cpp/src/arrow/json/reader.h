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

#include <memory>

#include "arrow/io/type_fwd.h"
#include "arrow/json/options.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace json {

/// A class that reads an entire JSON file into a Arrow Table
///
/// The file is expected to consist of individual line-separated JSON objects
class ARROW_EXPORT TableReader {
 public:
  virtual ~TableReader() = default;

  /// Read the entire JSON file and convert it to a Arrow Table
  virtual Result<std::shared_ptr<Table>> Read() = 0;

  /// Create a TableReader instance
  static Result<std::shared_ptr<TableReader>> Make(MemoryPool* pool,
                                                   std::shared_ptr<io::InputStream> input,
                                                   const ReadOptions&,
                                                   const ParseOptions&);
};

ARROW_EXPORT Result<std::shared_ptr<RecordBatch>> ParseOne(ParseOptions options,
                                                           std::shared_ptr<Buffer> json);

/// \brief A class that reads a JSON file incrementally
///
/// JSON data is read from a stream in fixed-size blocks (configurable with
/// `ReadOptions::block_size`). Each block is converted to a `RecordBatch`. Yielded
/// batches have a consistent schema but may differ in row count.
///
/// The supplied `ParseOptions` are used to determine a schema, based either on a
/// provided explicit schema or inferred from the first non-empty block.
/// Afterwards, the schema is frozen and unexpected fields will be ignored on
/// subsequent reads (unless `UnexpectedFieldBehavior::Error` was specified).
///
/// For each block, the reader will launch its subsequent parsing/decoding task on the
/// given `cpu_executor` - potentially in parallel. If `ReadOptions::use_threads` is
/// specified, readahead will be applied to these tasks in accordance with the executor's
/// capacity.
class ARROW_EXPORT StreamingReader : public RecordBatchReader {
 public:
  virtual ~StreamingReader() = default;

  /// \brief Read the next `RecordBatch` asynchronously
  ///
  /// This function is async-reentrant (but not synchronously reentrant)
  virtual Future<std::shared_ptr<RecordBatch>> ReadNextAsync() = 0;

  /// \brief Return the number of bytes which have been read and processed
  ///
  /// The returned number includes JSON bytes which the StreamingReader has finished
  /// processing, but not bytes for which some processing (e.g. JSON parsing or conversion
  /// to Arrow layout) is still ongoing.
  [[nodiscard]] virtual int64_t bytes_read() const = 0;

  /// \brief Create a `StreamingReader` instance asynchronously
  ///
  /// This involves some I/O as the first batch must be loaded during the creation process
  /// so it is returned as a future
  static Future<std::shared_ptr<StreamingReader>> MakeAsync(
      std::shared_ptr<io::InputStream> stream, io::IOContext io_context,
      ::arrow::internal::Executor* cpu_executor, const ReadOptions&, const ParseOptions&);

  /// \brief Create a `StreamingReader` instance
  static Result<std::shared_ptr<StreamingReader>> Make(
      std::shared_ptr<io::InputStream> stream, io::IOContext io_context,
      ::arrow::internal::Executor* cpu_executor, const ReadOptions&, const ParseOptions&);
};

}  // namespace json
}  // namespace arrow
