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

#include "arrow/csv/options.h"  // IWYU pragma: keep
#include "arrow/io/interfaces.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/future.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace io {
class InputStream;
}  // namespace io

namespace csv {

/// A class that reads an entire CSV file into a Arrow Table
class ARROW_EXPORT TableReader {
 public:
  virtual ~TableReader() = default;

  /// Read the entire CSV file and convert it to a Arrow Table
  virtual Result<std::shared_ptr<Table>> Read() = 0;
  /// Read the entire CSV file and convert it to a Arrow Table
  virtual Future<std::shared_ptr<Table>> ReadAsync() = 0;

  /// Create a TableReader instance
  static Result<std::shared_ptr<TableReader>> Make(io::IOContext io_context,
                                                   std::shared_ptr<io::InputStream> input,
                                                   const ReadOptions&,
                                                   const ParseOptions&,
                                                   const ConvertOptions&);

  ARROW_DEPRECATED("Use MemoryPool-less variant (the IOContext holds a pool already)")
  static Result<std::shared_ptr<TableReader>> Make(
      MemoryPool* pool, io::IOContext io_context, std::shared_ptr<io::InputStream> input,
      const ReadOptions&, const ParseOptions&, const ConvertOptions&);
};

/// Experimental
class ARROW_EXPORT StreamingReader : public RecordBatchReader {
 public:
  virtual ~StreamingReader() = default;

  /// Create a StreamingReader instance
  static Result<std::shared_ptr<StreamingReader>> Make(
      io::IOContext io_context, std::shared_ptr<io::InputStream> input,
      const ReadOptions&, const ParseOptions&, const ConvertOptions&);

  ARROW_DEPRECATED("Use IOContext-based overload")
  static Result<std::shared_ptr<StreamingReader>> Make(
      MemoryPool* pool, std::shared_ptr<io::InputStream> input,
      const ReadOptions& read_options, const ParseOptions& parse_options,
      const ConvertOptions& convert_options);
};

}  // namespace csv
}  // namespace arrow
