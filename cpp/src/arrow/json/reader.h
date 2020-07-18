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

#include "arrow/json/options.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Table;
class RecordBatch;
class Array;
class DataType;

namespace io {
class InputStream;
}  // namespace io

namespace json {

/// A class that reads an entire JSON file into a Arrow Table
///
/// The file is expected to consist of individual line-separated JSON objects
class ARROW_EXPORT TableReader {
 public:
  virtual ~TableReader() = default;

  /// Read the entire JSON file and convert it to a Arrow Table
  virtual Result<std::shared_ptr<Table>> Read() = 0;

  ARROW_DEPRECATED("Use Result-returning version")
  Status Read(std::shared_ptr<Table>* out);

  /// Create a TableReader instance
  static Result<std::shared_ptr<TableReader>> Make(MemoryPool* pool,
                                                   std::shared_ptr<io::InputStream> input,
                                                   const ReadOptions&,
                                                   const ParseOptions&);

  ARROW_DEPRECATED("Use Result-returning version")
  static Status Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                     const ReadOptions&, const ParseOptions&,
                     std::shared_ptr<TableReader>* out);
};

ARROW_EXPORT Result<std::shared_ptr<RecordBatch>> ParseOne(ParseOptions options,
                                                           std::shared_ptr<Buffer> json);

}  // namespace json
}  // namespace arrow
