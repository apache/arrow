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

#ifndef ARROW_JSON_READER_H
#define ARROW_JSON_READER_H

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/json/options.h"  // IWYU pragma: keep
#include "arrow/json/parser.h"   // IWYU pragma: keep
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class Table;

namespace io {
class InputStream;
}  // namespace io

namespace json {

class ARROW_EXPORT TableReader {
 public:
  virtual ~TableReader() = default;

  virtual Status Read(std::shared_ptr<Table>* out) = 0;

  // XXX pass optional schema?
  static Status Make(MemoryPool* pool, std::shared_ptr<io::InputStream> input,
                     const ReadOptions&, const ParseOptions&,
                     std::shared_ptr<TableReader>* out);
};

ARROW_EXPORT Status ParseOne(ParseOptions options, std::shared_ptr<Buffer> json,
                             std::shared_ptr<RecordBatch>* out);

}  // namespace json
}  // namespace arrow

#endif  // ARROW_JSON_READER_H
