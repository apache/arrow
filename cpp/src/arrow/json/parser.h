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

#ifndef ARROW_JSON_PARSER_H
#define ARROW_JSON_PARSER_H

#include <memory>
#include <string>

#include "arrow/json/options.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class MemoryPool;
class KeyValueMetadata;
class ResizableBuffer;

namespace json {

struct Kind {
  enum type : uint8_t { kNull, kBoolean, kNumber, kString, kArray, kObject };

  static const std::string& Name(Kind::type);

  static const std::shared_ptr<const KeyValueMetadata>& Tag(Kind::type);

  static Kind::type FromTag(const std::shared_ptr<const KeyValueMetadata>& tag);

  static Status ForType(const DataType& type, Kind::type* kind);
};

constexpr int32_t kMaxParserNumRows = 100000;

/// \class BlockParser
/// \brief A reusable block-based parser for JSON data
///
/// The parser takes a block of newline delimited JSON data and extracts
/// keys and value pairs, inserting into provided ArrayBuilders.
/// Parsed data is own by the
/// parser, so the original buffer can be discarded after Parse() returns.
class ARROW_EXPORT BlockParser {
 public:
  BlockParser(MemoryPool* pool, const ParseOptions& options,
              const std::shared_ptr<ResizableBuffer>& scalar_storage);
  BlockParser(ParseOptions options,
              const std::shared_ptr<ResizableBuffer>& scalar_storage);

  /// \brief Parse a block of data
  Status Parse(const std::shared_ptr<Buffer>& json) { return impl_->Parse(json); }

  /// \brief Extract parsed data
  Status Finish(std::shared_ptr<Array>* parsed) { return impl_->Finish(parsed); }

  /// \brief Return the number of parsed rows
  int32_t num_rows() const { return impl_->num_rows(); }

  struct Impl {
    virtual ~Impl() = default;
    virtual Status Parse(const std::shared_ptr<Buffer>& json) = 0;
    virtual Status Finish(std::shared_ptr<Array>* parsed) = 0;
    virtual int32_t num_rows() = 0;
  };

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(BlockParser);

  MemoryPool* pool_;
  const ParseOptions options_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace json
}  // namespace arrow

#endif  // ARROW_JSON_PARSER_H
