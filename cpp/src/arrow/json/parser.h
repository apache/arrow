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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/builder.h"
#include "arrow/json/options.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"
#include "arrow/visitor_inline.h"

namespace arrow {

class MemoryPool;
class RecordBatch;

namespace json {

struct Kind {
  enum type : uint8_t { kNull, kBoolean, kNumber, kString, kArray, kObject };
};

inline static const std::shared_ptr<const KeyValueMetadata>& Tag(Kind::type k) {
  static std::shared_ptr<const KeyValueMetadata> tags[] = {
      key_value_metadata({{"json_kind", "null"}}),
      key_value_metadata({{"json_kind", "boolean"}}),
      key_value_metadata({{"json_kind", "number"}}),
      key_value_metadata({{"json_kind", "string"}}),
      key_value_metadata({{"json_kind", "array"}}),
      key_value_metadata({{"json_kind", "object"}})};
  return tags[static_cast<uint8_t>(k)];
}

inline static Status KindForType(const DataType& type, Kind::type* kind) {
  struct {
    Status Visit(const NullType&) { return SetKind(Kind::kNull); }
    Status Visit(const BooleanType&) { return SetKind(Kind::kBoolean); }
    Status Visit(const Number&) { return SetKind(Kind::kNumber); }
    // XXX should TimeType & DateType be Kind::kNumber or Kind::kString?
    Status Visit(const TimeType&) { return SetKind(Kind::kNumber); }
    Status Visit(const DateType&) { return SetKind(Kind::kNumber); }
    Status Visit(const BinaryType&) { return SetKind(Kind::kString); }
    // XXX should Decimal128Type be Kind::kNumber or Kind::kString?
    Status Visit(const FixedSizeBinaryType&) { return SetKind(Kind::kString); }
    Status Visit(const DictionaryType& dict_type) {
      return KindForType(*dict_type.dictionary()->type(), kind_);
    }
    Status Visit(const ListType&) { return SetKind(Kind::kArray); }
    Status Visit(const StructType&) { return SetKind(Kind::kObject); }
    Status Visit(const DataType& not_impl) {
      return Status::NotImplemented("JSON parsing of ", not_impl);
    }
    Status SetKind(Kind::type kind) {
      *kind_ = kind;
      return Status::OK();
    }
    Kind::type* kind_;
  } visitor = {kind};
  return VisitTypeInline(type, &visitor);
}

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
  BlockParser(MemoryPool* pool, ParseOptions options,
              const std::shared_ptr<Buffer>& scalar_storage);
  BlockParser(ParseOptions options, const std::shared_ptr<Buffer>& scalar_storage);

  /// \brief Parse a block of data insitu (destructively)
  /// \warning The input must be null terminated
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
