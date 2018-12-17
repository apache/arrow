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
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class RecordBatch;

namespace json {

/// \class AdaptiveArrayBuilder
/// \brief Base class for array builders in contexts where the
///        final type is unknown
class AdaptiveArrayBuilder {
 public:
  AdaptiveArrayBuilder(std::shared_ptr<DataType> type) : type_(type) {}

  virtual ~AdaptiveArrayBuilder() = default;

  /// Complete the built array
  virtual Status Finish(std::shared_ptr<Array>* out) = 0;

  /// If necessary, promote this builder to accommodate the given type.
  /// It is not guaranteed that type() == type after this method is called.
  /// It is possible that a new builder must be constructed
  /// in order to accomodate this fallback. If this occurs,
  /// *out will point to the new builder and the object against
  /// which this method was called should be considered moved from.
  virtual Status MaybePromoteTo(std::shared_ptr<DataType> type,
                                std::unique_ptr<AdaptiveArrayBuilder>* out) = 0;

  /// The current type of this builder
  ///
  /// \warning this property may be lazily updated, it is only guaranteed
  /// to be accurate after a call to UpdateType()
  std::shared_ptr<DataType> type() { return type_; }

  int64_t length() { return length_; }

  /// Force update of built type. If an implementation of AdaptiveArrayBuilder updates
  /// type lazily, UpdateType() must be overridden. Implementations may not require that
  /// it be called before Finish() or MaybePromoteTo()
  virtual Status UpdateType() { return Status::OK(); }

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(AdaptiveArrayBuilder);

  int64_t length_ = 0;
  std::shared_ptr<DataType> type_;
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
  explicit BlockParser(ParseOptions options, int32_t num_cols = -1,
                       int32_t max_num_rows = kMaxParserNumRows);
  explicit BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols = -1,
                       int32_t max_num_rows = kMaxParserNumRows);

  /// \brief Parse a block of data
  ///
  /// Parse a block of JSON data, ingesting up to max_num_rows rows.
  /// The number of bytes actually parsed is returned in out_size.
  Status Parse(const char* data, uint32_t size, uint32_t* out_size);

  /// \brief Extract parsed data as a RecordBatch
  Status Finish(std::shared_ptr<RecordBatch>* parsed) {
    *parsed = parsed_;
    return Status::OK();
  }

  /// \brief Return the number of parsed rows
  int32_t num_rows() const { return num_rows_; }
  /// \brief Return the number of parsed columns
  int32_t num_cols() const { return num_cols_; }
  /// \brief Return the total size in bytes of parsed data
  uint32_t num_bytes() const { return parsed_size_; }

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(BlockParser);

  template <unsigned Flags, typename Handler>
  Status DoParse(Handler& handler, const char* data, uint32_t size, uint32_t* out_size);

  MemoryPool* pool_;
  const ParseOptions options_;
  // The number of rows parsed from the block
  int32_t num_rows_;
  // The number of columns (can be -1 at start)
  int32_t num_cols_;
  // The maximum number of rows to parse from this block
  int32_t max_num_rows_;
  /// The total size in bytes of parsed data
  int32_t parsed_size_;
  /// In the case of a known schema, the RecordBatch for a chunk can be finalized
  /// as soon as the chunk has been parsed
  std::shared_ptr<RecordBatch> parsed_;
  /// In the case of type inference, we can't finish until all blocks have been
  /// examined. (For example, another block might define a column not present here)
  std::unique_ptr<AdaptiveArrayBuilder> adaptive_builder_;
};

}  // namespace json
}  // namespace arrow

#endif  // ARROW_JSON_PARSER_H
