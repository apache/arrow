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

#ifndef ARROW_CSV_PARSER_H
#define ARROW_CSV_PARSER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/csv/options.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace csv {

constexpr int32_t kMaxParserNumRows = 100000;

// Whether BlockParser will use bitfields for better memory consumption
// and cache locality.
#undef CSV_PARSER_USE_BITFIELD
#define CSV_PARSER_USE_BITFIELD

/// \class BlockParser
/// \brief A reusable block-based parser for CSV data
///
/// The parser takes a block of CSV data and delimits rows and fields,
/// unquoting and unescaping them on the fly.  Parsed data is own by the
/// parser, so the original buffer can be discarded after Parse() returns.
///
/// If the block is truncated (i.e. not all data can be parsed), it is up
/// to the caller to arrange the next block to start with the trailing data.
/// Also, if the previous block ends with CR (0x0d) and a new block starts
/// with LF (0x0a), the parser will consider the leading newline as an empty
/// line; the caller should therefore strip it.
class ARROW_EXPORT BlockParser {
 public:
  explicit BlockParser(ParseOptions options, int32_t num_cols = -1,
                       int32_t max_num_rows = kMaxParserNumRows);

  /// \brief Parse a block of data
  ///
  /// Parse a block of CSV data, ingesting up to max_num_rows rows.
  /// The number of bytes actually parsed is returned in out_size.
  Status Parse(const char* data, uint32_t size, uint32_t* out_size);

  /// \brief Parse the final block of data
  ///
  /// Like Parse(), but called with the final block in a file.
  /// The last row may lack a trailing line separator.
  Status ParseFinal(const char* data, uint32_t size, uint32_t* out_size);

  int32_t num_rows() const { return num_rows_; }
  int32_t num_cols() const { return num_cols_; }
  /// \brief Return the total size in bytes of parsed data
  int32_t num_bytes() const { return static_cast<int32_t>(parsed_.size()); }

  /// \brief Visit parsed values in a column
  ///
  /// The signature of the visitor is (const uint8_t* data, uint32_t size, bool quoted)
  template <typename Visitor>
  Status VisitColumn(int32_t col_index, Visitor&& visit) const {
#ifdef CSV_PARSER_USE_BITFIELD
    for (int32_t pos = col_index; pos < num_cols_ * num_rows_; pos += num_cols_) {
      auto start = values_[pos].offset;
      auto stop = values_[pos + 1].offset;
      auto quoted = values_[pos + 1].quoted;
      ARROW_RETURN_NOT_OK(visit(parsed_.data() + start, stop - start, quoted));
    }
#else
    for (int32_t pos = col_index; pos < num_cols_ * num_rows_; pos += num_cols_) {
      auto start = offsets_[pos];
      auto stop = offsets_[pos + 1];
      auto quoted = quoted_[pos];
      ARROW_RETURN_NOT_OK(visit(parsed_.data() + start, stop - start, quoted));
    }
#endif
    return Status::OK();
  }

  // XXX add a ClearColumn method to signal that a column won't be visited anymore?

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(BlockParser);

  Status DoParse(const char* data, uint32_t size, bool is_final, uint32_t* out_size);

  // Parse a single line from the data pointer
  Status ParseLine(const char* data, const char* data_end, bool is_final,
                   const char** out_data);

  ParseOptions options_;
  // The number of rows parsed from the block
  int32_t num_rows_;
  // The number of columns (can be -1 at start)
  int32_t num_cols_;
  // The maximum number of rows to parse from this block
  int32_t max_num_rows_;

  // Linear scratchpad for parsed values
  // XXX should we ensure it's padded with 8 or 16 excess zero bytes? it could help
  // with null parsing...
  // TODO should be able to presize scratch space
  std::vector<uint8_t> parsed_;
#ifdef CSV_PARSER_USE_BITFIELD
  struct ValueDesc {
    uint32_t offset : 31;
    bool quoted : 1;
  };
  std::vector<ValueDesc> values_;
#else
  // Value offsets inside the scratchpad
  std::vector<uint32_t> offsets_;
  // Whether each value was quoted or not
  std::vector<bool> quoted_;
#endif
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_PARSER_H
