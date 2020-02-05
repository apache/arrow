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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/csv/options.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

namespace csv {

constexpr int32_t kMaxParserNumRows = 100000;

/// Skip at most num_rows from the given input.  The input pointer is updated
/// and the number of actually skipped rows is returns (may be less than
/// requested if the input is too short).
ARROW_EXPORT
int32_t SkipRows(const uint8_t* data, uint32_t size, int32_t num_rows,
                 const uint8_t** out_data);

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
  explicit BlockParser(MemoryPool* pool, ParseOptions options, int32_t num_cols = -1,
                       int32_t max_num_rows = kMaxParserNumRows);

  /// \brief Parse a block of data
  ///
  /// Parse a block of CSV data, ingesting up to max_num_rows rows.
  /// The number of bytes actually parsed is returned in out_size.
  Status Parse(util::string_view data, uint32_t* out_size);

  /// \brief Parse sequential blocks of data
  ///
  /// Only the last block is allowed to be truncated.
  Status Parse(const std::vector<util::string_view>& data, uint32_t* out_size);

  /// \brief Parse the final block of data
  ///
  /// Like Parse(), but called with the final block in a file.
  /// The last row may lack a trailing line separator.
  Status ParseFinal(util::string_view data, uint32_t* out_size);

  /// \brief Parse the final sequential blocks of data
  ///
  /// Only the last block is allowed to be truncated.
  Status ParseFinal(const std::vector<util::string_view>& data, uint32_t* out_size);

  /// \brief Return the number of parsed rows
  int32_t num_rows() const { return num_rows_; }
  /// \brief Return the number of parsed columns
  int32_t num_cols() const { return num_cols_; }
  /// \brief Return the total size in bytes of parsed data
  uint32_t num_bytes() const { return parsed_size_; }

  /// \brief Visit parsed values in a column
  ///
  /// The signature of the visitor is
  /// Status(const uint8_t* data, uint32_t size, bool quoted)
  template <typename Visitor>
  Status VisitColumn(int32_t col_index, Visitor&& visit) const {
    for (size_t buf_index = 0; buf_index < values_buffers_.size(); ++buf_index) {
      const auto& values_buffer = values_buffers_[buf_index];
      const auto values = reinterpret_cast<const ValueDesc*>(values_buffer->data());
      const auto max_pos =
          static_cast<int32_t>(values_buffer->size() / sizeof(ValueDesc)) - 1;
      for (int32_t pos = col_index; pos < max_pos; pos += num_cols_) {
        auto start = values[pos].offset;
        auto stop = values[pos + 1].offset;
        auto quoted = values[pos + 1].quoted;
        ARROW_RETURN_NOT_OK(visit(parsed_ + start, stop - start, quoted));
      }
    }
    return Status::OK();
  }

  template <typename Visitor>
  Status VisitLastRow(Visitor&& visit) const {
    const auto& values_buffer = values_buffers_.back();
    const auto values = reinterpret_cast<const ValueDesc*>(values_buffer->data());
    const auto start_pos =
        static_cast<int32_t>(values_buffer->size() / sizeof(ValueDesc)) - num_cols_ - 1;
    for (int32_t col_index = 0; col_index < num_cols_; ++col_index) {
      auto start = values[start_pos + col_index].offset;
      auto stop = values[start_pos + col_index + 1].offset;
      auto quoted = values[start_pos + col_index + 1].quoted;
      ARROW_RETURN_NOT_OK(visit(parsed_ + start, stop - start, quoted));
    }
    return Status::OK();
  }

 protected:
  ARROW_DISALLOW_COPY_AND_ASSIGN(BlockParser);

  Status DoParse(const std::vector<util::string_view>& data, bool is_final,
                 uint32_t* out_size);
  template <typename SpecializedOptions>
  Status DoParseSpecialized(const std::vector<util::string_view>& data, bool is_final,
                            uint32_t* out_size);

  template <typename SpecializedOptions, typename ValuesWriter, typename ParsedWriter>
  Status ParseChunk(ValuesWriter* values_writer, ParsedWriter* parsed_writer,
                    const char* data, const char* data_end, bool is_final,
                    int32_t rows_in_chunk, const char** out_data, bool* finished_parsing);

  // Parse a single line from the data pointer
  template <typename SpecializedOptions, typename ValuesWriter, typename ParsedWriter>
  Status ParseLine(ValuesWriter* values_writer, ParsedWriter* parsed_writer,
                   const char* data, const char* data_end, bool is_final,
                   const char** out_data);

  MemoryPool* pool_;
  const ParseOptions options_;
  // The number of rows parsed from the block
  int32_t num_rows_;
  // The number of columns (can be -1 at start)
  int32_t num_cols_;
  // The maximum number of rows to parse from this block
  int32_t max_num_rows_;

  // Linear scratchpad for parsed values
  struct ValueDesc {
    uint32_t offset : 31;
    bool quoted : 1;
  };

  // XXX should we ensure the parsed buffer is padded with 8 or 16 excess zero bytes?
  // It may help with null parsing...
  std::vector<std::shared_ptr<Buffer>> values_buffers_;
  std::shared_ptr<Buffer> parsed_buffer_;
  const uint8_t* parsed_;
  int32_t values_size_;
  int32_t parsed_size_;

  class ResizableValuesWriter;
  class PresizedValuesWriter;
  class PresizedParsedWriter;
};

}  // namespace csv
}  // namespace arrow
