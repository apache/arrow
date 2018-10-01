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

#include "arrow/csv/parser.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include <sstream>
#include <string>

namespace arrow {
namespace csv {

static Status ParseError(const char* message) {
  std::stringstream ss;
  ss << "CSV parse error: " << message;
  return Status::Invalid(ss.str());
}

static Status MismatchingColumns(int32_t expected, int32_t actual) {
  char s[50];
  snprintf(s, sizeof(s), "Expected %d columns, got %d", expected, actual);
  return ParseError(s);
}

BlockParser::BlockParser(ParseOptions options, int32_t num_cols, int32_t max_num_rows)
    : options_(options), num_cols_(num_cols), max_num_rows_(max_num_rows) {}

Status BlockParser::ParseLine(const char* data, const char* data_end, bool is_final,
                              const char** out_data) {
  int32_t num_cols = 0;
  char c;
#ifdef CSV_PARSER_USE_BITFIELD
  bool quoted;
#endif

  auto saved_parsed_size = parsed_.size();
#ifdef CSV_PARSER_USE_BITFIELD
  auto saved_values_size = values_.size();
#else
  auto saved_offsets_size = offsets_.size();
  auto saved_quoted_size = quoted_.size();
#endif

  // Subroutines to manage parser state
  auto InitField = [&]() {};
  auto PushFieldChar = [&](char c) { parsed_.push_back(static_cast<uint8_t>(c)); };
  auto FinishField = [&]() {
#ifdef CSV_PARSER_USE_BITFIELD
    ValueDesc v = {static_cast<uint32_t>(parsed_.size()) & 0x7fffffffU, quoted};
    values_.push_back(v);
#else
    offsets_.push_back(parsed_.size());
#endif
    ++num_cols;
  };
  auto RewindState = [&]() {
    parsed_.resize(saved_parsed_size);
#ifdef CSV_PARSER_USE_BITFIELD
    values_.resize(saved_values_size);
#else
    offsets_.resize(saved_offsets_size);
    quoted_.resize(saved_quoted_size);
#endif
  };

  // The parsing state machine

FieldStart:
  // At the start of a field
  InitField();
  // Quoting is only recognized at start of field
  if (options_.quoting && data != data_end && *data == options_.quote_char) {
    ++data;
#ifdef CSV_PARSER_USE_BITFIELD
    quoted = true;
#else
    quoted_.push_back(true);
#endif
    goto InQuotedField;
  } else {
#ifdef CSV_PARSER_USE_BITFIELD
    quoted = false;
#else
    quoted_.push_back(false);
#endif
    goto InField;
  }

InField:
  // Inside a non-quoted part of a field
  if (data == data_end) {
    goto AbortLine;
  }
  c = *data++;
  if (options_.escaping && c == options_.escape_char) {
    if (data == data_end) {
      goto AbortLine;
    }
    c = *data++;
    PushFieldChar(c);
    goto InField;
  }
  if (c == '\n' || c == '\r') {
    goto LineEnd;
  }
  if (c == options_.delimiter) {
    goto FieldEnd;
  }
  PushFieldChar(c);
  goto InField;

InQuotedField:
  // Inside a quoted part of a field
  if (data == data_end) {
    goto AbortLine;
  }
  c = *data++;
  if (options_.escaping && c == options_.escape_char) {
    if (data == data_end) {
      goto AbortLine;
    }
    c = *data++;
    PushFieldChar(c);
    goto InQuotedField;
  }
  if (c == options_.quote_char) {
    if (options_.double_quote && data < data_end && *data == options_.quote_char) {
      // Double-quoting
      ++data;
    } else {
      // End of single-quoting
      goto InField;
    }
  }
  PushFieldChar(c);
  goto InQuotedField;

FieldEnd:
  // At the end of a field
  FinishField();
  goto FieldStart;

LineEnd:
  // At the end of line, possibly in the middle of the newline separator
  FinishField();
  if (data < data_end && data[-1] == '\r' && *data == '\n') {
    data++;
  }
  if (num_cols_ == -1) {
    num_cols_ = num_cols;
  } else if (num_cols != num_cols_) {
    return MismatchingColumns(num_cols_, num_cols);
  }
  *out_data = data;
  return Status::OK();

AbortLine:
  // Not a full line except perhaps if in final block
  if (is_final) {
    FinishField();
    if (num_cols_ == -1) {
      num_cols_ = num_cols;
    } else if (num_cols != num_cols_) {
      return MismatchingColumns(num_cols_, num_cols);
    }
    *out_data = data;
    return Status::OK();
  }
  // Truncated line at end of block, rewind parsed state
  RewindState();
  return Status::OK();
}

Status BlockParser::DoParse(const char* start, uint32_t size, bool is_final,
                            uint32_t* out_size) {
  num_rows_ = 0;
  // These don't shrink the allocated capacity, so reuses of BlockParser
  // avoid most allocations when appending data
  parsed_.clear();
#ifdef CSV_PARSER_USE_BITFIELD
  values_.clear();
  values_.push_back({0, false});
#else
  offsets_.clear();
  offsets_.push_back(0);
  quoted_.clear();
#endif

  const char* data = start;
  const char* data_end = start + size;

  while (data < data_end && num_rows_ < max_num_rows_) {
    const char* line_end = data;
    RETURN_NOT_OK(ParseLine(data, data_end, is_final, &line_end));
    if (line_end == data) {
      // Cannot parse any further
      break;
    }
    data = line_end;
    ++num_rows_;
  }
  if (num_cols_ == -1) {
    DCHECK_EQ(num_rows_, 0);
  }
#ifdef CSV_PARSER_USE_BITFIELD
  DCHECK_EQ(values_.size(), 1 + static_cast<size_t>(num_rows_ * num_cols_));
  DCHECK_EQ(parsed_.size(), values_[values_.size() - 1].offset);
#else
  DCHECK_EQ(offsets_.size(), 1 + static_cast<size_t>(num_rows_ * num_cols_));
  DCHECK_EQ(parsed_.size(), offsets_[offsets_.size() - 1]);
#endif
  *out_size = static_cast<uint32_t>(data - start);
  return Status::OK();
}

Status BlockParser::Parse(const char* data, uint32_t size, uint32_t* out_size) {
  return DoParse(data, size, false /* is_final */, out_size);
}

Status BlockParser::ParseFinal(const char* data, uint32_t size, uint32_t* out_size) {
  return DoParse(data, size, true /* is_final */, out_size);
}

}  // namespace csv
}  // namespace arrow
