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

#include "arrow/csv/chunker.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include <sstream>
#include <string>

namespace arrow {
namespace csv {

Chunker::Chunker(ParseOptions options, int32_t max_num_rows)
    : options_(options), max_num_rows_(max_num_rows) {}

// NOTE: cvsmonkey (https://github.com/dw/csvmonkey) has optimization ideas

template <bool quoting, bool escaping>
inline const char* Chunker::ReadLine(const char* data, const char* data_end) {
  DCHECK_EQ(quoting, options_.quoting);
  DCHECK_EQ(escaping, options_.escaping);

  // The parsing state machine
  char c;

FieldStart:
  // At the start of a field
  // Quoting is only recognized at start of field
  if (quoting && ARROW_PREDICT_TRUE(data != data_end) && *data == options_.quote_char) {
    data++;
    goto InQuotedField;
  } else {
    goto InField;
  }

InField:
  // Inside a non-quoted part of a field
  if (ARROW_PREDICT_FALSE(data == data_end)) {
    goto AbortLine;
  }
  c = *data++;
  if (escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      goto AbortLine;
    }
    data++;
    goto InField;
  }
  if (ARROW_PREDICT_FALSE(c == '\r')) {
    if (ARROW_PREDICT_TRUE(data != data_end) && *data == '\n') {
      data++;
    }
    goto LineEnd;
  }
  if (ARROW_PREDICT_FALSE(c == '\n')) {
    goto LineEnd;
  }
  if (ARROW_PREDICT_FALSE(c == options_.delimiter)) {
    goto FieldEnd;
  }
  goto InField;

InQuotedField:
  // Inside a quoted part of a field
  if (ARROW_PREDICT_FALSE(data == data_end)) {
    goto AbortLine;
  }
  c = *data++;
  if (escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
    if (data == data_end) {
      goto AbortLine;
    }
    data++;
    goto InQuotedField;
  }
  if (ARROW_PREDICT_FALSE(c == options_.quote_char)) {
    if (options_.double_quote && data != data_end && *data == options_.quote_char) {
      // Double-quoting
      data++;
    } else {
      // End of single-quoting
      goto InField;
    }
  }
  goto InQuotedField;

FieldEnd:
  // At the end of a field
  goto FieldStart;

LineEnd:
  // At the end of line, possibly in the middle of the newline separator
  //   if (ARROW_PREDICT_TRUE(data < data_end) && data[-1] == '\r' && *data == '\n') {
  //     data++;
  //   }
  return data;

AbortLine:
  // Truncated line at end of block
  return nullptr;
}

template <bool quoting, bool escaping>
Status Chunker::ProcessSpecialized(const char* start, uint32_t size, uint32_t* out_size) {
  DCHECK_EQ(quoting, options_.quoting);
  DCHECK_EQ(escaping, options_.escaping);

  num_rows_ = 0;
  const char* data = start;
  const char* data_end = start + size;

  while (data < data_end && num_rows_ < max_num_rows_) {
    const char* line_end = ReadLine<quoting, escaping>(data, data_end);
    if (line_end == nullptr) {
      // Cannot read any further
      break;
    }
    data = line_end;
    ++num_rows_;
  }
  *out_size = static_cast<uint32_t>(data - start);
  return Status::OK();
}

Status Chunker::Process(const char* start, uint32_t size, uint32_t* out_size) {
  if (options_.quoting) {
    if (options_.escaping) {
      return ProcessSpecialized<true, true>(start, size, out_size);
    } else {
      return ProcessSpecialized<true, false>(start, size, out_size);
    }
  } else {
    if (options_.escaping) {
      return ProcessSpecialized<false, true>(start, size, out_size);
    } else {
      return ProcessSpecialized<false, false>(start, size, out_size);
    }
  }
}

}  // namespace csv
}  // namespace arrow
