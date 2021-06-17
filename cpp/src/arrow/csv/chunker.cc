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

#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace csv {

namespace {

// NOTE: csvmonkey (https://github.com/dw/csvmonkey) has optimization ideas

template <bool quoting, bool escaping>
class Lexer {
 public:
  enum State {
    FIELD_START,
    IN_FIELD,
    AT_ESCAPE,
    IN_QUOTED_FIELD,
    AT_QUOTED_QUOTE,
    AT_QUOTED_ESCAPE
  };

  explicit Lexer(const ParseOptions& options) : options_(options) {
    DCHECK_EQ(quoting, options_.quoting);
    DCHECK_EQ(escaping, options_.escaping);
  }

  const char* ReadLine(const char* data, const char* data_end) {
    // The parsing state machine
    char c;
    DCHECK_GT(data_end - data, 0);
    if (ARROW_PREDICT_TRUE(state_ == FIELD_START)) {
      goto FieldStart;
    }
    switch (state_) {
      case FIELD_START:
        goto FieldStart;
      case IN_FIELD:
        goto InField;
      case AT_ESCAPE:
        goto AtEscape;
      case IN_QUOTED_FIELD:
        goto InQuotedField;
      case AT_QUOTED_QUOTE:
        goto AtQuotedQuote;
      case AT_QUOTED_ESCAPE:
        goto AtQuotedEscape;
    }

  FieldStart:
    // At the start of a field
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      state_ = FIELD_START;
      goto AbortLine;
    }
    // Quoting is only recognized at start of field
    if (quoting && *data == options_.quote_char) {
      data++;
      goto InQuotedField;
    } else {
      goto InField;
    }

  InField:
    // Inside a non-quoted part of a field
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      state_ = IN_FIELD;
      goto AbortLine;
    }
    c = *data++;
    if (escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = AT_ESCAPE;
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

  AtEscape:
    // Coming here if last block ended on a non-quoted escape
    data++;
    goto InField;

  InQuotedField:
    // Inside a quoted part of a field
    if (ARROW_PREDICT_FALSE(data == data_end)) {
      state_ = IN_QUOTED_FIELD;
      goto AbortLine;
    }
    c = *data++;
    if (escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = AT_QUOTED_ESCAPE;
        goto AbortLine;
      }
      data++;
      goto InQuotedField;
    }
    if (ARROW_PREDICT_FALSE(c == options_.quote_char)) {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = AT_QUOTED_QUOTE;
        goto AbortLine;
      }
      if (options_.double_quote && *data == options_.quote_char) {
        // Double-quoting
        data++;
      } else {
        // End of single-quoting
        goto InField;
      }
    }
    goto InQuotedField;

  AtQuotedEscape:
    // Coming here if last block ended on a quoted escape
    data++;
    goto InQuotedField;

  AtQuotedQuote:
    // Coming here if last block ended on a quoted quote
    if (options_.double_quote && *data == options_.quote_char) {
      // Double-quoting
      data++;
      goto InQuotedField;
    } else {
      // End of single-quoting
      goto InField;
    }

  FieldEnd:
    // At the end of a field
    goto FieldStart;

  LineEnd:
    state_ = FIELD_START;
    return data;

  AbortLine:
    // Truncated line
    return nullptr;
  }

 protected:
  const ParseOptions& options_;
  State state_ = FIELD_START;
};

// A BoundaryFinder implementation that assumes CSV cells can contain raw newlines,
// and uses actual CSV lexing to delimit them.
template <bool quoting, bool escaping>
class LexingBoundaryFinder : public BoundaryFinder {
 public:
  explicit LexingBoundaryFinder(ParseOptions options) : options_(std::move(options)) {}

  Status FindFirst(util::string_view partial, util::string_view block,
                   int64_t* out_pos) override {
    Lexer<quoting, escaping> lexer(options_);

    const char* line_end =
        lexer.ReadLine(partial.data(), partial.data() + partial.size());
    DCHECK_EQ(line_end, nullptr);  // Otherwise `partial` is a whole CSV line
    line_end = lexer.ReadLine(block.data(), block.data() + block.size());

    if (line_end == nullptr) {
      // No complete CSV line
      *out_pos = -1;
    } else {
      *out_pos = static_cast<int64_t>(line_end - block.data());
      DCHECK_GT(*out_pos, 0);
    }
    return Status::OK();
  }

  Status FindLast(util::string_view block, int64_t* out_pos) override {
    Lexer<quoting, escaping> lexer(options_);

    const char* data = block.data();
    const char* const data_end = block.data() + block.size();

    while (data < data_end) {
      const char* line_end = lexer.ReadLine(data, data_end);
      if (line_end == nullptr) {
        // Cannot read any further
        break;
      }
      DCHECK_GT(line_end, data);
      data = line_end;
    }
    if (data == block.data()) {
      // No complete CSV line
      *out_pos = -1;
    } else {
      *out_pos = static_cast<int64_t>(data - block.data());
      DCHECK_GT(*out_pos, 0);
    }
    return Status::OK();
  }

  Status FindNth(util::string_view partial, util::string_view block, int64_t count,
                 int64_t* out_pos, int64_t* num_found) override {
    Lexer<quoting, escaping> lexer(options_);
    int64_t found = 0;
    const char* data = block.data();
    const char* const data_end = block.data() + block.size();

    const char* line_end;
    if (partial.size()) {
      line_end = lexer.ReadLine(partial.data(), partial.data() + partial.size());
      DCHECK_EQ(line_end, nullptr);  // Otherwise `partial` is a whole CSV line
    }

    for (; data < data_end && found < count; ++found) {
      line_end = lexer.ReadLine(data, data_end);
      if (line_end == nullptr) {
        // Cannot read any further
        break;
      }
      DCHECK_GT(line_end, data);
      data = line_end;
    }

    if (data == block.data()) {
      // No complete CSV line
      *out_pos = kNoDelimiterFound;
    } else {
      *out_pos = static_cast<int64_t>(data - block.data());
    }
    *num_found = found;
    return Status::OK();
  }

 protected:
  ParseOptions options_;
};

}  // namespace

std::unique_ptr<Chunker> MakeChunker(const ParseOptions& options) {
  std::shared_ptr<BoundaryFinder> delimiter;
  if (!options.newlines_in_values) {
    delimiter = MakeNewlineBoundaryFinder();
  } else {
    if (options.quoting) {
      if (options.escaping) {
        delimiter = std::make_shared<LexingBoundaryFinder<true, true>>(options);
      } else {
        delimiter = std::make_shared<LexingBoundaryFinder<true, false>>(options);
      }
    } else {
      if (options.escaping) {
        delimiter = std::make_shared<LexingBoundaryFinder<false, true>>(options);
      } else {
        delimiter = std::make_shared<LexingBoundaryFinder<false, false>>(options);
      }
    }
  }
  return internal::make_unique<Chunker>(std::move(delimiter));
}

}  // namespace csv
}  // namespace arrow
