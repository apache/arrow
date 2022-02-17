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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

#include "arrow/csv/lexing_internal.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace csv {

namespace {

// NOTE: csvmonkey (https://github.com/dw/csvmonkey) has optimization ideas

template <typename SpecializedOptions>
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

  explicit Lexer(const ParseOptions& options)
      : options_(options), bulk_filter_(options_) {
    DCHECK_EQ(SpecializedOptions::quoting, options_.quoting);
    DCHECK_EQ(SpecializedOptions::escaping, options_.escaping);
  }

  void Reset() { state_ = FIELD_START; }

  // Decide whether it's worth using a bulk filter over the given data area
  bool ShouldUseBulkFilter(const char* data, const char* data_end) {
    constexpr int32_t kWordSize = static_cast<int32_t>(sizeof(BulkWordType));

    // Only probe the 32 first words and assume they are representative of the rest
    const int64_t n_words = std::min<int64_t>(32, (data_end - data) / kWordSize);
    int64_t n_skips = 0;
    const auto words = reinterpret_cast<const BulkWordType*>(data);
    for (int64_t i = 0; i < n_words - 3; i += 4) {
      const auto a = words[i + 0];
      const auto b = words[i + 1];
      const auto c = words[i + 2];
      const auto d = words[i + 3];
      n_skips += !bulk_filter_.Matches(a) + !bulk_filter_.Matches(b) +
                 !bulk_filter_.Matches(c) + !bulk_filter_.Matches(d);
    }
    return (n_skips * 4 + 1 >= n_words);
  }

  template <bool UseBulkFilter = true>
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
        // will never reach here if escaping = false
        // just to hint the compiler to remove dead code
        if (!SpecializedOptions::escaping) return nullptr;
        goto AtEscape;
      case IN_QUOTED_FIELD:
        if (!SpecializedOptions::quoting) return nullptr;
        goto InQuotedField;
      case AT_QUOTED_QUOTE:
        if (!SpecializedOptions::quoting) return nullptr;
        goto AtQuotedQuote;
      case AT_QUOTED_ESCAPE:
        if (!SpecializedOptions::quoting) return nullptr;
        goto AtQuotedEscape;
    }

  FieldStart:
    if (!SpecializedOptions::quoting) {
      goto InField;
    } else {
      // At the start of a field
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = FIELD_START;
        goto AbortLine;
      }
      // Quoting is only recognized at start of field
      if (*data == options_.quote_char) {
        data++;
        goto InQuotedField;
      } else {
        goto InField;
      }
    }

  InField:
    // Inside a non-quoted part of a field
    if (UseBulkFilter) {
      const char* bulk_end = RunBulkFilter(data, data_end);
      if (ARROW_PREDICT_FALSE(bulk_end == nullptr)) {
        state_ = IN_FIELD;
        goto AbortLine;
      }
      data = bulk_end;
    } else {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = IN_FIELD;
        goto AbortLine;
      }
    }
    c = *data++;
    if (SpecializedOptions::escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
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
    // treat delimiter as a normal token if quoting is disabled
    if (ARROW_PREDICT_FALSE(SpecializedOptions::quoting && c == options_.delimiter)) {
      goto FieldEnd;
    }
    goto InField;

  AtEscape:
    // Coming here if last block ended on a non-quoted escape
    data++;
    goto InField;

  InQuotedField:
    // Inside a quoted part of a field
    if (UseBulkFilter) {
      const char* bulk_end = RunBulkFilter(data, data_end);
      if (ARROW_PREDICT_FALSE(bulk_end == nullptr)) {
        state_ = IN_QUOTED_FIELD;
        goto AbortLine;
      }
      data = bulk_end;
    } else {
      if (ARROW_PREDICT_FALSE(data == data_end)) {
        state_ = IN_QUOTED_FIELD;
        goto AbortLine;
      }
    }
    c = *data++;
    if (SpecializedOptions::escaping && ARROW_PREDICT_FALSE(c == options_.escape_char)) {
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
  using BulkFilterType = internal::PreferredBulkFilterType<SpecializedOptions>;
  using BulkWordType = typename BulkFilterType::WordType;

  const char* RunBulkFilter(const char* data, const char* data_end) {
    while (true) {
      if (ARROW_PREDICT_FALSE(static_cast<size_t>(data_end - data) <
                              sizeof(BulkWordType))) {
        if (ARROW_PREDICT_FALSE(data == data_end)) {
          return nullptr;
        }
        return data;
      }
      BulkWordType word;
      memcpy(&word, data, sizeof(BulkWordType));
      if (bulk_filter_.Matches(word)) {
        return data;
      }
      // No special chars
      data += sizeof(BulkWordType);
    }
  }

  const ParseOptions& options_;
  const BulkFilterType bulk_filter_;
  State state_ = FIELD_START;
};

// A BoundaryFinder implementation that assumes CSV cells can contain raw newlines,
// and uses actual CSV lexing to delimit them.
template <typename SpecializedOptions>
class LexingBoundaryFinder : public BoundaryFinder {
 public:
  explicit LexingBoundaryFinder(ParseOptions options)
      : options_(std::move(options)), lexer_(options_) {}

  Status FindFirst(util::string_view partial, util::string_view block,
                   int64_t* out_pos) override {
    lexer_.Reset();
    if (lexer_.ShouldUseBulkFilter(block.data(), block.data() + block.size())) {
      return FindFirstInternal<true>(partial, block, out_pos);
    } else {
      return FindFirstInternal<false>(partial, block, out_pos);
    }
  }

  template <bool UseBulkFilter>
  Status FindFirstInternal(util::string_view partial, util::string_view block,
                           int64_t* out_pos) {
    const char* line_end = lexer_.template ReadLine<UseBulkFilter>(
        partial.data(), partial.data() + partial.size());
    DCHECK_EQ(line_end, nullptr);  // Otherwise `partial` is a whole CSV line
    line_end = lexer_.template ReadLine<UseBulkFilter>(block.data(),
                                                       block.data() + block.size());

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
    lexer_.Reset();
    if (lexer_.ShouldUseBulkFilter(block.data(), block.data() + block.size())) {
      return FindLastInternal<true>(block, out_pos);
    } else {
      return FindLastInternal<false>(block, out_pos);
    }
  }

  template <bool UseBulkFilter>
  Status FindLastInternal(util::string_view block, int64_t* out_pos) {
    const char* data = block.data();
    const char* const data_end = block.data() + block.size();

    while (data < data_end) {
      const char* line_end = lexer_.template ReadLine<UseBulkFilter>(data, data_end);
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
    lexer_.Reset();

    int64_t found = 0;
    const char* data = block.data();
    const char* const data_end = block.data() + block.size();

    const char* line_end;
    if (partial.size()) {
      line_end = lexer_.ReadLine(partial.data(), partial.data() + partial.size());
      DCHECK_EQ(line_end, nullptr);  // Otherwise `partial` is a whole CSV line
    }

    for (; data < data_end && found < count; ++found) {
      line_end = lexer_.ReadLine(data, data_end);
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
  Lexer<SpecializedOptions> lexer_;
};

}  // namespace

std::unique_ptr<Chunker> MakeChunker(const ParseOptions& options) {
  std::shared_ptr<BoundaryFinder> delimiter;
  if (!options.newlines_in_values) {
    delimiter = MakeNewlineBoundaryFinder();
  } else {
    if (options.quoting) {
      if (options.escaping) {
        delimiter = std::make_shared<
            LexingBoundaryFinder<internal::SpecializedOptions<true, true>>>(options);
      } else {
        delimiter = std::make_shared<
            LexingBoundaryFinder<internal::SpecializedOptions<true, false>>>(options);
      }
    } else {
      if (options.escaping) {
        delimiter = std::make_shared<
            LexingBoundaryFinder<internal::SpecializedOptions<false, true>>>(options);
      } else {
        delimiter = std::make_shared<
            LexingBoundaryFinder<internal::SpecializedOptions<false, false>>>(options);
      }
    }
  }
  return ::arrow::internal::make_unique<Chunker>(std::move(delimiter));
}

}  // namespace csv
}  // namespace arrow
