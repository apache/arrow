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

#include "arrow/json/chunker.h"

#include <string_view>
#include <utility>

#include <simdjson.h>

#include "arrow/buffer.h"
#include "arrow/json/options.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/simdjson_internal.h"

namespace arrow {
namespace json {
namespace {

// XXX We could try to SIMD-accelerate this routine but it's called only
// once per chunk and also will presumably examine a minimal amount of bytes.
int64_t ConsumeWhitespace(std::string_view view) {
  const auto ws_count = view.find_first_not_of(" \t\r\n");
  if (ws_count == std::string_view::npos) {
    return view.size();
  }
  return static_cast<int64_t>(ws_count);
}

Status ConsumeDocument(simdjson::ondemand::document_stream::iterator& it) {
  ARROW_ASSIGN_OR_RAISE(
      auto document, internal::ResolveSimdjsonResult(*it, "Failed to get JSON document"));
  ARROW_ASSIGN_OR_RAISE(
      auto value,
      internal::ResolveSimdjsonResult(document.get_value(), "Failed to get JSON value"));
  return internal::ConsumeJsonValue(value);
}

// A BoundaryFinder implementation that assumes JSON objects can contain raw newlines,
// and uses actual JSON parsing to delimit them.
class ParsingBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(std::string_view partial, std::string_view block,
                   int64_t* out_pos) override {
    auto input = GetPaddedStringView(partial, block);
    ARROW_ASSIGN_OR_RAISE(auto consumed_length,
                          ConsumeWholeObject(input, /*until_end=*/false));

    DCHECK_NE(consumed_length, std::string_view::npos);
    if (consumed_length == 0) {
      *out_pos = kNoDelimiterFound;
    } else if (ARROW_PREDICT_FALSE(consumed_length <= partial.size())) {
      // Something bad happened: partial wasn't supposed to be a full document
      return Status::Invalid("JSON parse error: invalid data at end of document");
    } else {
      consumed_length -= partial.size();
      DCHECK_LE(consumed_length, block.size());
      *out_pos = static_cast<int64_t>(consumed_length);
    }
    return Status::OK();
  }

  Status FindLast(std::string_view block, int64_t* out_pos) override {
    auto input = GetPaddedStringView(block);
    ARROW_ASSIGN_OR_RAISE(auto consumed_length,
                          ConsumeWholeObject(input, /*until_end=*/true));

    if (consumed_length == 0) {
      *out_pos = kNoDelimiterFound;
    } else {
      *out_pos = static_cast<int64_t>(consumed_length);
    }
    return Status::OK();
  }

  Status FindNth(std::string_view partial, std::string_view block, int64_t count,
                 int64_t* out_pos, int64_t* num_found) override {
    return Status::NotImplemented("ParsingBoundaryFinder::FindNth");
  }

 private:
  simdjson::ondemand::parser parser_;
  // A persistent buffer to keep padded contents for simdjson.
  // This should be more efficient than allocating a new padded_string everytime.
  std::string buffer_;

  simdjson::padded_string_view GetPaddedStringView(std::string_view partial,
                                                   std::string_view block = {}) {
    // Adjust buffer size without copying old contents.
    buffer_.clear();
    buffer_.reserve(partial.size() + block.size() + simdjson::SIMDJSON_PADDING);
    buffer_.append(partial);
    buffer_.append(block);
    // XXX Hopefully this upholds for all std::string implementations
    DCHECK_GE(buffer_.capacity() - buffer_.size(), simdjson::SIMDJSON_PADDING);
    auto view = simdjson::padded_string_view(buffer_);
    DCHECK(view.has_sufficient_padding());
    return view;
  }

  // Consume the first or last JSON object (depending on `until_end`)
  // and return the consumed JSON byte length, or 0 if no valid document
  // can be parsed.
  Result<size_t> ConsumeWholeObject(simdjson::padded_string_view input, bool until_end) {
    simdjson::ondemand::document_stream stream;
    // XXX Should be pass a specific batch_size?
    // The default value used by simdjson is 1MB, probably enough for most purposes.
    RETURN_NOT_OK(ToStatus(parser_.iterate_many(input).get(stream)));
    auto it = stream.begin();
    if (it == stream.end()) {
      // Empty input (only whitespace?)
      return 0;
    }

    int64_t consumed_length = 0;
    if (!until_end) {
      // Parsing the first document only.
      if (!ConsumeDocument(it).ok()) {
        // Could be either a partial document or invalid JSON, we'll let
        // followup chunker or parser calls decide.
        return 0;
      }
      // current_index() is the start of the current document;
      // source() is the complete source span of the current document.
      consumed_length = it.current_index() + it.source().size();
    } else {
      while (it != stream.end()) {
        if (!ConsumeDocument(it).ok()) {
          // Could be either a partial document or invalid JSON, we'll let
          // followup chunker or parser calls decide.
          break;
        }
        consumed_length = it.current_index() + it.source().size();
        ++it;
      }
    }
    if (consumed_length > 0) {
      // If we found at least one document, also consume its trailing whitespace
      // to avoid stray bytes at the end of the stream.
      consumed_length += ConsumeWhitespace(input.substr(consumed_length));
    }
    return consumed_length;
  }

  static Status ToStatus(simdjson::error_code error,
                         std::string_view error_prefix = "JSON parse error: ") {
    if (error == simdjson::SUCCESS) {
      return Status::OK();
    }
    return Status::Invalid(std::string(error_prefix) + simdjson::error_message(error));
  }
};

}  // namespace

std::unique_ptr<Chunker> MakeChunker(const ParseOptions& options) {
  std::shared_ptr<BoundaryFinder> delimiter;
  if (options.newlines_in_values) {
    delimiter = std::make_shared<ParsingBoundaryFinder>();
  } else {
    delimiter = MakeNewlineBoundaryFinder();
  }
  return std::make_unique<Chunker>(std::move(delimiter));
}

}  // namespace json
}  // namespace arrow
