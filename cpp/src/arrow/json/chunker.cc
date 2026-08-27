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

static size_t ConsumeWhitespace(std::string_view view) {
  const auto ws_count = view.find_first_not_of(" \t\r\n");
  if (ws_count == std::string_view::npos) {
    return view.size();
  }
  return ws_count;
}

static size_t ConsumeWholeObject(std::string_view input) {
  if (input.empty()) {
    return 0;
  }

  const size_t start = ConsumeWhitespace(input);
  if (start >= input.size()) {
    return 0;
  }

  // Keep the padded buffer alive while iterating the document stream.
  simdjson::padded_string padded(input);
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document_stream stream;

  auto stream_status = internal::ResolveSimdjsonResult(
      parser.iterate_many(padded), "Failed to create JSON document stream");
  if (!stream_status.ok()) {
    return std::string_view::npos;
  }

  stream = std::move(stream_status).ValueUnsafe();

  auto it = stream.begin();
  if (it == stream.end()) {
    return 0;
  }

  // Force parsing of the first document.
  auto document = *it;
  if (it.error() != simdjson::SUCCESS) {
    return std::string_view::npos;
  }

  auto value_status =
      internal::ResolveSimdjsonResult(document.get_value(), "Failed to get JSON value");
  if (!value_status.ok()) {
    return std::string_view::npos;
  }

  auto value = std::move(value_status).ValueUnsafe();
  auto consume_status = internal::ConsumeJsonValue(value);
  if (!consume_status.ok()) {
    return std::string_view::npos;
  }

  // current_index() is the start of this document. source() is the
  // complete source span of the current document.
  const size_t document_start = it.current_index();
  const size_t document_length = it.source().size();

  return document_start + document_length;
}

namespace {

// A BoundaryFinder implementation that assumes JSON objects can contain raw newlines,
// and uses actual JSON parsing to delimit them.
class ParsingBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(std::string_view partial, std::string_view block,
                   int64_t* out_pos) override {
    std::string combined;
    combined.reserve(partial.size() + block.size());
    combined.append(partial);
    combined.append(block);

    const size_t start = ConsumeWhitespace(combined);
    if (start < combined.size() && combined[start] != '{' && combined[start] != '[') {
      return Status::Invalid("JSON chunk error: invalid data at end of document");
    }

    const auto length = ConsumeWholeObject(combined);

    if (length == std::string_view::npos) {
      *out_pos = -1;
    } else if (ARROW_PREDICT_FALSE(length < partial.size())) {
      return Status::Invalid("JSON chunk error: invalid data at end of document");
    } else {
      DCHECK_LE(length, partial.size() + block.size());
      *out_pos = static_cast<int64_t>(length - partial.size());
    }

    return Status::OK();
  }

  Status FindLast(std::string_view block, int64_t* out_pos) override {
    const size_t block_length = block.size();
    size_t consumed_length = 0;

    if (block_length > 0) {
      const size_t start = ConsumeWhitespace(block);
      if (start < block.size() && block[start] != '{' && block[start] != '[') {
        return Status::Invalid("JSON parse error: Invalid value");
      }
    }

    while (consumed_length < block_length) {
      const auto length = ConsumeWholeObject(block);

      if (length == std::string_view::npos || length == 0) {
        const size_t start = ConsumeWhitespace(block);

        if (start < block.size()) {
          const char first_char = block[start];

          if (first_char != '{' && first_char != '[') {
            const size_t remaining_len = block.size() - start;

            if (remaining_len > 1 || (first_char != '}' && first_char != ']')) {
              return Status::Invalid("JSON parse error: Invalid value");
            }
          }
        }

        break;
      }

      consumed_length += length;
      block = block.substr(length);
    }

    if (consumed_length == 0) {
      *out_pos = -1;
    } else {
      consumed_length += ConsumeWhitespace(block);
      DCHECK_LE(consumed_length, block_length);
      *out_pos = static_cast<int64_t>(consumed_length);
    }

    return Status::OK();
  }

  Status FindNth(std::string_view partial, std::string_view block, int64_t count,
                 int64_t* out_pos, int64_t* num_found) override {
    return Status::NotImplemented("ParsingBoundaryFinder::FindNth");
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
