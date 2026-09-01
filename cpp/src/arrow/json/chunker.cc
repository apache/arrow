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

static Status ConsumeDocument(simdjson::ondemand::document_stream::iterator& it) {
  ARROW_ASSIGN_OR_RAISE(
      auto document, internal::ResolveSimdjsonResult(*it, "Failed to get JSON document"));

  ARROW_ASSIGN_OR_RAISE(
      auto value,
      internal::ResolveSimdjsonResult(document.get_value(), "Failed to get JSON value"));

  return internal::ConsumeJsonValue(value);
}

static size_t ConsumeWholeObject(const simdjson::padded_string& input) {
  if (input.size() == 0) {
    return 0;
  }

  simdjson::ondemand::parser parser;
  simdjson::ondemand::document_stream stream;

  if (parser.iterate_many(input).get(stream) != simdjson::SUCCESS) {
    return std::string_view::npos;
  }

  auto it = stream.begin();
  if (it == stream.end()) {
    return 0;
  }

  // Force parsing of the first document.
  if (!ConsumeDocument(it).ok()) {
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
    simdjson::padded_string input;

    if (partial.empty()) {
      input = simdjson::padded_string(block);
    } else if (block.empty()) {
      input = simdjson::padded_string(partial);
    } else {
      simdjson::padded_string_builder builder(partial.size() + block.size());
      builder.append(partial);
      builder.append(block);
      input = builder.convert();
    }

    const std::string_view input_view(input.data(), input.size());
    const size_t start = ConsumeWhitespace(input_view);
    if (start < input_view.size() && input_view[start] != '{' &&
        input_view[start] != '[') {
      return Status::Invalid("JSON chunk error: invalid data at end of document");
    }

    const auto length = ConsumeWholeObject(input);

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
    size_t consumed_length = 0;

    simdjson::padded_string padded(block);
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document_stream stream;

    if (parser.iterate_many(padded).get(stream) != simdjson::SUCCESS) {
      *out_pos = -1;
      return Status::OK();
    }

    auto it = stream.begin();
    if (it == stream.end()) {
      *out_pos = -1;
      return Status::OK();
    }

    while (it != stream.end()) {
      if (!ConsumeDocument(it).ok()) {
        break;
      }
      consumed_length = it.current_index() + it.source().size();
      ++it;
    }

    if (consumed_length == 0) {
      *out_pos = -1;
    } else {
      consumed_length += ConsumeWhitespace(block.substr(consumed_length));
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
