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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#if defined(ARROW_HAVE_SSE4_2)
#define RAPIDJSON_SSE42 1
#define ARROW_RAPIDJSON_SKIP_WHITESPACE_SIMD 1
#endif
#if defined(ARROW_HAVE_SSE2)
#define RAPIDJSON_SSE2 1
#define ARROW_RAPIDJSON_SKIP_WHITESPACE_SIMD 1
#endif
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>

#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/stl.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace json {

using internal::make_unique;
using util::string_view;

Status StraddlingTooLarge() {
  return Status::Invalid("straddling object straddles two block boundaries");
}

std::size_t ConsumeWhitespace(string_view* str) {
#if defined(ARROW_RAPIDJSON_SKIP_WHITESPACE_SIMD)
  auto nonws_begin =
      rapidjson::SkipWhitespace_SIMD(str->data(), str->data() + str->size());
  auto ws_count = nonws_begin - str->data();
  *str = str->substr(ws_count);
  return static_cast<std::size_t>(ws_count);
#undef ARROW_RAPIDJSON_SKIP_WHITESPACE_SIMD
#else
  auto ws_count = str->find_first_not_of(" \t\r\n");
  *str = str->substr(ws_count);
  return ws_count;
#endif
}

class NewlinesStrictlyDelimitChunker : public Chunker {
 public:
  Status Process(string_view block, string_view* chunked) override {
    auto last_newline = block.find_last_of("\n\r");
    if (last_newline == string_view::npos) {
      // no newlines in this block, return empty chunk
      *chunked = string_view();
    } else {
      *chunked = block.substr(0, last_newline + 1);
    }
    return Status::OK();
  }

  Status Process(string_view partial, string_view block,
                 string_view* completion) override {
    ConsumeWhitespace(&partial);
    if (partial.size() == 0) {
      // if partial is empty, don't bother looking for completion
      *completion = string_view();
      return Status::OK();
    }
    auto first_newline = block.find_first_of("\n\r");
    if (first_newline == string_view::npos) {
      // no newlines in this block; straddling object straddles *two* block boundaries.
      // retry with larger buffer
      return StraddlingTooLarge();
    }
    *completion = block.substr(0, first_newline + 1);
    return Status::OK();
  }
};

/// RapidJson custom stream for reading JSON stored in multiple buffers
/// http://rapidjson.org/md_doc_stream.html#CustomStream
class MultiStringStream {
 public:
  using Ch = char;
  explicit MultiStringStream(std::vector<string_view> strings)
      : strings_(std::move(strings)) {
    std::remove(strings_.begin(), strings_.end(), string_view(""));
    std::reverse(strings_.begin(), strings_.end());
  }
  char Peek() const {
    if (strings_.size() == 0) return '\0';
    return strings_.back()[0];
  }
  char Take() {
    if (strings_.size() == 0) return '\0';
    char taken = strings_.back()[0];
    if (strings_.back().size() == 1) {
      strings_.pop_back();
    } else {
      strings_.back() = strings_.back().substr(1);
    }
    ++index_;
    return taken;
  }
  std::size_t Tell() { return index_; }
  void Put(char) { ARROW_LOG(FATAL) << "not implemented"; }
  void Flush() { ARROW_LOG(FATAL) << "not implemented"; }
  char* PutBegin() {
    ARROW_LOG(FATAL) << "not implemented";
    return nullptr;
  }
  std::size_t PutEnd(char*) {
    ARROW_LOG(FATAL) << "not implemented";
    return 0;
  }

 private:
  std::size_t index_ = 0;
  std::vector<string_view> strings_;
};

template <typename Stream>
std::size_t ConsumeWholeObject(Stream&& stream) {
  static constexpr unsigned parse_flags = rapidjson::kParseIterativeFlag |
                                          rapidjson::kParseStopWhenDoneFlag |
                                          rapidjson::kParseNumbersAsStringsFlag;
  rapidjson::BaseReaderHandler<rapidjson::UTF8<>> handler;
  rapidjson::Reader reader;
  // parse a single JSON object
  switch (reader.Parse<parse_flags>(stream, handler).Code()) {
    case rapidjson::kParseErrorNone:
      return stream.Tell();
    case rapidjson::kParseErrorDocumentEmpty:
      return 0;
    default:
      // rapidjson emitted an error, the most recent object was partial
      return string_view::npos;
  }
}

class ParsingChunker : public Chunker {
 public:
  Status Process(string_view block, string_view* chunked) override {
    if (block.size() == 0) {
      *chunked = string_view();
      return Status::OK();
    }
    std::size_t total_length = 0;
    for (auto consumed = block;; consumed = block.substr(total_length)) {
      using rapidjson::MemoryStream;
      MemoryStream ms(consumed.data(), consumed.size());
      using InputStream = rapidjson::EncodedInputStream<rapidjson::UTF8<>, MemoryStream>;
      auto length = ConsumeWholeObject(InputStream(ms));
      if (length == string_view::npos || length == 0) {
        // found incomplete object or consumed is empty
        break;
      }
      if (length > consumed.size()) {
        total_length += consumed.size();
        break;
      }
      total_length += length;
    }
    *chunked = block.substr(0, total_length);
    return Status::OK();
  }

  Status Process(string_view partial, string_view block,
                 string_view* completion) override {
    ConsumeWhitespace(&partial);
    if (partial.size() == 0) {
      // if partial is empty, don't bother looking for completion
      *completion = string_view();
      return Status::OK();
    }
    auto length = ConsumeWholeObject(MultiStringStream({partial, block}));
    if (length == string_view::npos) {
      // straddling object straddles *two* block boundaries.
      // retry with larger buffer
      return StraddlingTooLarge();
    }
    *completion = block.substr(0, length - partial.size());
    return Status::OK();
  }
};

std::unique_ptr<Chunker> Chunker::Make(ParseOptions options) {
  if (!options.newlines_in_values) {
    return make_unique<NewlinesStrictlyDelimitChunker>();
  }
  return make_unique<ParsingChunker>();
}

}  // namespace json
}  // namespace arrow
