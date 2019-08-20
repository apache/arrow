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
#include <utility>
#include <vector>

#include "arrow/json/rapidjson_defs.h"
#include "rapidjson/reader.h"

#include "arrow/buffer.h"
#include "arrow/json/options.h"
#include "arrow/util/stl.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace json {

namespace rj = arrow::rapidjson;

using internal::make_unique;
using util::string_view;

static Status StraddlingTooLarge() {
  return Status::Invalid("straddling object straddles two block boundaries");
}

static size_t ConsumeWhitespace(std::shared_ptr<Buffer>* buf) {
#if defined(ARROW_RAPIDJSON_SKIP_WHITESPACE_SIMD)
  auto data = reinterpret_cast<const char*>((*buf)->data());
  auto nonws_begin = rj::SkipWhitespace_SIMD(data, data + (*buf)->size());
  auto ws_count = nonws_begin - data;
  *buf = SliceBuffer(*buf, ws_count);
  return static_cast<size_t>(ws_count);
#else
  auto ws_count = string_view(**buf).find_first_not_of(" \t\r\n");
  if (ws_count == string_view::npos) {
    ws_count = (*buf)->size();
  }
  *buf = SliceBuffer(*buf, ws_count);
  return ws_count;
#endif
}

class NewlinesStrictlyDelimitChunker : public Chunker {
 public:
  Status Process(const std::shared_ptr<Buffer>& block, std::shared_ptr<Buffer>* whole,
                 std::shared_ptr<Buffer>* partial) override {
    auto last_newline = string_view(*block).find_last_of("\n\r");
    if (last_newline == string_view::npos) {
      // no newlines in this block, return empty chunk
      *whole = SliceBuffer(block, 0, 0);
      *partial = block;
    } else {
      *whole = SliceBuffer(block, 0, last_newline + 1);
      *partial = SliceBuffer(block, last_newline + 1);
    }
    return Status::OK();
  }

  Status ProcessWithPartial(const std::shared_ptr<Buffer>& partial_original,
                            const std::shared_ptr<Buffer>& block,
                            std::shared_ptr<Buffer>* completion,
                            std::shared_ptr<Buffer>* rest) override {
    auto partial = partial_original;
    ConsumeWhitespace(&partial);
    if (partial->size() == 0) {
      // if partial is empty, don't bother looking for completion
      *completion = SliceBuffer(block, 0, 0);
      *rest = block;
      return Status::OK();
    }
    auto first_newline = string_view(*block).find_first_of("\n\r");
    if (first_newline == string_view::npos) {
      // no newlines in this block; straddling object straddles *two* block boundaries.
      // retry with larger buffer
      return StraddlingTooLarge();
    }
    *completion = SliceBuffer(block, 0, first_newline + 1);
    *rest = SliceBuffer(block, first_newline + 1);
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
    std::reverse(strings_.begin(), strings_.end());
  }
  explicit MultiStringStream(const BufferVector& buffers) : strings_(buffers.size()) {
    for (size_t i = 0; i < buffers.size(); ++i) {
      strings_[i] = string_view(*buffers[i]);
    }
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
  size_t Tell() { return index_; }
  void Put(char) { ARROW_LOG(FATAL) << "not implemented"; }
  void Flush() { ARROW_LOG(FATAL) << "not implemented"; }
  char* PutBegin() {
    ARROW_LOG(FATAL) << "not implemented";
    return nullptr;
  }
  size_t PutEnd(char*) {
    ARROW_LOG(FATAL) << "not implemented";
    return 0;
  }

 private:
  size_t index_ = 0;
  std::vector<string_view> strings_;
};

template <typename Stream>
static size_t ConsumeWholeObject(Stream&& stream) {
  static constexpr unsigned parse_flags = rj::kParseIterativeFlag |
                                          rj::kParseStopWhenDoneFlag |
                                          rj::kParseNumbersAsStringsFlag;
  rj::BaseReaderHandler<rj::UTF8<>> handler;
  rj::Reader reader;
  // parse a single JSON object
  switch (reader.Parse<parse_flags>(stream, handler).Code()) {
    case rj::kParseErrorNone:
      return stream.Tell();
    case rj::kParseErrorDocumentEmpty:
      return 0;
    default:
      // rapidjson emitted an error, the most recent object was partial
      return string_view::npos;
  }
}

class ParsingChunker : public Chunker {
 public:
  Status Process(const std::shared_ptr<Buffer>& block, std::shared_ptr<Buffer>* whole,
                 std::shared_ptr<Buffer>* partial) override {
    if (block->size() == 0) {
      *whole = SliceBuffer(block, 0, 0);
      *partial = block;
      return Status::OK();
    }
    size_t total_length = 0;
    for (auto consumed = block;; consumed = SliceBuffer(block, total_length)) {
      rj::MemoryStream ms(reinterpret_cast<const char*>(consumed->data()),
                          consumed->size());
      using InputStream = rj::EncodedInputStream<rj::UTF8<>, rj::MemoryStream>;
      auto length = ConsumeWholeObject(InputStream(ms));
      if (length == string_view::npos || length == 0) {
        // found incomplete object or consumed is empty
        break;
      }
      if (static_cast<int64_t>(length) > consumed->size()) {
        total_length += consumed->size();
        break;
      }
      total_length += length;
    }
    *whole = SliceBuffer(block, 0, total_length);
    *partial = SliceBuffer(block, total_length);
    return Status::OK();
  }

  Status ProcessWithPartial(const std::shared_ptr<Buffer>& partial_original,
                            const std::shared_ptr<Buffer>& block,
                            std::shared_ptr<Buffer>* completion,
                            std::shared_ptr<Buffer>* rest) override {
    auto partial = partial_original;
    ConsumeWhitespace(&partial);
    if (partial->size() == 0) {
      // if partial is empty, don't bother looking for completion
      *completion = SliceBuffer(block, 0, 0);
      *rest = block;
      return Status::OK();
    }
    auto length = ConsumeWholeObject(MultiStringStream({partial, block}));
    if (length == string_view::npos) {
      // straddling object straddles *two* block boundaries.
      // retry with larger buffer
      return StraddlingTooLarge();
    }
    auto completion_length = length - partial->size();
    *completion = SliceBuffer(block, 0, completion_length);
    *rest = SliceBuffer(block, completion_length);
    return Status::OK();
  }
};

std::unique_ptr<Chunker> Chunker::Make(const ParseOptions& options) {
  if (!options.newlines_in_values) {
    return make_unique<NewlinesStrictlyDelimitChunker>();
  }
  return make_unique<ParsingChunker>();
}

}  // namespace json
}  // namespace arrow
