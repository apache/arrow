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
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/string_view.h"

namespace arrow {

using internal::make_unique;
using util::string_view;

namespace json {

namespace rj = arrow::rapidjson;

static size_t ConsumeWhitespace(string_view view) {
#ifdef RAPIDJSON_SIMD
  auto data = view.data();
  auto nonws_begin = rj::SkipWhitespace_SIMD(data, data + view.size());
  return nonws_begin - data;
#else
  auto ws_count = view.find_first_not_of(" \t\r\n");
  if (ws_count == string_view::npos) {
    return view.size();
  } else {
    return ws_count;
  }
#endif
}

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

namespace {

// A BoundaryFinder implementation that assumes JSON objects can contain raw newlines,
// and uses actual JSON parsing to delimit them.
class ParsingBoundaryFinder : public BoundaryFinder {
 public:
  Status FindFirst(string_view partial, string_view block, int64_t* out_pos) override {
    // NOTE: We could bubble up JSON parse errors here, but the actual parsing
    // step will detect them later anyway.
    auto length = ConsumeWholeObject(MultiStringStream({partial, block}));
    if (length == string_view::npos) {
      *out_pos = -1;
    } else {
      DCHECK_GE(length, partial.size());
      DCHECK_LE(length, partial.size() + block.size());
      *out_pos = static_cast<int64_t>(length - partial.size());
    }
    return Status::OK();
  }

  Status FindLast(util::string_view block, int64_t* out_pos) override {
    const size_t block_length = block.size();
    size_t consumed_length = 0;
    while (consumed_length < block_length) {
      rj::MemoryStream ms(reinterpret_cast<const char*>(block.data()), block.size());
      using InputStream = rj::EncodedInputStream<rj::UTF8<>, rj::MemoryStream>;
      auto length = ConsumeWholeObject(InputStream(ms));
      if (length == string_view::npos || length == 0) {
        // found incomplete object or block is empty
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

  Status FindNth(util::string_view partial, util::string_view block, int64_t count,
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
  return std::unique_ptr<Chunker>(new Chunker(std::move(delimiter)));
}

}  // namespace json
}  // namespace arrow
