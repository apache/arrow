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

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/csv/column_decoder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::checked_cast;
using internal::GetCpuThreadPool;

using ChunkData = std::vector<std::vector<std::string>>;

class ThreadJoiner {
 public:
  explicit ThreadJoiner(std::shared_ptr<std::thread> thread)
      : thread_(std::move(thread)) {}

  ~ThreadJoiner() {
    if (thread_->joinable()) {
      thread_->join();
    }
  }

 protected:
  std::shared_ptr<std::thread> thread_;
};

template <typename Func>
ThreadJoiner RunThread(Func&& func) {
  return ThreadJoiner(std::make_shared<std::thread>(std::forward<Func>(func)));
}

template <typename Func>
void RunThreadsAndJoin(Func&& func, int iters) {
  std::vector<ThreadJoiner> threads;
  for (int i = 0; i < iters; i++) {
    threads.emplace_back(std::make_shared<std::thread>([i, func] { func(i); }));
  }
}

class ColumnDecoderTest : public ::testing::Test {
 public:
  ColumnDecoderTest() : num_chunks_(0), read_ptr_(0) {}

  void SetDecoder(std::shared_ptr<ColumnDecoder> decoder) {
    decoder_ = std::move(decoder);
    decoded_chunks_.clear();
    num_chunks_ = 0;
    read_ptr_ = 0;
  }

  void InsertChunk(std::vector<std::string> chunk) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    auto decoded = decoder_->Decode(parser);
    decoded_chunks_.push_back(decoded);
    ++num_chunks_;
  }

  void AppendChunks(const ChunkData& chunks) {
    for (const auto& chunk : chunks) {
      InsertChunk(chunk);
    }
  }

  Result<std::shared_ptr<Array>> NextChunk() {
    EXPECT_LT(read_ptr_, static_cast<int64_t>(decoded_chunks_.size()));
    return decoded_chunks_[read_ptr_++].result();
  }

  void AssertChunk(std::vector<std::string> chunk, std::shared_ptr<Array> expected) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    ASSERT_FINISHES_OK_AND_ASSIGN(auto decoded, decoder_->Decode(parser));
    AssertArraysEqual(*expected, *decoded);
  }

  void AssertChunkInvalid(std::vector<std::string> chunk) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    ASSERT_FINISHES_AND_RAISES(Invalid, decoder_->Decode(parser));
  }

  void AssertFetch(std::shared_ptr<Array> expected_chunk) {
    ASSERT_OK_AND_ASSIGN(auto chunk, NextChunk());
    ASSERT_NE(chunk, nullptr);
    AssertArraysEqual(*expected_chunk, *chunk);
  }

  void AssertFetchInvalid() { ASSERT_RAISES(Invalid, NextChunk()); }

 protected:
  std::shared_ptr<ColumnDecoder> decoder_;
  std::vector<Future<std::shared_ptr<Array>>> decoded_chunks_;
  int64_t num_chunks_ = 0;
  int64_t read_ptr_ = 0;

  ConvertOptions default_options = ConvertOptions::Defaults();
};

//////////////////////////////////////////////////////////////////////////
// Tests for null column decoder

class NullColumnDecoderTest : public ColumnDecoderTest {
 public:
  NullColumnDecoderTest() {}

  void MakeDecoder(std::shared_ptr<DataType> type) {
    ASSERT_OK_AND_ASSIGN(auto decoder,
                         ColumnDecoder::MakeNull(default_memory_pool(), type));
    SetDecoder(decoder);
  }

  void TestNullType() {
    auto type = null();

    MakeDecoder(type);

    AppendChunks({{"1", "2", "3"}, {"4", "5"}});
    AssertFetch(ArrayFromJSON(type, "[null, null, null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));

    MakeDecoder(type);

    AppendChunks({{}, {"6"}});
    AssertFetch(ArrayFromJSON(type, "[]"));
    AppendChunks({{"7", "8"}});
    AssertFetch(ArrayFromJSON(type, "[null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
  }

  void TestOtherType() {
    auto type = int32();

    MakeDecoder(type);

    AppendChunks({{"1", "2", "3"}, {"4", "5"}});
    AssertFetch(ArrayFromJSON(type, "[null, null, null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
  }

  void TestThreaded() {
    constexpr int NITERS = 10;
    auto type = int32();
    MakeDecoder(type);

    RunThreadsAndJoin(
        [&](int thread_id) {
          AssertChunk({"4", "5", std::to_string(thread_id)},
                      ArrayFromJSON(type, "[null, null, null]"));
        },
        NITERS);
  }
};

TEST_F(NullColumnDecoderTest, NullType) { this->TestNullType(); }

TEST_F(NullColumnDecoderTest, OtherType) { this->TestOtherType(); }

TEST_F(NullColumnDecoderTest, Threaded) { this->TestThreaded(); }

//////////////////////////////////////////////////////////////////////////
// Tests for fixed-type column decoder

class TypedColumnDecoderTest : public ColumnDecoderTest {
 public:
  TypedColumnDecoderTest() {}

  void MakeDecoder(const std::shared_ptr<DataType>& type, const ConvertOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto decoder,
                         ColumnDecoder::Make(default_memory_pool(), type, 0, options));
    SetDecoder(decoder);
  }

  void TestIntegers() {
    auto type = int16();

    MakeDecoder(type, default_options);

    AppendChunks({{"123", "456", "-78"}, {"901", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetch(ArrayFromJSON(type, "[901, null]"));

    MakeDecoder(type, default_options);

    AppendChunks({{}, {"-987"}});
    AssertFetch(ArrayFromJSON(type, "[]"));
    AppendChunks({{"N/A", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[-987]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
  }

  void TestOptions() {
    auto type = boolean();

    MakeDecoder(type, default_options);

    AppendChunks({{"true", "false", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[true, false, null]"));

    // With non-default options
    auto options = default_options;
    options.null_values = {"true"};
    options.true_values = {"false"};
    options.false_values = {"N/A"};
    MakeDecoder(type, options);

    AppendChunks({{"true", "false", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[null, true, false]"));
  }

  void TestErrors() {
    auto type = uint64();

    MakeDecoder(type, default_options);

    AppendChunks({{"123", "456", "N/A"}, {"-901"}});
    AppendChunks({{"N/A", "1000"}});
    AssertFetch(ArrayFromJSON(type, "[123, 456, null]"));
    AssertFetchInvalid();
    AssertFetch(ArrayFromJSON(type, "[null, 1000]"));
  }

  void TestThreaded() {
    constexpr int NITERS = 10;
    auto type = uint32();
    MakeDecoder(type, default_options);

    RunThreadsAndJoin(
        [&](int thread_id) {
          if (thread_id % 2 == 0) {
            AssertChunkInvalid({"4", "-5"});
          } else {
            AssertChunk({"1", "2", "3"}, ArrayFromJSON(type, "[1, 2, 3]"));
          }
        },
        NITERS);
  }
};

TEST_F(TypedColumnDecoderTest, Integers) { this->TestIntegers(); }

TEST_F(TypedColumnDecoderTest, Options) { this->TestOptions(); }

TEST_F(TypedColumnDecoderTest, Errors) { this->TestErrors(); }

TEST_F(TypedColumnDecoderTest, Threaded) { this->TestThreaded(); }

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring column decoder

class InferringColumnDecoderTest : public ColumnDecoderTest {
 public:
  InferringColumnDecoderTest() {}

  void MakeDecoder(const ConvertOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto decoder,
                         ColumnDecoder::Make(default_memory_pool(), 0, options));
    SetDecoder(decoder);
  }

  void TestIntegers() {
    auto type = int64();

    MakeDecoder(default_options);

    AppendChunks({{"123", "456", "-78"}, {"901", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetch(ArrayFromJSON(type, "[901, null]"));
  }

  void TestThreaded() {
    constexpr int NITERS = 10;
    auto type = float64();
    MakeDecoder(default_options);

    // One of these will do the inference so we need to make sure they all have floating
    // point
    RunThreadsAndJoin(
        [&](int thread_id) {
          if (thread_id % 2 == 0) {
            AssertChunk({"6.3", "7.2"}, ArrayFromJSON(type, "[6.3, 7.2]"));
          } else {
            AssertChunk({"1.1", "2", "3"}, ArrayFromJSON(type, "[1.1, 2, 3]"));
          }
        },
        NITERS);

    // These will run after the inference
    RunThreadsAndJoin(
        [&](int thread_id) {
          if (thread_id % 2 == 0) {
            AssertChunk({"1", "2"}, ArrayFromJSON(type, "[1, 2]"));
          } else {
            AssertChunkInvalid({"xyz"});
          }
        },
        NITERS);
  }

  void TestOptions() {
    auto type = boolean();

    auto options = default_options;
    options.null_values = {"true"};
    options.true_values = {"false"};
    options.false_values = {"N/A"};
    MakeDecoder(options);

    AppendChunks({{"true", "false", "N/A"}, {"true"}});
    AssertFetch(ArrayFromJSON(type, "[null, true, false]"));
    AssertFetch(ArrayFromJSON(type, "[null]"));
  }

  void TestErrors() {
    auto type = int64();

    MakeDecoder(default_options);

    AppendChunks({{"123", "456", "-78"}, {"9.5", "N/A"}});
    AppendChunks({{"1000", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetchInvalid();
    AssertFetch(ArrayFromJSON(type, "[1000, null]"));
  }

  void TestEmpty() {
    auto type = null();

    MakeDecoder(default_options);

    AppendChunks({{}, {}});
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetch(ArrayFromJSON(type, "[]"));
  }
};

TEST_F(InferringColumnDecoderTest, Integers) { this->TestIntegers(); }

TEST_F(InferringColumnDecoderTest, Threaded) { this->TestThreaded(); }

TEST_F(InferringColumnDecoderTest, Options) { this->TestOptions(); }

TEST_F(InferringColumnDecoderTest, Errors) { this->TestErrors(); }

TEST_F(InferringColumnDecoderTest, Empty) { this->TestEmpty(); }

// More inference tests are in InferringColumnBuilderTest

}  // namespace csv
}  // namespace arrow
