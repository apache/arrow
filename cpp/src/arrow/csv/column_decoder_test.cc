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

#include "arrow/csv/column_decoder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;

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

struct SerialExecutor {
  static std::shared_ptr<TaskGroup> task_group() { return TaskGroup::MakeSerial(); }
};

struct ParallelExecutor {
  static std::shared_ptr<TaskGroup> task_group() {
    return TaskGroup::MakeThreaded(GetCpuThreadPool());
  }
};

using ExecutorTypes = ::testing::Types<SerialExecutor, ParallelExecutor>;

class ColumnDecoderTest : public ::testing::Test {
 public:
  ColumnDecoderTest() : tg_(TaskGroup::MakeSerial()), num_chunks_(0) {}

  void SetDecoder(std::shared_ptr<ColumnDecoder> decoder) {
    decoder_ = std::move(decoder);
    num_chunks_ = 0;
  }

  void InsertChunk(int64_t num_chunk, std::vector<std::string> chunk) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    decoder_->Insert(num_chunk, parser);
  }

  void AppendChunks(const ChunkData& chunks) {
    for (const auto& chunk : chunks) {
      std::shared_ptr<BlockParser> parser;
      MakeColumnParser(chunk, &parser);
      decoder_->Append(parser);
      ++num_chunks_;
    }
  }

  void SetEOF() { decoder_->SetEOF(num_chunks_); }

  void AssertFetch(std::shared_ptr<Array> expected_chunk) {
    ASSERT_OK_AND_ASSIGN(auto chunk, decoder_->NextChunk());
    ASSERT_NE(chunk, nullptr);
    AssertArraysEqual(*expected_chunk, *chunk);
  }

  void AssertFetchInvalid() { ASSERT_RAISES(Invalid, decoder_->NextChunk()); }

  void AssertFetchEOF() { ASSERT_OK_AND_EQ(nullptr, decoder_->NextChunk()); }

 protected:
  std::shared_ptr<TaskGroup> tg_;
  std::shared_ptr<ColumnDecoder> decoder_;
  int64_t num_chunks_;

  ConvertOptions default_options = ConvertOptions::Defaults();
};

//////////////////////////////////////////////////////////////////////////
// Tests for null column decoder

template <typename ExecutorType>
class NullColumnDecoderTest : public ColumnDecoderTest {
 public:
  NullColumnDecoderTest() { tg_ = ExecutorType::task_group(); }

  void MakeDecoder(std::shared_ptr<DataType> type) {
    ASSERT_OK_AND_ASSIGN(auto decoder,
                         ColumnDecoder::MakeNull(default_memory_pool(), type, tg_));
    SetDecoder(decoder);
  }

  void TestNullType() {
    auto type = null();

    MakeDecoder(type);

    AppendChunks({{"1", "2", "3"}, {"4", "5"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[null, null, null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
    AssertFetchEOF();

    MakeDecoder(type);

    AppendChunks({{}, {"6"}});
    AssertFetch(ArrayFromJSON(type, "[]"));
    AppendChunks({{"7", "8"}});
    AssertFetch(ArrayFromJSON(type, "[null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
    SetEOF();
    AssertFetchEOF();
  }

  void TestOtherType() {
    auto type = int32();

    MakeDecoder(type);

    AppendChunks({{"1", "2", "3"}, {"4", "5"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[null, null, null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestThreaded() {
    auto type = int32();

    MakeDecoder(type);

    auto joiner = RunThread([&]() {
      InsertChunk(1, {"4", "5"});
      InsertChunk(0, {"1", "2", "3"});
      InsertChunk(3, {"6"});
      InsertChunk(2, {});
      decoder_->SetEOF(4);
    });

    AssertFetch(ArrayFromJSON(type, "[null, null, null]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetch(ArrayFromJSON(type, "[null]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

 protected:
  ExecutorType executor_;
};

TYPED_TEST_SUITE(NullColumnDecoderTest, ExecutorTypes);

TYPED_TEST(NullColumnDecoderTest, NullType) { this->TestNullType(); }

TYPED_TEST(NullColumnDecoderTest, OtherType) { this->TestOtherType(); }

TYPED_TEST(NullColumnDecoderTest, Threaded) { this->TestThreaded(); }

//////////////////////////////////////////////////////////////////////////
// Tests for fixed-type column decoder

template <typename ExecutorType>
class TypedColumnDecoderTest : public ColumnDecoderTest {
 public:
  TypedColumnDecoderTest() { tg_ = ExecutorType::task_group(); }

  void MakeDecoder(const std::shared_ptr<DataType>& type, const ConvertOptions& options) {
    ASSERT_OK_AND_ASSIGN(
        auto decoder, ColumnDecoder::Make(default_memory_pool(), type, 0, options, tg_));
    SetDecoder(decoder);
  }

  void TestIntegers() {
    auto type = int16();

    MakeDecoder(type, default_options);

    AppendChunks({{"123", "456", "-78"}, {"901", "N/A"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetch(ArrayFromJSON(type, "[901, null]"));
    AssertFetchEOF();
    AssertFetchEOF();

    MakeDecoder(type, default_options);

    AppendChunks({{}, {"-987"}});
    AssertFetch(ArrayFromJSON(type, "[]"));
    AppendChunks({{"N/A", "N/A"}});
    AssertFetch(ArrayFromJSON(type, "[-987]"));
    AssertFetch(ArrayFromJSON(type, "[null, null]"));
    SetEOF();
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestOptions() {
    auto type = boolean();

    MakeDecoder(type, default_options);

    AppendChunks({{"true", "false", "N/A"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[true, false, null]"));
    AssertFetchEOF();
    AssertFetchEOF();

    // With non-default options
    auto options = default_options;
    options.null_values = {"true"};
    options.true_values = {"false"};
    options.false_values = {"N/A"};
    MakeDecoder(type, options);

    AppendChunks({{"true", "false", "N/A"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[null, true, false]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestErrors() {
    auto type = uint64();

    MakeDecoder(type, default_options);

    AppendChunks({{"123", "456", "N/A"}, {"-901"}});
    AppendChunks({{"N/A", "1000"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[123, 456, null]"));
    AssertFetchInvalid();
    AssertFetch(ArrayFromJSON(type, "[null, 1000]"));
    AssertFetchEOF();
  }

  void TestThreaded() {
    auto type = uint32();

    MakeDecoder(type, default_options);

    auto joiner = RunThread([&]() {
      InsertChunk(1, {"4", "-5"});
      InsertChunk(0, {"1", "2", "3"});
      InsertChunk(3, {"6"});
      InsertChunk(2, {});
      decoder_->SetEOF(4);
    });

    AssertFetch(ArrayFromJSON(type, "[1, 2, 3]"));
    AssertFetchInvalid();
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetch(ArrayFromJSON(type, "[6]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }
};

TYPED_TEST_SUITE(TypedColumnDecoderTest, ExecutorTypes);

TYPED_TEST(TypedColumnDecoderTest, Integers) { this->TestIntegers(); }

TYPED_TEST(TypedColumnDecoderTest, Options) { this->TestOptions(); }

TYPED_TEST(TypedColumnDecoderTest, Errors) { this->TestErrors(); }

TYPED_TEST(TypedColumnDecoderTest, Threaded) { this->TestThreaded(); }

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring column decoder

template <typename ExecutorType>
class InferringColumnDecoderTest : public ColumnDecoderTest {
 public:
  InferringColumnDecoderTest() { tg_ = ExecutorType::task_group(); }

  void MakeDecoder(const ConvertOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto decoder,
                         ColumnDecoder::Make(default_memory_pool(), 0, options, tg_));
    SetDecoder(decoder);
  }

  void TestIntegers() {
    auto type = int64();

    MakeDecoder(default_options);

    AppendChunks({{"123", "456", "-78"}, {"901", "N/A"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetch(ArrayFromJSON(type, "[901, null]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestThreaded() {
    auto type = float64();

    MakeDecoder(default_options);

    auto joiner = RunThread([&]() {
      SleepFor(1e-3);
      InsertChunk(0, {"1.5", "2", "3"});
      InsertChunk(3, {"6"});
      decoder_->SetEOF(4);
    });

    // These chunks will wait for inference to run on chunk 0
    InsertChunk(1, {"4", "-5", "N/A"});
    InsertChunk(2, {});

    AssertFetch(ArrayFromJSON(type, "[1.5, 2, 3]"));
    AssertFetch(ArrayFromJSON(type, "[4, -5, null]"));
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetch(ArrayFromJSON(type, "[6]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestOptions() {
    auto type = boolean();

    auto options = default_options;
    options.null_values = {"true"};
    options.true_values = {"false"};
    options.false_values = {"N/A"};
    MakeDecoder(options);

    AppendChunks({{"true", "false", "N/A"}, {"true"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[null, true, false]"));
    AssertFetch(ArrayFromJSON(type, "[null]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestErrors() {
    auto type = int64();

    MakeDecoder(default_options);

    AppendChunks({{"123", "456", "-78"}, {"9.5", "N/A"}});
    AppendChunks({{"1000", "N/A"}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[123, 456, -78]"));
    AssertFetchInvalid();
    AssertFetch(ArrayFromJSON(type, "[1000, null]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }

  void TestEmpty() {
    auto type = null();

    MakeDecoder(default_options);

    AppendChunks({{}, {}});
    SetEOF();
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetch(ArrayFromJSON(type, "[]"));
    AssertFetchEOF();
    AssertFetchEOF();
  }
};

TYPED_TEST_SUITE(InferringColumnDecoderTest, ExecutorTypes);

TYPED_TEST(InferringColumnDecoderTest, Integers) { this->TestIntegers(); }

TYPED_TEST(InferringColumnDecoderTest, Threaded) { this->TestThreaded(); }

TYPED_TEST(InferringColumnDecoderTest, Options) { this->TestOptions(); }

TYPED_TEST(InferringColumnDecoderTest, Errors) { this->TestErrors(); }

TYPED_TEST(InferringColumnDecoderTest, Empty) { this->TestEmpty(); }

// More inference tests are in InferringColumnBuilderTest

}  // namespace csv
}  // namespace arrow
