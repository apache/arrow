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

#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <ostream>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/slow.h"
#include "arrow/io/transform.h"
#include "arrow/io/util_internal.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

namespace io {

std::ostream& operator<<(std::ostream& os, const ReadRange& range) {
  return os << "<offset=" << range.offset << ", length=" << range.length << ">";
}

class TestBufferOutputStream : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(buffer_, AllocateResizableBuffer(0));
    stream_.reset(new BufferOutputStream(buffer_));
  }

 protected:
  std::shared_ptr<ResizableBuffer> buffer_;
  std::unique_ptr<OutputStream> stream_;
};

TEST_F(TestBufferOutputStream, DtorCloses) {
  std::string data = "data123456";

  const int K = 100;
  for (int i = 0; i < K; ++i) {
    ARROW_EXPECT_OK(stream_->Write(data));
  }

  stream_ = nullptr;
  ASSERT_EQ(static_cast<int64_t>(K * data.size()), buffer_->size());
}

TEST_F(TestBufferOutputStream, CloseResizes) {
  std::string data = "data123456";

  const int K = 100;
  for (int i = 0; i < K; ++i) {
    ARROW_EXPECT_OK(stream_->Write(data));
  }

  ASSERT_OK(stream_->Close());
  ASSERT_EQ(static_cast<int64_t>(K * data.size()), buffer_->size());
}

TEST_F(TestBufferOutputStream, WriteAfterFinish) {
  std::string data = "data123456";
  ASSERT_OK(stream_->Write(data));

  auto buffer_stream = checked_cast<BufferOutputStream*>(stream_.get());

  ASSERT_OK(buffer_stream->Finish());

  ASSERT_RAISES(IOError, stream_->Write(data));
}

TEST_F(TestBufferOutputStream, Reset) {
  std::string data = "data123456";

  auto stream = checked_cast<BufferOutputStream*>(stream_.get());

  ASSERT_OK(stream->Write(data));

  ASSERT_OK_AND_ASSIGN(auto buffer, stream->Finish());
  ASSERT_EQ(buffer->size(), static_cast<int64_t>(data.size()));

  ASSERT_OK(stream->Reset(2048));
  ASSERT_OK(stream->Write(data));
  ASSERT_OK(stream->Write(data));
  ASSERT_OK_AND_ASSIGN(auto buffer2, stream->Finish());

  ASSERT_EQ(buffer2->size(), static_cast<int64_t>(data.size() * 2));
}

TEST(TestFixedSizeBufferWriter, Basics) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, AllocateBuffer(1024));

  FixedSizeBufferWriter writer(buffer);

  ASSERT_OK_AND_EQ(0, writer.Tell());

  std::string data = "data123456";
  auto nbytes = static_cast<int64_t>(data.size());
  ASSERT_OK(writer.Write(data.c_str(), nbytes));

  ASSERT_OK_AND_EQ(nbytes, writer.Tell());

  ASSERT_OK(writer.Seek(4));
  ASSERT_OK_AND_EQ(4, writer.Tell());

  ASSERT_OK(writer.Seek(1024));
  ASSERT_OK_AND_EQ(1024, writer.Tell());

  // Write out of bounds
  ASSERT_RAISES(IOError, writer.Write(data.c_str(), 1));

  // Seek out of bounds
  ASSERT_RAISES(IOError, writer.Seek(-1));
  ASSERT_RAISES(IOError, writer.Seek(1025));

  ASSERT_OK(writer.Close());
}

TEST(TestFixedSizeBufferWriter, InvalidWrites) {
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer, AllocateBuffer(1024));

  FixedSizeBufferWriter writer(buffer);
  const uint8_t data[10]{};
  ASSERT_RAISES(Invalid, writer.WriteAt(-1, data, 1));
  ASSERT_RAISES(Invalid, writer.WriteAt(1, data, -1));
}

TEST(TestBufferReader, FromStrings) {
  // ARROW-3291: construct BufferReader from std::string or
  // arrow::util::string_view

  std::string data = "data123456";
  auto view = util::string_view(data);

  BufferReader reader1(data);
  BufferReader reader2(view);

  std::shared_ptr<Buffer> piece;
  ASSERT_OK_AND_ASSIGN(piece, reader1.Read(4));
  ASSERT_EQ(0, memcmp(piece->data(), data.data(), 4));

  ASSERT_OK(reader2.Seek(2));
  ASSERT_OK_AND_ASSIGN(piece, reader2.Read(4));
  ASSERT_EQ(0, memcmp(piece->data(), data.data() + 2, 4));
}

TEST(TestBufferReader, Seeking) {
  std::string data = "data123456";

  BufferReader reader(data);
  ASSERT_OK_AND_EQ(0, reader.Tell());

  ASSERT_OK(reader.Seek(9));
  ASSERT_OK_AND_EQ(9, reader.Tell());

  ASSERT_OK(reader.Seek(10));
  ASSERT_OK_AND_EQ(10, reader.Tell());

  ASSERT_RAISES(IOError, reader.Seek(11));
  ASSERT_OK_AND_EQ(10, reader.Tell());
}

TEST(TestBufferReader, Peek) {
  std::string data = "data123456";

  BufferReader reader(std::make_shared<Buffer>(data));

  util::string_view view;

  ASSERT_OK_AND_ASSIGN(view, reader.Peek(4));

  ASSERT_EQ(4, view.size());
  ASSERT_EQ(data.substr(0, 4), view.to_string());

  ASSERT_OK_AND_ASSIGN(view, reader.Peek(20));
  ASSERT_EQ(data.size(), view.size());
  ASSERT_EQ(data, view.to_string());
}

TEST(TestBufferReader, ReadAsync) {
  std::string data = "data123456";

  BufferReader reader(std::make_shared<Buffer>(data));

  auto fut1 = reader.ReadAsync({}, 2, 6);
  auto fut2 = reader.ReadAsync({}, 1, 4);
  ASSERT_EQ(fut1.state(), FutureState::SUCCESS);
  ASSERT_EQ(fut2.state(), FutureState::SUCCESS);
  ASSERT_OK_AND_ASSIGN(auto buf, fut1.result());
  AssertBufferEqual(*buf, "ta1234");
  ASSERT_OK_AND_ASSIGN(buf, fut2.result());
  AssertBufferEqual(*buf, "ata1");
}

TEST(TestBufferReader, InvalidReads) {
  std::string data = "data123456";
  BufferReader reader(std::make_shared<Buffer>(data));
  uint8_t buffer[10];

  ASSERT_RAISES(Invalid, reader.ReadAt(-1, 1));
  ASSERT_RAISES(Invalid, reader.ReadAt(1, -1));
  ASSERT_RAISES(Invalid, reader.ReadAt(-1, 1, buffer));
  ASSERT_RAISES(Invalid, reader.ReadAt(1, -1, buffer));

  ASSERT_RAISES(Invalid, reader.ReadAsync({}, -1, 1).result());
  ASSERT_RAISES(Invalid, reader.ReadAsync({}, 1, -1).result());
}

TEST(TestBufferReader, RetainParentReference) {
  // ARROW-387
  std::string data = "data123456";

  std::shared_ptr<Buffer> slice1;
  std::shared_ptr<Buffer> slice2;
  {
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer,
                         AllocateBuffer(static_cast<int64_t>(data.size())));
    std::memcpy(buffer->mutable_data(), data.c_str(), data.size());
    BufferReader reader(buffer);
    ASSERT_OK_AND_ASSIGN(slice1, reader.Read(4));
    ASSERT_OK_AND_ASSIGN(slice2, reader.Read(6));
  }

  ASSERT_TRUE(slice1->parent() != nullptr);

  ASSERT_EQ(0, std::memcmp(slice1->data(), data.c_str(), 4));
  ASSERT_EQ(0, std::memcmp(slice2->data(), data.c_str() + 4, 6));
}

TEST(TestBufferReader, WillNeed) {
  {
    std::string data = "data123456";
    BufferReader reader(std::make_shared<Buffer>(data));

    ASSERT_OK(reader.WillNeed({}));
    ASSERT_OK(reader.WillNeed({{0, 4}, {4, 6}}));
    ASSERT_OK(reader.WillNeed({{10, 0}}));
    ASSERT_RAISES(IOError, reader.WillNeed({{11, 1}}));  // Out of bounds
  }
  {
    std::string data = "data123456";
    BufferReader reader(reinterpret_cast<const uint8_t*>(data.data()),
                        static_cast<int64_t>(data.size()));

    ASSERT_OK(reader.WillNeed({{0, 4}, {4, 6}}));
    ASSERT_RAISES(IOError, reader.WillNeed({{11, 1}}));  // Out of bounds
  }
}

TEST(TestRandomAccessFile, GetStream) {
  std::string data = "data1data2data3data4data5";

  auto buf = std::make_shared<Buffer>(data);
  auto file = std::make_shared<BufferReader>(buf);

  std::shared_ptr<InputStream> stream1, stream2;

  stream1 = RandomAccessFile::GetStream(file, 0, 10);
  stream2 = RandomAccessFile::GetStream(file, 9, 16);

  ASSERT_OK_AND_EQ(0, stream1->Tell());

  std::shared_ptr<Buffer> buf2;
  uint8_t buf3[20];

  ASSERT_OK_AND_EQ(4, stream2->Read(4, buf3));
  ASSERT_EQ(0, std::memcmp(buf3, "2dat", 4));
  ASSERT_OK_AND_EQ(4, stream2->Tell());

  ASSERT_OK_AND_EQ(6, stream1->Read(6, buf3));
  ASSERT_EQ(0, std::memcmp(buf3, "data1d", 6));
  ASSERT_OK_AND_EQ(6, stream1->Tell());

  ASSERT_OK_AND_ASSIGN(buf2, stream1->Read(2));
  ASSERT_TRUE(SliceBuffer(buf, 6, 2)->Equals(*buf2));

  // Read to end of each stream
  ASSERT_OK_AND_EQ(2, stream1->Read(4, buf3));
  ASSERT_EQ(0, std::memcmp(buf3, "a2", 2));
  ASSERT_OK_AND_EQ(10, stream1->Tell());

  ASSERT_OK_AND_EQ(0, stream1->Read(1, buf3));
  ASSERT_OK_AND_EQ(10, stream1->Tell());

  // stream2 had its extent limited
  ASSERT_OK_AND_ASSIGN(buf2, stream2->Read(20));
  ASSERT_TRUE(SliceBuffer(buf, 13, 12)->Equals(*buf2));

  ASSERT_OK_AND_ASSIGN(buf2, stream2->Read(1));
  ASSERT_EQ(0, buf2->size());
  ASSERT_OK_AND_EQ(16, stream2->Tell());

  ASSERT_OK(stream1->Close());

  // idempotent
  ASSERT_OK(stream1->Close());
  ASSERT_TRUE(stream1->closed());

  // Check whether closed
  ASSERT_RAISES(IOError, stream1->Tell());
  ASSERT_RAISES(IOError, stream1->Read(1));
  ASSERT_RAISES(IOError, stream1->Read(1, buf3));
}

TEST(TestMemcopy, ParallelMemcopy) {
#if defined(ARROW_VALGRIND)
  // Compensate for Valgrind's slowness
  constexpr int64_t THRESHOLD = 32 * 1024;
#else
  constexpr int64_t THRESHOLD = 1024 * 1024;
#endif

  for (int i = 0; i < 5; ++i) {
    // randomize size so the memcopy alignment is tested
    int64_t total_size = 3 * THRESHOLD + std::rand() % 100;

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer1, AllocateBuffer(total_size));
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Buffer> buffer2, AllocateBuffer(total_size));

    random_bytes(total_size, 0, buffer2->mutable_data());

    io::FixedSizeBufferWriter writer(buffer1);
    writer.set_memcopy_threads(4);
    writer.set_memcopy_threshold(THRESHOLD);
    ASSERT_OK(writer.Write(buffer2->data(), buffer2->size()));

    ASSERT_EQ(0, memcmp(buffer1->data(), buffer2->data(), buffer1->size()));
  }
}

// -----------------------------------------------------------------------
// Test slow streams

template <typename SlowStreamType>
void TestSlowInputStream() {
  using clock = std::chrono::high_resolution_clock;

  auto stream = std::make_shared<BufferReader>(util::string_view("abcdefghijkl"));
  const double latency = 0.6;
  auto slow = std::make_shared<SlowStreamType>(stream, latency);

  ASSERT_FALSE(slow->closed());
  auto t1 = clock::now();
  ASSERT_OK_AND_ASSIGN(auto buf, slow->Read(6));
  auto t2 = clock::now();
  AssertBufferEqual(*buf, "abcdef");
  auto dt = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1).count();
#ifdef ARROW_WITH_TIMING_TESTS
  ASSERT_LT(dt, latency * 3);  // likely
  ASSERT_GT(dt, latency / 3);  // likely
#else
  ARROW_UNUSED(dt);
#endif

  ASSERT_OK_AND_ASSIGN(util::string_view view, slow->Peek(4));
  ASSERT_EQ(view, util::string_view("ghij"));

  ASSERT_OK(slow->Close());
  ASSERT_TRUE(slow->closed());
  ASSERT_TRUE(stream->closed());
  ASSERT_OK(slow->Close());
  ASSERT_TRUE(slow->closed());
  ASSERT_TRUE(stream->closed());
}

TEST(TestSlowInputStream, Basics) { TestSlowInputStream<SlowInputStream>(); }

TEST(TestSlowRandomAccessFile, Basics) { TestSlowInputStream<SlowRandomAccessFile>(); }

// -----------------------------------------------------------------------
// Test transform streams

struct DoublingTransform {
  // A transform that duplicates every byte
  Result<std::shared_ptr<Buffer>> operator()(const std::shared_ptr<Buffer>& buf) {
    ARROW_ASSIGN_OR_RAISE(auto dest, AllocateBuffer(buf->size() * 2));
    const uint8_t* data = buf->data();
    uint8_t* out_data = dest->mutable_data();
    for (int64_t i = 0; i < buf->size(); ++i) {
      out_data[i * 2] = data[i];
      out_data[i * 2 + 1] = data[i];
    }
    return std::shared_ptr<Buffer>(std::move(dest));
  }
};

struct SwappingTransform {
  // A transform that swaps every pair of bytes
  Result<std::shared_ptr<Buffer>> operator()(const std::shared_ptr<Buffer>& buf) {
    int64_t dest_size = BitUtil::RoundDown(buf->size() + has_pending_, 2);
    ARROW_ASSIGN_OR_RAISE(auto dest, AllocateBuffer(dest_size));
    const uint8_t* data = buf->data();
    uint8_t* out_data = dest->mutable_data();
    if (has_pending_) {
      *out_data++ = *data++;
      *out_data++ = pending_byte_;
      dest_size -= 2;
    }
    for (int64_t i = 0; i < dest_size; i += 2) {
      out_data[i] = data[i + 1];
      out_data[i + 1] = data[i];
    }
    has_pending_ = has_pending_ ^ (buf->size() & 1);
    if (has_pending_) {
      pending_byte_ = buf->data()[buf->size() - 1];
    }
    return std::shared_ptr<Buffer>(std::move(dest));
  }

 protected:
  bool has_pending_ = 0;
  uint8_t pending_byte_ = 0;
};

struct BaseShrinkingTransform {
  // A transform that keeps one byte every N bytes
  explicit BaseShrinkingTransform(int64_t keep_every) : keep_every_(keep_every) {}

  Result<std::shared_ptr<Buffer>> operator()(const std::shared_ptr<Buffer>& buf) {
    int64_t dest_size = (buf->size() - skip_bytes_ + keep_every_ - 1) / keep_every_;
    ARROW_ASSIGN_OR_RAISE(auto dest, AllocateBuffer(dest_size));
    const uint8_t* data = buf->data() + skip_bytes_;
    uint8_t* out_data = dest->mutable_data();
    for (int64_t i = 0; i < dest_size; ++i) {
      out_data[i] = data[i * keep_every_];
    }
    if (dest_size > 0) {
      skip_bytes_ = skip_bytes_ + dest_size * keep_every_ - buf->size();
    } else {
      skip_bytes_ = skip_bytes_ - buf->size();
    }
    DCHECK_GE(skip_bytes_, 0);
    DCHECK_LT(skip_bytes_, keep_every_);
    return std::shared_ptr<Buffer>(std::move(dest));
  }

 protected:
  int64_t skip_bytes_ = 0;
  const int64_t keep_every_;
};

template <int N>
struct ShrinkingTransform : public BaseShrinkingTransform {
  ShrinkingTransform() : BaseShrinkingTransform(N) {}
};

template <typename T>
class TestTransformInputStream : public ::testing::Test {
 public:
  TransformInputStream::TransformFunc transform() const { return T(); }

  void TestEmptyStream() {
    auto wrapped = std::make_shared<BufferReader>(util::string_view());
    auto stream = std::make_shared<TransformInputStream>(wrapped, transform());

    ASSERT_OK_AND_EQ(0, stream->Tell());
    ASSERT_OK_AND_ASSIGN(auto buf, stream->Read(123));
    ASSERT_EQ(buf->size(), 0);
    ASSERT_OK_AND_ASSIGN(buf, stream->Read(0));
    ASSERT_EQ(buf->size(), 0);
    ASSERT_OK_AND_EQ(0, stream->Read(5, out_data_));
    ASSERT_OK_AND_EQ(0, stream->Tell());
  }

  void TestBasics() {
    auto src = Buffer::FromString("1234567890abcdefghi");
    ASSERT_OK_AND_ASSIGN(auto expected, this->transform()(src));

    auto stream = std::make_shared<TransformInputStream>(
        std::make_shared<BufferReader>(src), this->transform());
    std::shared_ptr<Buffer> actual;
    AccumulateReads(stream, 200, &actual);
    AssertBufferEqual(*actual, *expected);
  }

  void TestClose() {
    auto src = Buffer::FromString("1234567890abcdefghi");
    auto stream = std::make_shared<TransformInputStream>(
        std::make_shared<BufferReader>(src), this->transform());
    ASSERT_FALSE(stream->closed());
    ASSERT_OK(stream->Close());
    ASSERT_TRUE(stream->closed());
    ASSERT_RAISES(Invalid, stream->Read(1));
    ASSERT_RAISES(Invalid, stream->Read(1, out_data_));
    ASSERT_RAISES(Invalid, stream->Tell());
    ASSERT_OK(stream->Close());
    ASSERT_TRUE(stream->closed());
  }

  void TestChunked() {
    auto src = Buffer::FromString("1234567890abcdefghi");
    ASSERT_OK_AND_ASSIGN(auto expected, this->transform()(src));

    auto stream = std::make_shared<TransformInputStream>(
        std::make_shared<BufferReader>(src), this->transform());
    std::shared_ptr<Buffer> actual;
    AccumulateReads(stream, 5, &actual);
    AssertBufferEqual(*actual, *expected);
  }

  void TestStressChunked() {
    ASSERT_OK_AND_ASSIGN(auto unique_src, AllocateBuffer(1000));
    auto src = std::shared_ptr<Buffer>(std::move(unique_src));
    random_bytes(src->size(), /*seed=*/42, src->mutable_data());

    ASSERT_OK_AND_ASSIGN(auto expected, this->transform()(src));

    std::default_random_engine gen(42);
    std::uniform_int_distribution<int> chunk_sizes(0, 20);

    auto stream = std::make_shared<TransformInputStream>(
        std::make_shared<BufferReader>(src), this->transform());
    std::shared_ptr<Buffer> actual;
    AccumulateReads(
        stream, [&]() -> int64_t { return chunk_sizes(gen); }, &actual);
    AssertBufferEqual(*actual, *expected);
  }

  void AccumulateReads(const std::shared_ptr<InputStream>& stream,
                       std::function<int64_t()> gen_chunk_sizes,
                       std::shared_ptr<Buffer>* out) {
    std::vector<std::shared_ptr<Buffer>> buffers;
    int64_t total_size = 0;
    while (true) {
      const int64_t chunk_size = gen_chunk_sizes();
      ASSERT_OK_AND_ASSIGN(auto buf, stream->Read(chunk_size));
      const int64_t buf_size = buf->size();
      total_size += buf_size;
      ASSERT_OK_AND_EQ(total_size, stream->Tell());
      if (chunk_size > 0 && buf_size == 0) {
        // EOF
        break;
      }
      buffers.push_back(std::move(buf));
      if (buf_size < chunk_size) {
        // Short read should imply EOF on next read
        ASSERT_OK_AND_ASSIGN(auto buf, stream->Read(100));
        ASSERT_EQ(buf->size(), 0);
        break;
      }
    }
    ASSERT_OK_AND_ASSIGN(*out, ConcatenateBuffers(buffers));
  }

  void AccumulateReads(const std::shared_ptr<InputStream>& stream, int64_t chunk_size,
                       std::shared_ptr<Buffer>* out) {
    return AccumulateReads(
        stream, [=]() { return chunk_size; }, out);
  }

 protected:
  uint8_t* out_data_[10];
};

using TransformTypes =
    ::testing::Types<DoublingTransform, SwappingTransform, ShrinkingTransform<2>,
                     ShrinkingTransform<3>, ShrinkingTransform<7>>;

TYPED_TEST_SUITE(TestTransformInputStream, TransformTypes);

TYPED_TEST(TestTransformInputStream, EmptyStream) { this->TestEmptyStream(); }

TYPED_TEST(TestTransformInputStream, Basics) { this->TestBasics(); }

TYPED_TEST(TestTransformInputStream, Close) { this->TestClose(); }

TYPED_TEST(TestTransformInputStream, Chunked) { this->TestChunked(); }

TYPED_TEST(TestTransformInputStream, StressChunked) { this->TestStressChunked(); }

static Result<std::shared_ptr<Buffer>> FailingTransform(
    const std::shared_ptr<Buffer>& buf) {
  return Status::UnknownError("Failed transform");
}

TEST(TestTransformInputStream, FailingTransform) {
  auto src = Buffer::FromString("1234567890abcdefghi");
  auto stream = std::make_shared<TransformInputStream>(
      std::make_shared<BufferReader>(src), FailingTransform);
  ASSERT_RAISES(UnknownError, stream->Read(5));
}

// -----------------------------------------------------------------------
// Test various utilities

TEST(TestInputStreamIterator, Basics) {
  auto reader = std::make_shared<BufferReader>(Buffer::FromString("data123456"));
  ASSERT_OK_AND_ASSIGN(auto it, MakeInputStreamIterator(reader, /*block_size=*/3));
  std::shared_ptr<Buffer> buf;
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  AssertBufferEqual(*buf, "dat");
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  AssertBufferEqual(*buf, "a12");
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  AssertBufferEqual(*buf, "345");
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  AssertBufferEqual(*buf, "6");
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  ASSERT_EQ(buf, nullptr);
  ASSERT_OK_AND_ASSIGN(buf, it.Next());
  ASSERT_EQ(buf, nullptr);
}

TEST(TestInputStreamIterator, Closed) {
  auto reader = std::make_shared<BufferReader>(Buffer::FromString("data123456"));
  ASSERT_OK(reader->Close());
  ASSERT_RAISES(Invalid, MakeInputStreamIterator(reader, 3));

  reader = std::make_shared<BufferReader>(Buffer::FromString("data123456"));
  ASSERT_OK_AND_ASSIGN(auto it, MakeInputStreamIterator(reader, /*block_size=*/3));
  ASSERT_OK_AND_ASSIGN(auto buf, it.Next());
  AssertBufferEqual(*buf, "dat");
  // Close stream and read from iterator
  ASSERT_OK(reader->Close());
  ASSERT_RAISES(Invalid, it.Next().status());
}

TEST(CoalesceReadRanges, Basics) {
  auto check = [](std::vector<ReadRange> ranges,
                  std::vector<ReadRange> expected) -> void {
    const int64_t hole_size_limit = 9;
    const int64_t range_size_limit = 99;
    auto coalesced =
        internal::CoalesceReadRanges(ranges, hole_size_limit, range_size_limit);
    ASSERT_EQ(coalesced, expected);
  };

  check({}, {});
  // Zero sized range that ends up in empty list
  check({{110, 0}}, {});
  // Combination on 1 zero sized range and 1 non-zero sized range
  check({{110, 10}, {120, 0}}, {{110, 10}});
  // 1 non-zero sized range
  check({{110, 10}}, {{110, 10}});
  // No holes + unordered ranges
  check({{130, 10}, {110, 10}, {120, 10}}, {{110, 30}});
  // No holes
  check({{110, 10}, {120, 10}, {130, 10}}, {{110, 30}});
  // Small holes only
  check({{110, 11}, {130, 11}, {150, 11}}, {{110, 51}});
  // Large holes
  check({{110, 10}, {130, 10}}, {{110, 10}, {130, 10}});
  check({{110, 11}, {130, 11}, {150, 10}, {170, 11}, {190, 11}}, {{110, 50}, {170, 31}});

  // With zero-sized ranges
  check({{110, 11}, {130, 0}, {130, 11}, {145, 0}, {150, 11}, {200, 0}}, {{110, 51}});

  // No holes but large ranges
  check({{110, 100}, {210, 100}}, {{110, 100}, {210, 100}});
  // Small holes and large range in the middle (*)
  check({{110, 10}, {120, 11}, {140, 100}, {240, 11}, {260, 11}},
        {{110, 21}, {140, 100}, {240, 31}});
  // Mid-size ranges that would turn large after coalescing
  check({{100, 50}, {150, 50}}, {{100, 50}, {150, 50}});
  check({{100, 30}, {130, 30}, {160, 30}, {190, 30}, {220, 30}}, {{100, 90}, {190, 60}});

  // Same as (*) but unsorted
  check({{140, 100}, {120, 11}, {240, 11}, {110, 10}, {260, 11}},
        {{110, 21}, {140, 100}, {240, 31}});
}

TEST(RangeReadCache, Basics) {
  std::string data = "abcdefghijklmnopqrstuvwxyz";

  auto file = std::make_shared<BufferReader>(Buffer(data));
  CacheOptions options = CacheOptions::Defaults();
  options.hole_size_limit = 2;
  options.range_size_limit = 10;
  internal::ReadRangeCache cache(file, {}, options);

  ASSERT_OK(cache.Cache({{1, 2}, {3, 2}, {8, 2}, {20, 2}, {25, 0}}));
  ASSERT_OK(cache.Cache({{10, 4}, {14, 0}, {15, 4}}));

  ASSERT_OK_AND_ASSIGN(auto buf, cache.Read({20, 2}));
  AssertBufferEqual(*buf, "uv");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({1, 2}));
  AssertBufferEqual(*buf, "bc");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({3, 2}));
  AssertBufferEqual(*buf, "de");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({8, 2}));
  AssertBufferEqual(*buf, "ij");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({10, 4}));
  AssertBufferEqual(*buf, "klmn");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({15, 4}));
  AssertBufferEqual(*buf, "pqrs");
  // Zero-sized
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({14, 0}));
  AssertBufferEqual(*buf, "");
  ASSERT_OK_AND_ASSIGN(buf, cache.Read({25, 0}));
  AssertBufferEqual(*buf, "");

  // Non-cached ranges
  ASSERT_RAISES(Invalid, cache.Read({20, 3}));
  ASSERT_RAISES(Invalid, cache.Read({19, 3}));
  ASSERT_RAISES(Invalid, cache.Read({0, 3}));
  ASSERT_RAISES(Invalid, cache.Read({25, 2}));
}

TEST(CacheOptions, Basics) {
  auto check = [](const CacheOptions actual, const double expected_hole_size_limit_MiB,
                  const double expected_range_size_limit_MiB) -> void {
    const CacheOptions expected = {
        static_cast<int64_t>(std::round(expected_hole_size_limit_MiB * 1024 * 1024)),
        static_cast<int64_t>(std::round(expected_range_size_limit_MiB * 1024 * 1024))};
    ASSERT_EQ(actual, expected);
  };

  // Test: normal usage.
  // TTFB = 5 ms, BW = 500 MiB/s,
  // we expect hole_size_limit = 2.5 MiB, and range_size_limit = 22.5 MiB
  check(CacheOptions::MakeFromNetworkMetrics(5, 500), 2.5, 22.5);
  // Test: custom bandwidth utilization.
  // TTFB = 5 ms, BW = 500 MiB/s, BW_utilization = 75%,
  // we expect a change in range_size_limit = 7.5 MiB.
  check(CacheOptions::MakeFromNetworkMetrics(5, 500, .75), 2.5, 7.5);
  // Test: custom max_ideal_request_size, range_size_limit gets capped.
  // TTFB = 5 ms, BW = 500 MiB/s, BW_utilization = 75%, max_ideal_request_size = 5 MiB,
  // we expect the range_size_limit to be capped at 5 MiB.
  check(CacheOptions::MakeFromNetworkMetrics(5, 500, .75, 5), 2.5, 5);
}

}  // namespace io
}  // namespace arrow
