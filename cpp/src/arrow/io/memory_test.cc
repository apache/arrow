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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/caching.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/slow.h"
#include "arrow/io/util_internal.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/iterator.h"

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

  auto fut1 = reader.ReadAsync(2, 6);
  auto fut2 = reader.ReadAsync(1, 4);
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

  ASSERT_RAISES(Invalid, reader.ReadAsync(-1, 1).result());
  ASSERT_RAISES(Invalid, reader.ReadAsync(1, -1).result());
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
  const int64_t hole_size_limit = 2;
  const int64_t range_size_limit = 10;
  internal::ReadRangeCache cache(file, hole_size_limit, range_size_limit);

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

}  // namespace io
}  // namespace arrow
