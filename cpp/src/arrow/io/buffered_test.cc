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

#ifndef _WIN32
#  include <fcntl.h>  // IWYU pragma: keep
#  include <unistd.h>
#endif

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <iterator>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <valarray>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/buffer.h"
#include "arrow/io/buffered.h"
#include "arrow/io/file.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"

namespace arrow::io {

using ::arrow::internal::TemporaryDir;

static std::string GenerateRandomData(size_t nbytes) {
  // MSVC doesn't accept uint8_t for std::independent_bits_engine<>
  typedef unsigned long UInt;  // NOLINT
  std::independent_bits_engine<std::default_random_engine, 8 * sizeof(UInt), UInt> engine;

  std::vector<UInt> data(nbytes / sizeof(UInt) + 1);
  std::generate(begin(data), end(data), std::ref(engine));
  return std::string(reinterpret_cast<char*>(data.data()), nbytes);
}

template <typename FileType>
class FileTestFixture : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK_AND_ASSIGN(temp_dir_, TemporaryDir::Make("buffered-test-"));
    path_ = temp_dir_->path()
                .Join("arrow-test-io-buffered-stream.txt")
                .ValueOrDie()
                .ToString();
    EnsureFileDeleted();
  }

  void TearDown() { EnsureFileDeleted(); }

  void EnsureFileDeleted() {
    if (FileExists(path_)) {
      ARROW_UNUSED(std::remove(path_.c_str()));
    }
  }

  void AssertTell(int64_t expected) { ASSERT_OK_AND_EQ(expected, buffered_->Tell()); }

 protected:
  int fd_;
  std::shared_ptr<FileType> buffered_;
  std::string path_;
  std::unique_ptr<TemporaryDir> temp_dir_;
};

// ----------------------------------------------------------------------
// Buffered output tests

constexpr int64_t kDefaultBufferSize = 4096;

class TestBufferedOutputStream : public FileTestFixture<BufferedOutputStream> {
 public:
  void OpenBuffered(int64_t buffer_size = kDefaultBufferSize, bool append = false) {
    // So that any open file is closed
    buffered_.reset();

    ASSERT_OK_AND_ASSIGN(auto file, FileOutputStream::Open(path_, append));
    fd_ = file->file_descriptor();
    if (append) {
      // Workaround for ARROW-2466 ("append" flag doesn't set file pos)
#if defined(_MSC_VER)
      _lseeki64(fd_, 0, SEEK_END);
#else
      lseek(fd_, 0, SEEK_END);
#endif
    }
    ASSERT_OK_AND_ASSIGN(buffered_, BufferedOutputStream::Create(
                                        buffer_size, default_memory_pool(), file));
  }

  void WriteChunkwise(const std::string& datastr, const std::valarray<int64_t>& sizes) {
    const char* data = datastr.data();
    const int64_t data_size = static_cast<int64_t>(datastr.size());
    int64_t data_pos = 0;
    auto size_it = std::begin(sizes);

    // Write datastr, chunk by chunk, until exhausted
    while (true) {
      int64_t size = *size_it++;
      if (size_it == std::end(sizes)) {
        size_it = std::begin(sizes);
      }
      if (data_pos + size > data_size) {
        break;
      }
      ASSERT_OK(buffered_->Write(data + data_pos, size));
      data_pos += size;
    }
    ASSERT_OK(buffered_->Write(data + data_pos, data_size - data_pos));
  }
};

TEST_F(TestBufferedOutputStream, DestructorClosesFile) {
  OpenBuffered();
  ASSERT_FALSE(FileIsClosed(fd_));
  buffered_.reset();
  ASSERT_TRUE(FileIsClosed(fd_));
}

TEST_F(TestBufferedOutputStream, Detach) {
  OpenBuffered();
  const std::string datastr = "1234568790";

  ASSERT_OK(buffered_->Write(datastr.data(), 10));

  ASSERT_OK_AND_ASSIGN(auto detached_stream, buffered_->Detach());

  // Destroying the stream does not close the file because we have detached
  buffered_.reset();
  ASSERT_FALSE(FileIsClosed(fd_));

  ASSERT_OK(detached_stream->Close());
  ASSERT_TRUE(FileIsClosed(fd_));

  AssertFileContents(path_, datastr);
}

TEST_F(TestBufferedOutputStream, ExplicitCloseClosesFile) {
  OpenBuffered();
  ASSERT_FALSE(buffered_->closed());
  ASSERT_FALSE(FileIsClosed(fd_));
  ASSERT_OK(buffered_->Close());
  ASSERT_TRUE(buffered_->closed());
  ASSERT_TRUE(FileIsClosed(fd_));
  // Idempotency
  ASSERT_OK(buffered_->Close());
  ASSERT_TRUE(buffered_->closed());
  ASSERT_TRUE(FileIsClosed(fd_));
}

TEST_F(TestBufferedOutputStream, InvalidWrites) {
  OpenBuffered();

  const char* data = "";
  ASSERT_RAISES(Invalid, buffered_->Write(data, -1));
}

TEST_F(TestBufferedOutputStream, TinyWrites) {
  OpenBuffered();

  const std::string datastr = "1234568790";
  const char* data = datastr.data();

  ASSERT_OK(buffered_->Write(data, 2));
  ASSERT_OK(buffered_->Write(data + 2, 6));
  ASSERT_OK(buffered_->Close());

  AssertFileContents(path_, datastr.substr(0, 8));
}

TEST_F(TestBufferedOutputStream, SmallWrites) {
  OpenBuffered();

  // Data here should be larger than BufferedOutputStream's buffer size
  const std::string data = GenerateRandomData(200000);
  const std::valarray<int64_t> sizes = {1, 1, 2, 3, 5, 8, 13};

  WriteChunkwise(data, sizes);
  ASSERT_OK(buffered_->Close());

  AssertFileContents(path_, data);
}

TEST_F(TestBufferedOutputStream, MixedWrites) {
  OpenBuffered();

  const std::string data = GenerateRandomData(300000);
  const std::valarray<int64_t> sizes = {1, 1, 2, 3, 70000};

  WriteChunkwise(data, sizes);
  ASSERT_OK(buffered_->Close());

  AssertFileContents(path_, data);
}

TEST_F(TestBufferedOutputStream, LargeWrites) {
  OpenBuffered();

  const std::string data = GenerateRandomData(800000);
  const std::valarray<int64_t> sizes = {10000, 60000, 70000};

  WriteChunkwise(data, sizes);
  ASSERT_OK(buffered_->Close());

  AssertFileContents(path_, data);
}

TEST_F(TestBufferedOutputStream, Flush) {
  OpenBuffered();

  const std::string datastr = "1234568790";
  const char* data = datastr.data();

  ASSERT_OK(buffered_->Write(data, datastr.size()));
  ASSERT_OK(buffered_->Flush());

  AssertFileContents(path_, datastr);

  ASSERT_OK(buffered_->Close());
}

TEST_F(TestBufferedOutputStream, SetBufferSize) {
  OpenBuffered(20);

  ASSERT_EQ(20, buffered_->buffer_size());

  const std::string datastr = "1234568790abcdefghij";
  const char* data = datastr.data();

  // Write part of the data, then shrink buffer size to make sure it gets
  // flushed
  ASSERT_OK(buffered_->Write(data, 10));
  ASSERT_OK(buffered_->SetBufferSize(10));

  ASSERT_EQ(10, buffered_->buffer_size());

  // Shrink buffer, write some buffered bytes, then expand buffer
  ASSERT_OK(buffered_->SetBufferSize(5));
  ASSERT_OK(buffered_->Write(data + 10, 3));
  ASSERT_OK(buffered_->SetBufferSize(10));
  ASSERT_EQ(3, buffered_->bytes_buffered());

  ASSERT_OK(buffered_->Write(data + 13, 7));
  ASSERT_OK(buffered_->Flush());

  AssertFileContents(path_, datastr);
  ASSERT_OK(buffered_->Close());
}

TEST_F(TestBufferedOutputStream, Tell) {
  OpenBuffered();

  AssertTell(0);
  AssertTell(0);
  WriteChunkwise(std::string(100, 'x'), {1, 1, 2, 3, 5, 8});
  AssertTell(100);
  WriteChunkwise(std::string(100000, 'x'), {60000});
  AssertTell(100100);

  ASSERT_OK(buffered_->Close());

  OpenBuffered(kDefaultBufferSize, true /* append */);
  AssertTell(100100);
  WriteChunkwise(std::string(90, 'x'), {1, 1, 2, 3, 5, 8});
  AssertTell(100190);

  ASSERT_OK(buffered_->Close());

  // write long bytes after raw_pos is cached
  OpenBuffered(/*buffer_size=*/3);
  AssertTell(0);
  ASSERT_OK(buffered_->Write("1234568790", 5));
  AssertTell(5);

  ASSERT_OK(buffered_->Close());

  OpenBuffered();
  AssertTell(0);
}

TEST_F(TestBufferedOutputStream, TruncatesFile) {
  OpenBuffered();

  const std::string datastr = "1234568790";
  ASSERT_OK(buffered_->Write(datastr.data(), datastr.size()));
  ASSERT_OK(buffered_->Close());

  AssertFileContents(path_, datastr);

  OpenBuffered();
  AssertFileContents(path_, "");
}

// ----------------------------------------------------------------------
// BufferedInputStream tests

const std::string_view kExample1 = "informaticacrobaticsimmolation";

class TestBufferedInputStream : public FileTestFixture<BufferedInputStream> {
 public:
  void SetUp() {
    FileTestFixture<BufferedInputStream>::SetUp();
    local_pool_ = MemoryPool::CreateDefault();
  }

  void MakeExample1(int64_t buffer_size, MemoryPool* pool = default_memory_pool(),
                    int64_t raw_read_bound = -1) {
    test_data_ = kExample1;

    ASSERT_OK_AND_ASSIGN(auto file_out, FileOutputStream::Open(path_));
    ASSERT_OK(file_out->Write(test_data_));
    ASSERT_OK(file_out->Close());

    ASSERT_OK_AND_ASSIGN(auto file_in, ReadableFile::Open(path_));
    raw_ = file_in;
    ASSERT_OK_AND_ASSIGN(
        buffered_, BufferedInputStream::Create(buffer_size, pool, raw_, raw_read_bound));
  }

 protected:
  std::unique_ptr<MemoryPool> local_pool_;
  std::string test_data_;
  std::shared_ptr<InputStream> raw_;
};

TEST_F(TestBufferedInputStream, InvalidReads) {
  const int64_t kBufferSize = 10;
  MakeExample1(kBufferSize);
  ASSERT_EQ(kBufferSize, buffered_->buffer_size());
  std::vector<char> buf(test_data_.size());
  ASSERT_RAISES(Invalid, buffered_->Read(-1, buf.data()));
}

TEST_F(TestBufferedInputStream, BasicOperation) {
  const int64_t kBufferSize = 10;
  MakeExample1(kBufferSize);
  ASSERT_EQ(kBufferSize, buffered_->buffer_size());

  ASSERT_OK_AND_EQ(0, buffered_->Tell());

  // Nothing in the buffer
  ASSERT_EQ(0, buffered_->bytes_buffered());

  std::vector<char> buf(test_data_.size());
  ASSERT_OK_AND_EQ(0, buffered_->Read(0, buf.data()));
  ASSERT_OK_AND_EQ(4, buffered_->Read(4, buf.data()));
  ASSERT_EQ(0, memcmp(buf.data(), test_data_.data(), 4));

  // 6 bytes remaining in buffer
  ASSERT_EQ(6, buffered_->bytes_buffered());

  // This make sure Peek() works well when buffered bytes are not enough
  ASSERT_OK_AND_ASSIGN(auto peek, buffered_->Peek(8));
  ASSERT_EQ(8, peek.size());
  ASSERT_EQ('r', peek.data()[0]);
  ASSERT_EQ('m', peek.data()[1]);
  ASSERT_EQ('a', peek.data()[2]);
  ASSERT_EQ('t', peek.data()[3]);
  ASSERT_EQ('i', peek.data()[4]);
  ASSERT_EQ('c', peek.data()[5]);
  ASSERT_EQ('a', peek.data()[6]);
  ASSERT_EQ('c', peek.data()[7]);

  // Buffered position is 4
  ASSERT_OK_AND_EQ(4, buffered_->Tell());

  // Raw position actually 12
  ASSERT_OK_AND_EQ(12, raw_->Tell());

  // Reading to end of buffered bytes does not cause any more data to be
  // buffered
  ASSERT_OK_AND_EQ(8, buffered_->Read(8, buf.data()));
  ASSERT_EQ(0, memcmp(buf.data(), test_data_.data() + 4, 8));

  ASSERT_EQ(0, buffered_->bytes_buffered());

  // Read to EOF, exceeding buffer size
  ASSERT_OK_AND_EQ(18, buffered_->Read(18, buf.data()));
  ASSERT_EQ(0, memcmp(buf.data(), test_data_.data() + 12, 18));
  ASSERT_EQ(0, buffered_->bytes_buffered());

  // Read to EOF
  ASSERT_OK_AND_EQ(0, buffered_->Read(1, buf.data()));
  ASSERT_OK_AND_EQ(test_data_.size(), buffered_->Tell());

  // Peek at EOF
  ASSERT_OK_AND_ASSIGN(peek, buffered_->Peek(10));
  ASSERT_EQ(0, peek.size());

  // Calling Close closes raw_
  ASSERT_OK(buffered_->Close());
  ASSERT_TRUE(buffered_->raw()->closed());
}

TEST_F(TestBufferedInputStream, Detach) {
  MakeExample1(10);
  auto raw = buffered_->Detach();
  ASSERT_OK(buffered_->Close());
  ASSERT_FALSE(raw->closed());
}

TEST_F(TestBufferedInputStream, ReadBuffer) {
  const int64_t kBufferSize = 10;
  MakeExample1(kBufferSize);

  std::shared_ptr<Buffer> buf;

  // Read exceeding buffer size
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(15));
  ASSERT_EQ(0, memcmp(buf->data(), test_data_.data(), 15));
  ASSERT_EQ(0, buffered_->bytes_buffered());

  // Buffered reads
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(6));
  ASSERT_EQ(6, buf->size());
  ASSERT_EQ(0, memcmp(buf->data(), test_data_.data() + 15, 6));
  ASSERT_EQ(4, buffered_->bytes_buffered());

  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(4));
  ASSERT_EQ(4, buf->size());
  ASSERT_EQ(0, memcmp(buf->data(), test_data_.data() + 21, 4));
  ASSERT_EQ(0, buffered_->bytes_buffered());
}

TEST_F(TestBufferedInputStream, SetBufferSize) {
  MakeExample1(5);

  std::shared_ptr<Buffer> buf;
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(5));
  ASSERT_EQ(5, buf->size());

  // Increase buffer size
  ASSERT_OK(buffered_->SetBufferSize(10));
  ASSERT_EQ(10, buffered_->buffer_size());
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(6));
  ASSERT_EQ(4, buffered_->bytes_buffered());

  // Consume until 5 byte left
  ASSERT_OK(buffered_->Read(15));

  // Read at EOF so there will be only 5 bytes in the buffer
  ASSERT_OK(buffered_->Read(2));

  // Cannot shrink buffer if it would destroy data
  ASSERT_RAISES(Invalid, buffered_->SetBufferSize(4));

  // Shrinking to exactly number of buffered bytes is ok
  ASSERT_OK(buffered_->SetBufferSize(5));
}

// GH-43060: Internal buffer should not greater than the
// bytes could buffer.
TEST_F(TestBufferedInputStream, BufferSizeLimit) {
  {
    // Buffer size should not exceeds raw_read_bound
    MakeExample1(/*buffer_size=*/100000, default_memory_pool(), /*raw_read_bound=*/15);
    EXPECT_EQ(15, buffered_->buffer_size());
  }
  {
    // Set a buffer size after read.
    MakeExample1(/*buffer_size=*/10, default_memory_pool(), /*raw_read_bound=*/15);
    ASSERT_OK(buffered_->Read(10));
    ASSERT_OK(buffered_->SetBufferSize(/*new_buffer_size=*/100000));
    EXPECT_EQ(5, buffered_->buffer_size());
  }
}

TEST_F(TestBufferedInputStream, PeekPastBufferedBytes) {
  // GH-43949: Peek and SetBufferSize should not affect the
  // buffered bytes.
  MakeExample1(/*buffer_size=*/10, default_memory_pool(), /*raw_read_bound=*/15);
  ASSERT_OK_AND_ASSIGN(auto bytes, buffered_->Read(9));
  EXPECT_EQ(std::string_view(*bytes), kExample1.substr(0, 9));
  ASSERT_EQ(1, buffered_->bytes_buffered());
  ASSERT_EQ(10, buffered_->buffer_size());
  ASSERT_OK_AND_ASSIGN(auto view, buffered_->Peek(3));
  EXPECT_EQ(view, kExample1.substr(9, 3));
  ASSERT_EQ(3, buffered_->bytes_buffered());
  ASSERT_EQ(12, buffered_->buffer_size());
  ASSERT_OK_AND_ASSIGN(view, buffered_->Peek(10));
  // Peek() cannot go past the `raw_read_bound`
  EXPECT_EQ(view, kExample1.substr(9, 6));
  ASSERT_EQ(6, buffered_->bytes_buffered());
  ASSERT_EQ(15, buffered_->buffer_size());
  // Do read
  ASSERT_OK_AND_ASSIGN(bytes, buffered_->Read(6));
  EXPECT_EQ(std::string_view(*bytes), kExample1.substr(9, 6));
  ASSERT_EQ(0, buffered_->bytes_buffered());
}

class TestBufferedInputStreamBound : public ::testing::Test {
 public:
  void SetUp() { CreateExample(/*bounded=*/true); }

  void CreateExample(bool bounded = true) {
    // Create a buffer larger than source size, to check that the
    // stream end is respected
    ASSERT_OK_AND_ASSIGN(auto buf, AllocateResizableBuffer(source_size_ + 10));
    ASSERT_LT(source_size_, buf->size());
    for (int i = 0; i < source_size_; i++) {
      buf->mutable_data()[i] = static_cast<uint8_t>(i);
    }
    source_ = std::make_shared<BufferReader>(std::move(buf));
    ASSERT_OK(source_->Advance(stream_offset_));
    ASSERT_OK_AND_ASSIGN(
        stream_, BufferedInputStream::Create(chunk_size_, default_memory_pool(), source_,
                                             bounded ? stream_size_ : -1));
  }

 protected:
  int64_t source_size_ = 256;
  int64_t stream_offset_ = 10;
  int64_t stream_size_ = source_size_ - stream_offset_;
  int64_t chunk_size_ = 50;
  std::shared_ptr<InputStream> source_;
  std::shared_ptr<BufferedInputStream> stream_;
};

TEST_F(TestBufferedInputStreamBound, Basics) {
  std::shared_ptr<Buffer> buffer;
  std::string_view view;

  // source is at offset 10
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(10));
  ASSERT_EQ(10, view.size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, view[i]) << i;
  }

  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(10));
  ASSERT_EQ(10, buffer->size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(10 + i, (*buffer)[i]) << i;
  }

  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(10));
  ASSERT_EQ(10, buffer->size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(20 + i, (*buffer)[i]) << i;
  }
  ASSERT_OK(stream_->Advance(5));
  ASSERT_OK(stream_->Advance(5));

  // source is at offset 40
  // read across buffer boundary. buffer size is 50
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(20));
  ASSERT_EQ(20, buffer->size());
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(40 + i, (*buffer)[i]) << i;
  }

  // read more than original chunk size
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(60));
  ASSERT_EQ(60, buffer->size());
  for (int i = 0; i < 60; i++) {
    ASSERT_EQ(60 + i, (*buffer)[i]) << i;
  }

  ASSERT_OK(stream_->Advance(120));

  // source is at offset 240
  // read outside of source boundary. source size is 256
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(30));

  ASSERT_EQ(16, buffer->size());
  for (int i = 0; i < 16; i++) {
    ASSERT_EQ(240 + i, (*buffer)[i]) << i;
  }
  // Stream exhausted
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(1));
  ASSERT_EQ(0, buffer->size());
}

TEST_F(TestBufferedInputStreamBound, LargeFirstPeek) {
  // Test a first peek larger than chunk size
  std::shared_ptr<Buffer> buffer;
  std::string_view view;
  int64_t n = 70;
  ASSERT_GT(n, chunk_size_);

  // source is at offset 10
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(n));
  ASSERT_EQ(n, static_cast<int>(view.size()));
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, view[i]) << i;
  }

  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(n));
  ASSERT_EQ(n, static_cast<int>(view.size()));
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, view[i]) << i;
  }

  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(n));
  ASSERT_EQ(n, buffer->size());
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, (*buffer)[i]) << i;
  }
  // source is at offset 10 + n
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(20));
  ASSERT_EQ(20, buffer->size());
  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(10 + n + i, (*buffer)[i]) << i;
  }
}

TEST_F(TestBufferedInputStreamBound, UnboundedPeek) {
  CreateExample(/*bounded=*/false);

  std::string_view view;
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(10));
  ASSERT_EQ(10, view.size());
  ASSERT_EQ(50, stream_->bytes_buffered());

  ASSERT_OK(stream_->Read(10));

  // Peek into buffered bytes
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(40));
  ASSERT_EQ(40, view.size());
  ASSERT_EQ(40, stream_->bytes_buffered());
  ASSERT_EQ(50, stream_->buffer_size());

  // Peek past buffered bytes
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(41));
  ASSERT_EQ(41, view.size());
  ASSERT_EQ(41, stream_->bytes_buffered());
  ASSERT_EQ(51, stream_->buffer_size());

  // Peek to the end of the buffer
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(246));
  ASSERT_EQ(246, view.size());
  ASSERT_EQ(246, stream_->bytes_buffered());
  ASSERT_EQ(256, stream_->buffer_size());

  // Larger peek returns the same, expands the buffer, but there is no
  // more data to buffer
  ASSERT_OK_AND_ASSIGN(view, stream_->Peek(300));
  ASSERT_EQ(246, view.size());
  ASSERT_EQ(246, stream_->bytes_buffered());
  ASSERT_EQ(310, stream_->buffer_size());
}

TEST_F(TestBufferedInputStreamBound, OneByteReads) {
  for (int i = 0; i < stream_size_; ++i) {
    ASSERT_OK_AND_ASSIGN(auto buffer, stream_->Read(1));
    ASSERT_EQ(1, buffer->size());
    ASSERT_EQ(10 + i, (*buffer)[0]) << i;
  }
  // Stream exhausted
  ASSERT_OK_AND_ASSIGN(auto buffer, stream_->Read(1));
  ASSERT_EQ(0, buffer->size());
}

TEST_F(TestBufferedInputStreamBound, BufferExactlyExhausted) {
  // Test exhausting the buffer exactly then issuing further reads (PARQUET-1571).
  std::shared_ptr<Buffer> buffer;

  // source is at offset 10
  int64_t n = 10;
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(n));
  ASSERT_EQ(n, buffer->size());
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(10 + i, (*buffer)[i]) << i;
  }
  // source is at offset 20
  // Exhaust buffer exactly
  n = stream_->bytes_buffered();
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(n));
  ASSERT_EQ(n, buffer->size());
  for (int i = 0; i < n; i++) {
    ASSERT_EQ(20 + i, (*buffer)[i]) << i;
  }

  // source is at offset 20 + n
  // Read new buffer
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(10));
  ASSERT_EQ(10, buffer->size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(20 + n + i, (*buffer)[i]) << i;
  }

  // source is at offset 30 + n
  ASSERT_OK_AND_ASSIGN(buffer, stream_->Read(10));
  ASSERT_EQ(10, buffer->size());
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(30 + n + i, (*buffer)[i]) << i;
  }
}

// These tests exercise the buffering algorithm by checking the reads issued
// to the underlying raw stream.
class TestBufferedInputStreamChunk : public TestBufferedInputStream {
 public:
  void SetUp() { TestBufferedInputStream::SetUp(); }

  void TearDown() {
    buffered_ = nullptr;
    tracked_ = nullptr;
    raw_ = nullptr;
  }

  void MakeExample(int64_t buffer_size,
                   std::optional<int64_t> read_bound = std::nullopt) {
    test_data_ = kExample1;
    MemoryPool* pool = default_memory_pool();

    ASSERT_OK_AND_ASSIGN(auto file_out, FileOutputStream::Open(path_));
    ASSERT_OK(file_out->Write(test_data_));
    ASSERT_OK(file_out->Close());

    ASSERT_OK_AND_ASSIGN(auto file_in, ReadableFile::Open(path_));
    raw_ = file_in;
    tracked_ = TrackedRandomAccessFile::Make(dynamic_cast<RandomAccessFile*>(raw_.get()));
    ASSERT_OK_AND_ASSIGN(buffered_,
                         BufferedInputStream::Create(buffer_size, pool, tracked_,
                                                     read_bound.value_or(-1)));
  }

 protected:
  std::shared_ptr<TrackedRandomAccessFile> tracked_;
};

TEST_F(TestBufferedInputStreamChunk, NoRead) {
  const int64_t kBufferSize = 5;
  MakeExample(kBufferSize);

  EXPECT_TRUE(tracked_->get_read_ranges().empty());
}

TEST_F(TestBufferedInputStreamChunk, LargeRead) {
  const int64_t kBufferSize = 5;
  MakeExample(kBufferSize);

  // Read bytes greater than buffer_size would not buffer.
  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(6));
  AssertBufferEqual(*buf, kExample1.substr(0, 6));

  EXPECT_EQ(0, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, 6}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

TEST_F(TestBufferedInputStreamChunk, SmallReadThenLargeRead) {
  const int64_t kBufferSize = 5;
  MakeExample(kBufferSize);

  // Small read would trigger buffer the whole chunk
  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(1));
  AssertBufferEqual(*buf, kExample1.substr(0, 1));
  EXPECT_EQ(4, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, 5}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // Large read with pre-buffered will copy the
  // pre-buffered data first, then read the remaining without filling
  // the buffer.
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(20));
  AssertBufferEqual(*buf, kExample1.substr(1, 20));
  EXPECT_EQ(0, buffered_->bytes_buffered());
  read_ranges.push_back(ReadRange{5, 16});
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // Small read again
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(2));
  AssertBufferEqual(*buf, kExample1.substr(21, 2));
  EXPECT_EQ(kBufferSize - 2, buffered_->bytes_buffered());
  read_ranges.push_back(ReadRange{21, 5});
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

TEST_F(TestBufferedInputStreamChunk, BufferWholeChunk) {
  const int64_t kBufferSize = 5;
  MakeExample(kBufferSize);

  // Small read
  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(1));
  AssertBufferEqual(*buf, kExample1.substr(0, 1));
  EXPECT_EQ(kBufferSize - 1, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, 5}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // Whole read size is larger than buffer size, but raw read size would be
  // smaller than buffer size => buffer is filled anew
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(6));
  AssertBufferEqual(*buf, kExample1.substr(1, 6));
  EXPECT_EQ(kBufferSize * 2 - 1 - 6, buffered_->bytes_buffered());
  read_ranges.push_back(ReadRange{5, 5});
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

TEST_F(TestBufferedInputStreamChunk, BufferLargerThanFileSize) {
  const int64_t kFileSize = static_cast<int64_t>(kExample1.size());
  const int64_t kBufferSize = kFileSize + 10;
  MakeExample(kBufferSize);

  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(1));
  AssertBufferEqual(*buf, kExample1.substr(0, 1));
  EXPECT_EQ(kFileSize - 1, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, kBufferSize}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(kFileSize - 3));
  AssertBufferEqual(*buf, kExample1.substr(1, kFileSize - 3));
  EXPECT_EQ(2, buffered_->bytes_buffered());
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // Short read up to EOF
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(20));
  AssertBufferEqual(*buf, kExample1.substr(kFileSize - 2, 2));
  EXPECT_EQ(0, buffered_->bytes_buffered());
  read_ranges.push_back(ReadRange{kFileSize, kBufferSize});
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // EOF
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(20));
  AssertBufferEqual(*buf, "");
  EXPECT_EQ(0, buffered_->bytes_buffered());
  // (BufferedInputStream still tries to fetch more bytes)
  read_ranges.push_back(ReadRange{kFileSize, kBufferSize});
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

TEST_F(TestBufferedInputStreamChunk, BufferLargerThanReadBound) {
  const int64_t kBufferSize = 6;
  const int64_t kReadBound = 5;
  MakeExample(kBufferSize, kReadBound);

  // Small read
  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(1));
  AssertBufferEqual(*buf, kExample1.substr(0, 1));
  EXPECT_EQ(kReadBound - 1, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, 5}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // Longer read, but truncated due to the read bound
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(10));
  AssertBufferEqual(*buf, kExample1.substr(1, 4));
  EXPECT_EQ(0, buffered_->bytes_buffered());
  // Due to the read bound, no more row reads were issued
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // EOF
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(10));
  AssertBufferEqual(*buf, "");
  EXPECT_EQ(0, buffered_->bytes_buffered());
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

TEST_F(TestBufferedInputStreamChunk, BufferSmallerThanReadBound) {
  const int64_t kBufferSize = 3;
  const int64_t kReadBound = 5;
  MakeExample(kBufferSize, kReadBound);

  ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(6));
  AssertBufferEqual(*buf, kExample1.substr(0, 5));
  EXPECT_EQ(0, buffered_->bytes_buffered());
  std::vector<ReadRange> read_ranges = {ReadRange{0, 5}};
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);

  // EOF
  ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(10));
  AssertBufferEqual(*buf, "");
  EXPECT_EQ(0, buffered_->bytes_buffered());
  EXPECT_EQ(tracked_->get_read_ranges(), read_ranges);
}

class TestBufferedInputStreamRandom : public ::testing::Test {
 public:
  void MakeBuffered(int64_t data_size, int64_t buffer_size,
                    std::optional<int64_t> read_bound = std::nullopt) {
    buffer_size_ = buffer_size;
    data_.clear();
    data_.reserve(data_size);
    while (data_.size() < static_cast<size_t>(data_size)) {
      data_.append(kExample1, /*pos*/ 0,
                   /*count*/ static_cast<size_t>(data_size) - data_.size());
    }
    EXPECT_EQ(data_.size(), data_size);

    // Clear dependent streams in reverse order
    buffered_.reset();
    tracked_.reset();
    // Read from a copy of data, so that data_.substr() below doesn't invalidate it
    raw_ = BufferReader::FromString(data_);
    tracked_ = TrackedRandomAccessFile::Make(raw_.get());
    EXPECT_OK_AND_ASSIGN(buffered_,
                         BufferedInputStream::Create(buffer_size, default_memory_pool(),
                                                     tracked_, read_bound.value_or(-1)));
    if (read_bound) {
      data_ = data_.substr(0, *read_bound);
    }
  }

  void TestReads() {
    std::default_random_engine gen(/*seed*/ 42);
    std::uniform_int_distribution<int64_t> read_len_dist(1, 80);

    int64_t pos = 0;
    const int64_t size = static_cast<int64_t>(data_.size());
    while (pos < size) {
      const int64_t read_len = read_len_dist(gen);
      ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(read_len));
      AssertBufferEqual(*buf, std::string_view(data_).substr(pos, read_len));
      pos += buf->size();
    }

    // EOF was reached
    ASSERT_EQ(pos, size);

    // Number of reads should not be excessive given the buffer size
    int64_t max_reads = (size + buffer_size_ - 1) / buffer_size_;
    EXPECT_LE(tracked_->num_reads(), max_reads);
    const auto& read_ranges = tracked_->get_read_ranges();
    pos = 0;
    for (size_t i = 0; i < read_ranges.size(); ++i) {
      const auto cur = read_ranges[i];
      ASSERT_EQ(cur.offset, pos);
      // Never read less than buffer size bytes, except perhaps if fewer bytes remain
      ASSERT_GE(cur.length, std::min(buffer_size_, size - pos));
      // Bump actual position in the file
      pos += std::min(cur.length, size - pos);
    }

    // Further reads return an empty buffer
    ASSERT_OK_AND_ASSIGN(auto buf, buffered_->Read(buffer_size_ - 1));
    AssertBufferEqual(*buf, "");
    ASSERT_OK_AND_ASSIGN(buf, buffered_->Read(buffer_size_ + 1));
    AssertBufferEqual(*buf, "");
  }

 protected:
  std::string data_;
  int64_t buffer_size_;
  std::shared_ptr<RandomAccessFile> raw_;
  std::shared_ptr<TrackedRandomAccessFile> tracked_;
  std::shared_ptr<BufferedInputStream> buffered_;
};

TEST_F(TestBufferedInputStreamRandom, ReadsWithoutReadBound) {
  constexpr int kNumIters = 10;

  for (int i = 0; i < kNumIters; ++i) {
    MakeBuffered(/*data_size=*/3000, /*buffer_size=*/11);
    TestReads();
  }
}

TEST_F(TestBufferedInputStreamRandom, ReadsWithReadBound) {
  constexpr int kNumIters = 10;

  for (int i = 0; i < kNumIters; ++i) {
    MakeBuffered(/*data_size=*/3000, /*buffer_size=*/11, /*read_bound=*/2000);
    TestReads();
  }
}

}  // namespace arrow::io
