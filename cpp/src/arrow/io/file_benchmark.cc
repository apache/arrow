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

#include "arrow/filesystem/api.h"
#include "arrow/io/buffered.h"
#include "arrow/io/file.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/windows_compatibility.h"

#include "benchmark/benchmark.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <random>
#include <thread>
#include <valarray>

#ifdef _WIN32

#include <io.h>

#else

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#endif

namespace arrow {

std::string GetNullFile() {
#ifdef _WIN32
  return "NUL";
#else
  return "/dev/null";
#endif
}

const std::valarray<int64_t> small_sizes = {8, 24, 33, 1, 32, 192, 16, 40};
const std::valarray<int64_t> large_sizes = {8192, 100000};

constexpr int64_t kBufferSize = 4096;

#ifdef _WIN32

class BackgroundReader {
  // A class that reads data in the background from a file descriptor
  // (Windows implementation)

 public:
  static std::shared_ptr<BackgroundReader> StartReader(int fd) {
    std::shared_ptr<BackgroundReader> reader(new BackgroundReader(fd));
    reader->worker_.reset(new std::thread([=] { reader->LoopReading(); }));
    return reader;
  }
  void Stop() { ARROW_CHECK(SetEvent(event_)); }
  void Join() { worker_->join(); }

  ~BackgroundReader() {
    ABORT_NOT_OK(internal::FileClose(fd_));
    ARROW_CHECK(CloseHandle(event_));
  }

 protected:
  explicit BackgroundReader(int fd) : fd_(fd), total_bytes_(0) {
    file_handle_ = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    ARROW_CHECK_NE(file_handle_, INVALID_HANDLE_VALUE);
    event_ =
        CreateEvent(nullptr, /* bManualReset=*/TRUE, /* bInitialState=*/FALSE, nullptr);
    ARROW_CHECK_NE(event_, INVALID_HANDLE_VALUE);
  }

  void LoopReading() {
    const HANDLE handles[] = {file_handle_, event_};
    while (true) {
      DWORD ret = WaitForMultipleObjects(2, handles, /* bWaitAll=*/FALSE, INFINITE);
      ARROW_CHECK_NE(ret, WAIT_FAILED);
      if (ret == WAIT_OBJECT_0 + 1) {
        // Got stop request
        break;
      } else if (ret == WAIT_OBJECT_0) {
        // File ready for reading
        total_bytes_ += *internal::FileRead(fd_, buffer_, buffer_size_);
      } else {
        ARROW_LOG(FATAL) << "Unexpected WaitForMultipleObjects return value " << ret;
      }
    }
  }

  int fd_;
  HANDLE file_handle_, event_;
  int64_t total_bytes_;

  static const int64_t buffer_size_ = 16384;
  uint8_t buffer_[buffer_size_];

  std::unique_ptr<std::thread> worker_;
};

#else

class BackgroundReader {
  // A class that reads data in the background from a file descriptor
  // (Unix implementation)

 public:
  static std::shared_ptr<BackgroundReader> StartReader(int fd) {
    std::shared_ptr<BackgroundReader> reader(new BackgroundReader(fd));
    reader->worker_.reset(new std::thread([=] { reader->LoopReading(); }));
    return reader;
  }
  void Stop() {
    const uint8_t data[] = "x";
    ABORT_NOT_OK(internal::FileWrite(wakeup_w_, data, 1));
  }
  void Join() { worker_->join(); }

  ~BackgroundReader() {
    for (int fd : {fd_, wakeup_r_, wakeup_w_}) {
      ABORT_NOT_OK(internal::FileClose(fd));
    }
  }

 protected:
  explicit BackgroundReader(int fd) : fd_(fd), total_bytes_(0) {
    // Prepare self-pipe trick
    auto pipe = *internal::CreatePipe();
    wakeup_r_ = pipe.rfd;
    wakeup_w_ = pipe.wfd;
    // Put fd in non-blocking mode
    fcntl(fd, F_SETFL, O_NONBLOCK);
  }

  void LoopReading() {
    struct pollfd pollfds[2];
    pollfds[0].fd = fd_;
    pollfds[0].events = POLLIN;
    pollfds[1].fd = wakeup_r_;
    pollfds[1].events = POLLIN;
    while (true) {
      int ret = poll(pollfds, 2, -1 /* timeout */);
      if (ret < 1) {
        std::cerr << "poll() failed with code " << ret << "\n";
        abort();
      }
      if (pollfds[1].revents & POLLIN) {
        // We're done
        break;
      }
      if (!(pollfds[0].revents & POLLIN)) {
        continue;
      }
      auto result = internal::FileRead(fd_, buffer_, buffer_size_);
      // There could be a spurious wakeup followed by EAGAIN
      if (result.ok()) {
        total_bytes_ += *result;
      }
    }
  }

  int fd_, wakeup_r_, wakeup_w_;
  int64_t total_bytes_;

  static const int64_t buffer_size_ = 16384;
  uint8_t buffer_[buffer_size_];

  std::unique_ptr<std::thread> worker_;
};

#endif

// Set up a pipe with an OutputStream at one end and a BackgroundReader at
// the other end.
static void SetupPipeWriter(std::shared_ptr<io::OutputStream>* stream,
                            std::shared_ptr<BackgroundReader>* reader) {
  auto pipe = *internal::CreatePipe();
  *stream = *io::FileOutputStream::Open(pipe.wfd);
  *reader = BackgroundReader::StartReader(pipe.rfd);
}

static void BenchmarkStreamingWrites(benchmark::State& state,
                                     std::valarray<int64_t> sizes,
                                     io::OutputStream* stream,
                                     BackgroundReader* reader = nullptr) {
  const std::string datastr(*std::max_element(std::begin(sizes), std::end(sizes)), 'x');
  const void* data = datastr.data();
  const int64_t sum_sizes = sizes.sum();

  while (state.KeepRunning()) {
    for (const int64_t size : sizes) {
      ABORT_NOT_OK(stream->Write(data, size));
    }
  }
  // For Windows: need to close writer before joining reader thread.
  ABORT_NOT_OK(stream->Close());

  const int64_t total_bytes = static_cast<int64_t>(state.iterations()) * sum_sizes;
  state.SetBytesProcessed(total_bytes);

  if (reader != nullptr) {
    // Wake up and stop
    reader->Stop();
    reader->Join();
  }
}

// Benchmark writing to /dev/null
//
// This situation is irrealistic as the kernel likely doesn't
// copy the data at all, so we only measure small writes.

static void FileOutputStreamSmallWritesToNull(
    benchmark::State& state) {  // NOLINT non-const reference
  auto stream = *io::FileOutputStream::Open(GetNullFile());

  BenchmarkStreamingWrites(state, small_sizes, stream.get());
}

static void BufferedOutputStreamSmallWritesToNull(
    benchmark::State& state) {  // NOLINT non-const reference
  auto file = *io::FileOutputStream::Open(GetNullFile());

  auto buffered_file =
      *io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(), file);
  BenchmarkStreamingWrites(state, small_sizes, buffered_file.get());
}

// Benchmark writing a pipe
//
// This is slightly more realistic than the above

static void FileOutputStreamSmallWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  BenchmarkStreamingWrites(state, small_sizes, stream.get(), reader.get());
}

static void FileOutputStreamLargeWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  BenchmarkStreamingWrites(state, large_sizes, stream.get(), reader.get());
}

static void BufferedOutputStreamSmallWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  auto buffered_stream =
      *io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(), stream);
  BenchmarkStreamingWrites(state, small_sizes, buffered_stream.get(), reader.get());
}

static void BufferedOutputStreamLargeWritesToPipe(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  std::shared_ptr<BackgroundReader> reader;
  SetupPipeWriter(&stream, &reader);

  auto buffered_stream =
      *io::BufferedOutputStream::Create(kBufferSize, default_memory_pool(), stream);

  BenchmarkStreamingWrites(state, large_sizes, buffered_stream.get(), reader.get());
}

#ifdef _WIN32
#else

constexpr const char* kTestFile = "/home/pace/testfile";
// constexpr const char* kTestFile = "/media/pace/Extreme SSD3/arrow/testfile";

static void CreateTemporaryFile(int64_t nbytes) {
  std::default_random_engine gen(42);
  std::vector<uint8_t> data(nbytes);
  std::uniform_int_distribution<uint8_t> dist(0, std::numeric_limits<uint8_t>::max());
  std::generate(data.begin(), data.end(), [&]() { return dist(gen); });
  fs::LocalFileSystem fs;
  // Ok to fail if file does not exist
  ARROW_UNUSED(fs.DeleteFile(kTestFile));
  int fd = open(kTestFile, O_WRONLY | O_CREAT, 00666);
  assert(fd != 0);
  ASSERT_OK(internal::FileWrite(fd, &*data.begin(), nbytes));
  int ret = close(fd);
  assert(ret == 0);
  sync();
}

static void ClearTemporaryFileFromCache(int64_t nbytes) {
  sync();
  int fd = open(kTestFile, O_SYNC | O_RDONLY);
  assert(fd != 0);
  int ret = posix_fadvise(fd, 0, nbytes, POSIX_FADV_DONTNEED);
  assert(ret == 0);
  ret = close(fd);
  assert(ret == 0);
  sync();
}

static void EnsureTemporaryFileCached(int64_t nbytes) {
  ASSERT_OK_AND_ASSIGN(auto in, io::ReadableFile::Open(kTestFile));
  ASSERT_OK(in->Read(nbytes));
  ASSERT_OK(in->Close());
}

using IteratorFactory = std::function<Iterator<std::shared_ptr<Buffer> >(int64_t)>;

static void BenchmarkIteratorRead(benchmark::State& state, IteratorFactory it_factory,
                                  bool cached) {
  int64_t nbytes = state.range(0);
  int64_t block_size = state.range(1);
  CreateTemporaryFile(nbytes);
  if (cached > 0) {
    EnsureTemporaryFileCached(nbytes);
  }
  while (state.KeepRunning()) {
    state.PauseTiming();
    if (cached == 0) {
      ClearTemporaryFileFromCache(nbytes);
    }
    state.ResumeTiming();
    uint64_t accumulator = 0;
    auto it = it_factory(block_size);
    ASSERT_OK(it.Visit([&accumulator](std::shared_ptr<Buffer> buf) {
      uint64_t local_accumulator = 0;
      for (const uint8_t *it = buf->data(), *end = it + buf->size(); it != end; ++it) {
        if (*it == 0x17) {
          local_accumulator++;
        }
      }
      accumulator += local_accumulator;
      return Status::OK();
    }));
    benchmark::DoNotOptimize(accumulator);
  }

  state.SetBytesProcessed(state.iterations() * nbytes);
}

static IteratorFactory InputStreamFactory() {
  return [](int64_t block_size) {
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<io::InputStream> in,
                         io::ReadableFile::Open(kTestFile));
    EXPECT_OK_AND_ASSIGN(auto it, io::MakeInputStreamIterator(std::move(in), block_size));
    return it;
  };
}

static IteratorFactory FadviseFactory() {
  return [](int64_t block_size) {
    EXPECT_OK_AND_ASSIGN(auto in, io::ReadableFile::Open(kTestFile));
    EXPECT_OK_AND_ASSIGN(auto it,
                         io::MakeFadviseInputStreamIterator(std::move(in), block_size));
    return it;
  };
}

static IteratorFactory BufferedInputStreamFactory(int blksize) {
  return [blksize](int64_t block_size) {
    EXPECT_OK_AND_ASSIGN(std::shared_ptr<io::InputStream> in,
                         io::ReadableFile::Open(kTestFile));
    EXPECT_OK_AND_ASSIGN(
        auto buffered_in,
        io::BufferedInputStream::Create(blksize, default_memory_pool(), std::move(in)));
    EXPECT_OK_AND_ASSIGN(auto it,
                         io::MakeInputStreamIterator(std::move(buffered_in), block_size));
    return it;
  };
}

static void ColdReadFromInputStreamViaIterator(benchmark::State& state) {
  BenchmarkIteratorRead(state, InputStreamFactory(), false);
}

static void ColdReadFromReadableFileViaIterator(benchmark::State& state) {
  BenchmarkIteratorRead(state, FadviseFactory(), false);
}

static void ColdReadFromBufferedInputStreamViaIterator(benchmark::State& state) {
  int64_t blksize = state.range(2);
  BenchmarkIteratorRead(state, BufferedInputStreamFactory(blksize), false);
}

static void HotReadFromInputStreamViaIterator(benchmark::State& state) {
  BenchmarkIteratorRead(state, InputStreamFactory(), true);
}

static void HotReadFromReadableFileViaIterator(benchmark::State& state) {
  BenchmarkIteratorRead(state, FadviseFactory(), true);
}

static void HotReadFromBufferedInputStreamViaIterator(benchmark::State& state) {
  int64_t blksize = state.range(2);
  BenchmarkIteratorRead(state, BufferedInputStreamFactory(blksize), true);
}

#endif

// We use real time as we don't want to count CPU time spent in the
// BackgroundReader thread

BENCHMARK(FileOutputStreamSmallWritesToNull)->UseRealTime();
BENCHMARK(FileOutputStreamSmallWritesToPipe)->UseRealTime();
BENCHMARK(FileOutputStreamLargeWritesToPipe)->UseRealTime();

BENCHMARK(BufferedOutputStreamSmallWritesToNull)->UseRealTime();
BENCHMARK(BufferedOutputStreamSmallWritesToPipe)->UseRealTime();
BENCHMARK(BufferedOutputStreamLargeWritesToPipe)->UseRealTime();

#ifdef _WIN32
#else
BENCHMARK(ColdReadFromInputStreamViaIterator)
    ->Iterations(3)
    ->ArgsProduct({{1 << 26}, {1 << 4, 1 << 10, 1 << 14, 1 << 20}})
    ->ArgNames({"nbytes", "nread"})
    ->UseRealTime();
BENCHMARK(ColdReadFromReadableFileViaIterator)
    ->Iterations(3)
    ->ArgsProduct({{1 << 26}, {1 << 4, 1 << 10, 1 << 14, 1 << 20}})
    ->ArgNames({"nbytes", "nread"})
    ->UseRealTime();
BENCHMARK(ColdReadFromBufferedInputStreamViaIterator)
    ->Iterations(3)
    ->ArgsProduct({{1 << 26}, {1 << 4, 1 << 10, 1 << 14, 1 << 20}, {1 << 14, 1 << 20}})
    ->ArgNames({"nbytes", "nread", "blksz"})
    ->UseRealTime();
BENCHMARK(HotReadFromInputStreamViaIterator)
    ->ArgsProduct({{1 << 26}, {1 << 4, 1 << 10, 1 << 14, 1 << 20, 1 << 22}})
    ->ArgNames({"nbytes", "nread"})
    ->UseRealTime();
BENCHMARK(HotReadFromReadableFileViaIterator)
    ->ArgsProduct({{1 << 26}, {1 << 4, 1 << 10, 1 << 14, 1 << 20, 1 << 22}})
    ->ArgNames({"nbytes", "nread"})
    ->UseRealTime();
BENCHMARK(HotReadFromBufferedInputStreamViaIterator)
    ->ArgsProduct({{1 << 26},
                   {1 << 4, 1 << 10, 1 << 14, 1 << 20, 1 << 22},
                   {1 << 14, 1 << 20}})
    ->ArgNames({"nbytes", "nread", "blksz"})
    ->UseRealTime();
#endif

}  // namespace arrow
