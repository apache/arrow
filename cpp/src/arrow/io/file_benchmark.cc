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

#include "arrow/io/buffered.h"
#include "arrow/io/file.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/windows_compatibility.h"

#include "benchmark/benchmark.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
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

using internal::FileDescriptor;
using internal::Pipe;

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
    ABORT_NOT_OK(internal::FileWrite(wakeup_pipe_.wfd.fd(), data, 1));
  }
  void Join() { worker_->join(); }

 protected:
  explicit BackgroundReader(int fd)
      : fd_(fd), wakeup_pipe_(*internal::CreatePipe()), total_bytes_(0) {
    // Put fd in non-blocking mode
    fcntl(fd, F_SETFL, O_NONBLOCK);
    // Note the wakeup pipe itself does not need to be non-blocking,
    // since we're not actually reading from it.
  }

  void LoopReading() {
    struct pollfd pollfds[2];
    pollfds[0].fd = fd_.fd();
    pollfds[0].events = POLLIN;
    pollfds[1].fd = wakeup_pipe_.rfd.fd();
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
      auto result = internal::FileRead(fd_.fd(), buffer_, buffer_size_);
      // There could be a spurious wakeup followed by EAGAIN
      if (result.ok()) {
        total_bytes_ += *result;
      }
    }
  }

  FileDescriptor fd_;
  Pipe wakeup_pipe_;
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
  *stream = *io::FileOutputStream::Open(pipe.wfd.Detach());
  *reader = BackgroundReader::StartReader(pipe.rfd.Detach());
}

static void BenchmarkStreamingWrites(benchmark::State& state,
                                     const std::valarray<int64_t>& sizes,
                                     io::OutputStream* stream,
                                     BackgroundReader* reader = nullptr) {
  const std::string datastr(sizes.max(), 'x');
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

// We use real time as we don't want to count CPU time spent in the
// BackgroundReader thread

BENCHMARK(FileOutputStreamSmallWritesToNull)->UseRealTime();
BENCHMARK(FileOutputStreamSmallWritesToPipe)->UseRealTime();
BENCHMARK(FileOutputStreamLargeWritesToPipe)->UseRealTime();

BENCHMARK(BufferedOutputStreamSmallWritesToNull)->UseRealTime();
BENCHMARK(BufferedOutputStreamSmallWritesToPipe)->UseRealTime();
BENCHMARK(BufferedOutputStreamLargeWritesToPipe)->UseRealTime();

}  // namespace arrow
