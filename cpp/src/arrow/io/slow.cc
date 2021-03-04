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

#include "arrow/io/slow.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <random>
#include <thread>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

// Multiply the average by this ratio to get the intended standard deviation
static constexpr double kStandardDeviationRatio = 0.1;

class LatencyGeneratorImpl : public LatencyGenerator {
 public:
  ~LatencyGeneratorImpl() override = default;

  LatencyGeneratorImpl(double average_latency, int32_t seed)
      : gen_(static_cast<decltype(gen_)::result_type>(seed)),
        latency_dist_(average_latency, average_latency * kStandardDeviationRatio) {}

  double NextLatency() override {
    // std::random distributions are unlikely to be thread-safe, and
    // a RandomAccessFile may be called from multiple threads
    std::lock_guard<std::mutex> lock(mutex_);
    return std::max<double>(0.0, latency_dist_(gen_));
  }

 private:
  std::default_random_engine gen_;
  std::normal_distribution<double> latency_dist_;
  std::mutex mutex_;
};

LatencyGenerator::~LatencyGenerator() {}

void LatencyGenerator::Sleep() {
  std::this_thread::sleep_for(std::chrono::duration<double>(NextLatency()));
}

std::shared_ptr<LatencyGenerator> LatencyGenerator::Make(double average_latency) {
  return std::make_shared<LatencyGeneratorImpl>(
      average_latency, static_cast<int32_t>(::arrow::internal::GetRandomSeed()));
}

std::shared_ptr<LatencyGenerator> LatencyGenerator::Make(double average_latency,
                                                         int32_t seed) {
  return std::make_shared<LatencyGeneratorImpl>(average_latency, seed);
}

//////////////////////////////////////////////////////////////////////////
// SlowInputStream implementation

SlowInputStream::~SlowInputStream() { internal::CloseFromDestructor(this); }

Status SlowInputStream::Close() { return stream_->Close(); }

Status SlowInputStream::Abort() { return stream_->Abort(); }

bool SlowInputStream::closed() const { return stream_->closed(); }

Result<int64_t> SlowInputStream::Tell() const { return stream_->Tell(); }

Result<int64_t> SlowInputStream::Read(int64_t nbytes, void* out) {
  latencies_->Sleep();
  return stream_->Read(nbytes, out);
}

Result<std::shared_ptr<Buffer>> SlowInputStream::Read(int64_t nbytes) {
  latencies_->Sleep();
  return stream_->Read(nbytes);
}

Result<util::string_view> SlowInputStream::Peek(int64_t nbytes) {
  return stream_->Peek(nbytes);
}

//////////////////////////////////////////////////////////////////////////
// SlowRandomAccessFile implementation

SlowRandomAccessFile::~SlowRandomAccessFile() { internal::CloseFromDestructor(this); }

Status SlowRandomAccessFile::Close() { return stream_->Close(); }

Status SlowRandomAccessFile::Abort() { return stream_->Abort(); }

bool SlowRandomAccessFile::closed() const { return stream_->closed(); }

Result<int64_t> SlowRandomAccessFile::GetSize() { return stream_->GetSize(); }

Status SlowRandomAccessFile::Seek(int64_t position) { return stream_->Seek(position); }

Result<int64_t> SlowRandomAccessFile::Tell() const { return stream_->Tell(); }

Result<int64_t> SlowRandomAccessFile::Read(int64_t nbytes, void* out) {
  latencies_->Sleep();
  return stream_->Read(nbytes, out);
}

Result<std::shared_ptr<Buffer>> SlowRandomAccessFile::Read(int64_t nbytes) {
  latencies_->Sleep();
  return stream_->Read(nbytes);
}

Result<int64_t> SlowRandomAccessFile::ReadAt(int64_t position, int64_t nbytes,
                                             void* out) {
  latencies_->Sleep();
  return stream_->ReadAt(position, nbytes, out);
}

Result<std::shared_ptr<Buffer>> SlowRandomAccessFile::ReadAt(int64_t position,
                                                             int64_t nbytes) {
  latencies_->Sleep();
  return stream_->ReadAt(position, nbytes);
}

Result<util::string_view> SlowRandomAccessFile::Peek(int64_t nbytes) {
  return stream_->Peek(nbytes);
}

}  // namespace io
}  // namespace arrow
