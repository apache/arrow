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

#pragma once

#include <memory>
#include <vector>

#include "arrow/io/type_fwd.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace io {
struct ReadRange;
}

namespace ipc {

namespace internal {
/// \class IoRecordedRandomAccessFile
/// \brief An RandomAccessFile that doesn't perform real IO, but only save all the IO
/// operations it receives, including read operation's <offset, length>, for replaying
/// later
class ARROW_EXPORT IoRecordedRandomAccessFile : public io::RandomAccessFile {
 public:
  explicit IoRecordedRandomAccessFile(const int64_t file_size)
      : file_size_(file_size), position_(0) {}

  Status Close() override;

  Status Abort() override;

  /// \brief Return the position in this stream
  Result<int64_t> Tell() const override;

  /// \brief Return whether the stream is closed
  bool closed() const override;

  Status Seek(int64_t position) override;

  Result<int64_t> GetSize() override;

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

  Result<int64_t> Read(int64_t nbytes, void* out) override;

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override;

  const io::IOContext& io_context() const override;

  /// \brief Return a vector containing all the read operations this file receives, each
  /// read operation is represented as an arrow::io::ReadRange
  ///
  /// \return a vector
  const std::vector<io::ReadRange>& GetReadRanges() const;

 private:
  const int64_t file_size_;
  std::vector<io::ReadRange> read_ranges_;
  int64_t position_;
  bool closed_ = false;
  io::IOContext io_context_;
};

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
