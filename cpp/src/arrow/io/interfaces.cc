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

#include "arrow/io/interfaces.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_view.h"

namespace arrow {
namespace io {

FileInterface::~FileInterface() = default;

Status InputStream::Advance(int64_t nbytes) {
  std::shared_ptr<Buffer> temp;
  return Read(nbytes, &temp);
}

Status InputStream::Peek(int64_t ARROW_ARG_UNUSED(nbytes),
                         util::string_view* ARROW_ARG_UNUSED(out)) {
  return Status::NotImplemented("Peek not implemented");
}

bool InputStream::supports_zero_copy() const { return false; }

struct RandomAccessFile::RandomAccessFileImpl {
  std::mutex lock_;
};

RandomAccessFile::~RandomAccessFile() = default;

RandomAccessFile::RandomAccessFile()
    : interface_impl_(new RandomAccessFile::RandomAccessFileImpl()) {}

Status RandomAccessFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                                void* out) {
  std::lock_guard<std::mutex> lock(interface_impl_->lock_);
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, bytes_read, out);
}

Status RandomAccessFile::ReadAt(int64_t position, int64_t nbytes,
                                std::shared_ptr<Buffer>* out) {
  std::lock_guard<std::mutex> lock(interface_impl_->lock_);
  RETURN_NOT_OK(Seek(position));
  return Read(nbytes, out);
}

Status Writable::Write(const std::string& data) {
  return Write(data.c_str(), static_cast<int64_t>(data.size()));
}

Status Writable::Flush() { return Status::OK(); }

class FileSegmentReader : public InputStream {
 public:
  FileSegmentReader(std::shared_ptr<RandomAccessFile> file, int64_t file_offset,
                    int64_t nbytes)
      : file_(std::move(file)), position_(0), file_offset_(file_offset), nbytes_(nbytes) {
    FileInterface::set_mode(FileMode::READ);
  }

  Status Close() override { return Status::OK(); }

  Status Tell(int64_t* position) const override {
    *position = position_;
    return Status::OK();
  }

  bool closed() const override { return false; }

  Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override {
    int64_t bytes_to_read = std::min(nbytes, nbytes_ - position_);
    RETURN_NOT_OK(
        file_->ReadAt(file_offset_ + position_, bytes_to_read, bytes_read, out));
    position_ += *bytes_read;
    return Status::OK();
  }

  Status Read(int64_t nbytes, std::shared_ptr<Buffer>* out) override {
    int64_t bytes_to_read = std::min(nbytes, nbytes_ - position_);
    RETURN_NOT_OK(file_->ReadAt(file_offset_ + position_, bytes_to_read, out));
    position_ += (*out)->size();
    return Status::OK();
  }

 private:
  std::shared_ptr<RandomAccessFile> file_;
  int64_t position_;
  int64_t file_offset_;
  int64_t nbytes_;
};

Status RandomAccessFile::GetStream(std::shared_ptr<RandomAccessFile> file,
                                   int64_t file_offset, int64_t nbytes,
                                   std::shared_ptr<InputStream>* out) {
  DCHECK_GE(file_offset, 0);
  int64_t size = -1;
  RETURN_NOT_OK(file->GetSize(&size));
  nbytes = std::min(size - file_offset, nbytes);
  *out = std::make_shared<FileSegmentReader>(std::move(file), file_offset, nbytes);
  return Status::OK();
}

}  // namespace io
}  // namespace arrow
