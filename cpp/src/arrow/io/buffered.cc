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

#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

// ----------------------------------------------------------------------
// BufferedOutputStream implementation

class BufferedOutputStream::Impl {
 public:
  explicit Impl(std::shared_ptr<OutputStream> raw)
      : raw_(std::move(raw)),
        is_open_(true),
        buffer_pos_(0),
        buffer_size_(0),
        raw_pos_(-1) {}

  ~Impl() { DCHECK(Close().ok()); }

  Status Close() {
    std::lock_guard<std::mutex> guard(lock_);
    if (is_open_) {
      Status st = FlushUnlocked();
      is_open_ = false;
      RETURN_NOT_OK(raw_->Close());
      return st;
    }
    return Status::OK();
  }

  bool closed() const {
    std::lock_guard<std::mutex> guard(lock_);
    return !is_open_;
  }

  Status Tell(int64_t* position) const {
    std::lock_guard<std::mutex> guard(lock_);
    if (raw_pos_ == -1) {
      RETURN_NOT_OK(raw_->Tell(&raw_pos_));
      DCHECK_GE(raw_pos_, 0);
    }
    *position = raw_pos_ + buffer_pos_;
    return Status::OK();
  }

  Status Write(const void* data, int64_t nbytes) {
    std::lock_guard<std::mutex> guard(lock_);
    if (nbytes < 0) {
      return Status::Invalid("write count should be >= 0");
    }
    if (nbytes == 0) {
      return Status::OK();
    }
    if (nbytes + buffer_pos_ >= buffer_size_) {
      RETURN_NOT_OK(FlushUnlocked());
      DCHECK_EQ(buffer_pos_, 0);
      if (nbytes >= buffer_size_) {
        // Direct write
        return raw_->Write(data, nbytes);
      }
    }
    DCHECK_LE(buffer_pos_ + nbytes, buffer_size_);
    std::memcpy(buffer_data_ + buffer_pos_, data, nbytes);
    buffer_pos_ += nbytes;
    return Status::OK();
  }

  Status FlushUnlocked() {
    if (buffer_pos_ > 0) {
      // Invalidate cached raw pos
      raw_pos_ = -1;
      RETURN_NOT_OK(raw_->Write(buffer_data_, buffer_pos_));
      buffer_pos_ = 0;
    }
    return Status::OK();
  }

  Status Flush() {
    std::lock_guard<std::mutex> guard(lock_);
    return FlushUnlocked();
  }

  std::shared_ptr<OutputStream> raw() const { return raw_; }

  Status SetBufferSize(int64_t new_buffer_size) {
    std::lock_guard<std::mutex> guard(lock_);
    DCHECK_GT(new_buffer_size, 0);
    if (!buffer_) {
      RETURN_NOT_OK(AllocateResizableBuffer(new_buffer_size, &buffer_));
    } else {
      if (buffer_pos_ >= new_buffer_size) {
        // If the buffer is shrinking, first flush to the raw OutputStream
        RETURN_NOT_OK(FlushUnlocked());
      }
      RETURN_NOT_OK(buffer_->Resize(new_buffer_size));
    }
    buffer_data_ = reinterpret_cast<char*>(buffer_->mutable_data());
    buffer_pos_ = 0;
    buffer_size_ = new_buffer_size;
    return Status::OK();
  }

  int64_t buffer_size() const { return buffer_size_; }

 private:
  std::shared_ptr<OutputStream> raw_;
  bool is_open_;

  std::shared_ptr<ResizableBuffer> buffer_;
  char* buffer_data_;
  int64_t buffer_pos_;
  int64_t buffer_size_;
  mutable int64_t raw_pos_;
  mutable std::mutex lock_;
};

BufferedOutputStream::BufferedOutputStream(std::shared_ptr<OutputStream> raw)
    : impl_(new BufferedOutputStream::Impl(std::move(raw))) {}

Status BufferedOutputStream::Create(std::shared_ptr<OutputStream> raw,
                                    int64_t buffer_size,
                                    std::shared_ptr<BufferedOutputStream>* out) {
  auto result =
      std::shared_ptr<BufferedOutputStream>(new BufferedOutputStream(std::move(raw)));
  RETURN_NOT_OK(result->SetBufferSize(buffer_size));
  *out = std::move(result);
  return Status::OK();
}

BufferedOutputStream::~BufferedOutputStream() {}

Status BufferedOutputStream::SetBufferSize(int64_t new_buffer_size) {
  return impl_->SetBufferSize(new_buffer_size);
}

int64_t BufferedOutputStream::buffer_size() const { return impl_->buffer_size(); }

Status BufferedOutputStream::Close() { return impl_->Close(); }

bool BufferedOutputStream::closed() const { return impl_->closed(); }

Status BufferedOutputStream::Tell(int64_t* position) const {
  return impl_->Tell(position);
}

Status BufferedOutputStream::Write(const void* data, int64_t nbytes) {
  return impl_->Write(data, nbytes);
}

Status BufferedOutputStream::Flush() { return impl_->Flush(); }

std::shared_ptr<OutputStream> BufferedOutputStream::raw() const { return impl_->raw(); }

}  // namespace io
}  // namespace arrow
