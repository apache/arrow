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

#include "arrow/io/transform.h"

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
#include "arrow/util/logging.h"

namespace arrow {
namespace io {

struct TransformInputStream::Impl {
  std::shared_ptr<InputStream> wrapped_;
  TransformInputStream::TransformFunc transform_;
  std::shared_ptr<Buffer> pending_;
  int64_t pos_ = 0;
  bool closed_ = false;

  Impl(std::shared_ptr<InputStream> wrapped,
       TransformInputStream::TransformFunc transform)
      : wrapped_(std::move(wrapped)), transform_(std::move(transform)) {}

  void Close() {
    closed_ = true;
    pending_.reset();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed file");
    }
    return Status::OK();
  }
};

TransformInputStream::TransformInputStream(std::shared_ptr<InputStream> wrapped,
                                           TransformInputStream::TransformFunc transform)
    : impl_(new Impl{std::move(wrapped), std::move(transform)}) {}

TransformInputStream::~TransformInputStream() {}

Status TransformInputStream::Close() {
  impl_->Close();
  return impl_->wrapped_->Close();
}

Status TransformInputStream::Abort() { return impl_->wrapped_->Abort(); }

bool TransformInputStream::closed() const { return impl_->closed_; }

Result<std::shared_ptr<Buffer>> TransformInputStream::Read(int64_t nbytes) {
  RETURN_NOT_OK(impl_->CheckClosed());

  ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes));
  ARROW_ASSIGN_OR_RAISE(auto bytes_read, this->Read(nbytes, buf->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buf->Resize(bytes_read, /*shrink_to_fit=*/true));
  }
  return std::shared_ptr<Buffer>(std::move(buf));
}

Result<int64_t> TransformInputStream::Read(int64_t nbytes, void* out) {
  RETURN_NOT_OK(impl_->CheckClosed());

  if (nbytes == 0) {
    return 0;
  }

  int64_t avail_size = 0;
  std::vector<std::shared_ptr<Buffer>> avail;
  if (impl_->pending_) {
    avail.push_back(impl_->pending_);
    avail_size += impl_->pending_->size();
  }
  // Accumulate enough transformed data to satisfy read
  while (avail_size < nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buf, impl_->wrapped_->Read(nbytes));
    const bool have_eof = (buf->size() == 0);
    // Even if EOF is met, let the transform function run a last time
    // (for example to flush internal buffers)
    ARROW_ASSIGN_OR_RAISE(buf, impl_->transform_(std::move(buf)));
    avail_size += buf->size();
    avail.push_back(std::move(buf));
    if (have_eof) {
      break;
    }
  }
  DCHECK(!avail.empty());

  // Coalesce buffer data
  uint8_t* out_data = reinterpret_cast<uint8_t*>(out);
  int64_t copied_bytes = 0;
  for (size_t i = 0; i < avail.size() - 1; ++i) {
    // All buffers except the last fit fully into `nbytes`
    const auto buf = std::move(avail[i]);
    DCHECK_LE(buf->size(), nbytes);
    memcpy(out_data, buf->data(), static_cast<size_t>(buf->size()));
    out_data += buf->size();
    nbytes -= buf->size();
    copied_bytes += buf->size();
  }
  {
    // Last buffer: splice into `out` and `pending_`
    const auto buf = std::move(avail.back());
    const int64_t to_copy = std::min(buf->size(), nbytes);
    memcpy(out_data, buf->data(), static_cast<size_t>(to_copy));
    copied_bytes += to_copy;
    if (buf->size() > to_copy) {
      impl_->pending_ = SliceBuffer(buf, to_copy);
    } else {
      impl_->pending_.reset();
    }
  }
  impl_->pos_ += copied_bytes;
  return copied_bytes;
}

Result<int64_t> TransformInputStream::Tell() const {
  RETURN_NOT_OK(impl_->CheckClosed());

  return impl_->pos_;
}

Result<std::shared_ptr<const KeyValueMetadata>> TransformInputStream::ReadMetadata() {
  RETURN_NOT_OK(impl_->CheckClosed());

  return impl_->wrapped_->ReadMetadata();
}

Future<std::shared_ptr<const KeyValueMetadata>> TransformInputStream::ReadMetadataAsync(
    const IOContext& io_context) {
  RETURN_NOT_OK(impl_->CheckClosed());

  return impl_->wrapped_->ReadMetadataAsync(io_context);
}

}  // namespace io
}  // namespace arrow
