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

#include "arrow/buffer.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/slice_util_internal.h"
#include "arrow/util/string.h"

namespace arrow {

Result<std::shared_ptr<Buffer>> Buffer::CopySlice(const int64_t start,
                                                  const int64_t nbytes,
                                                  MemoryPool* pool) const {
  // Sanity checks
  ARROW_CHECK_LE(start, size_);
  ARROW_CHECK_LE(nbytes, size_ - start);
  DCHECK_GE(nbytes, 0);

  ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateResizableBuffer(nbytes, pool));
  std::memcpy(new_buffer->mutable_data(), data() + start, static_cast<size_t>(nbytes));
  return std::move(new_buffer);
}

namespace {

Status CheckBufferSlice(const Buffer& buffer, int64_t offset, int64_t length) {
  return internal::CheckSliceParams(buffer.size(), offset, length, "buffer");
}

Status CheckBufferSlice(const Buffer& buffer, int64_t offset) {
  if (ARROW_PREDICT_FALSE(offset < 0)) {
    // Avoid UBSAN in subtraction below
    return Status::IndexError("Negative buffer slice offset");
  }
  return CheckBufferSlice(buffer, offset, buffer.size() - offset);
}

}  // namespace

Result<std::shared_ptr<Buffer>> SliceBufferSafe(const std::shared_ptr<Buffer>& buffer,
                                                int64_t offset) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset));
  return SliceBuffer(buffer, offset);
}

Result<std::shared_ptr<Buffer>> SliceBufferSafe(const std::shared_ptr<Buffer>& buffer,
                                                int64_t offset, int64_t length) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset, length));
  return SliceBuffer(buffer, offset, length);
}

Result<std::shared_ptr<Buffer>> SliceMutableBufferSafe(
    const std::shared_ptr<Buffer>& buffer, int64_t offset) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset));
  return SliceMutableBuffer(buffer, offset);
}

Result<std::shared_ptr<Buffer>> SliceMutableBufferSafe(
    const std::shared_ptr<Buffer>& buffer, int64_t offset, int64_t length) {
  RETURN_NOT_OK(CheckBufferSlice(*buffer, offset, length));
  return SliceMutableBuffer(buffer, offset, length);
}

std::string Buffer::ToHexString() {
  return HexEncode(data(), static_cast<size_t>(size()));
}

bool Buffer::Equals(const Buffer& other, const int64_t nbytes) const {
  return this == &other || (size_ >= nbytes && other.size_ >= nbytes &&
                            (data_ == other.data_ ||
                             !memcmp(data_, other.data_, static_cast<size_t>(nbytes))));
}

bool Buffer::Equals(const Buffer& other) const {
  return this == &other || (size_ == other.size_ &&
                            (data_ == other.data_ ||
                             !memcmp(data_, other.data_, static_cast<size_t>(size_))));
}

std::string Buffer::ToString() const {
  return std::string(reinterpret_cast<const char*>(data_), static_cast<size_t>(size_));
}

void Buffer::CheckMutable() const { DCHECK(is_mutable()) << "buffer not mutable"; }

void Buffer::CheckCPU() const {
  DCHECK(is_cpu()) << "not a CPU buffer (device: " << device()->ToString() << ")";
}

Result<std::shared_ptr<io::RandomAccessFile>> Buffer::GetReader(
    std::shared_ptr<Buffer> buf) {
  return buf->memory_manager_->GetBufferReader(buf);
}

Result<std::shared_ptr<io::OutputStream>> Buffer::GetWriter(std::shared_ptr<Buffer> buf) {
  if (!buf->is_mutable()) {
    return Status::Invalid("Expected mutable buffer");
  }
  return buf->memory_manager_->GetBufferWriter(buf);
}

Result<std::shared_ptr<Buffer>> Buffer::Copy(std::shared_ptr<Buffer> source,
                                             const std::shared_ptr<MemoryManager>& to) {
  return MemoryManager::CopyBuffer(source, to);
}

Result<std::unique_ptr<Buffer>> Buffer::CopyNonOwned(
    const Buffer& source, const std::shared_ptr<MemoryManager>& to) {
  return MemoryManager::CopyNonOwned(source, to);
}

Result<std::shared_ptr<Buffer>> Buffer::View(std::shared_ptr<Buffer> source,
                                             const std::shared_ptr<MemoryManager>& to) {
  return MemoryManager::ViewBuffer(source, to);
}

Result<std::shared_ptr<Buffer>> Buffer::ViewOrCopy(
    std::shared_ptr<Buffer> source, const std::shared_ptr<MemoryManager>& to) {
  auto maybe_buffer = MemoryManager::ViewBuffer(source, to);
  if (maybe_buffer.ok()) {
    return maybe_buffer;
  }
  return MemoryManager::CopyBuffer(source, to);
}

class StlStringBuffer : public Buffer {
 public:
  explicit StlStringBuffer(std::string data)
      : Buffer(nullptr, 0), input_(std::move(data)) {
    data_ = reinterpret_cast<const uint8_t*>(input_.c_str());
    size_ = static_cast<int64_t>(input_.size());
    capacity_ = size_;
  }

 private:
  std::string input_;
};

std::shared_ptr<Buffer> Buffer::FromString(std::string data) {
  return std::make_shared<StlStringBuffer>(std::move(data));
}

std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length) {
  return std::make_shared<MutableBuffer>(buffer, offset, length);
}

MutableBuffer::MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                             const int64_t size)
    : MutableBuffer(reinterpret_cast<uint8_t*>(parent->mutable_address()) + offset,
                    size) {
  DCHECK(parent->is_mutable()) << "Must pass mutable buffer";
  parent_ = parent;
}

Result<std::shared_ptr<Buffer>> AllocateBitmap(int64_t length, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto buf, AllocateBuffer(bit_util::BytesForBits(length), pool));
  // Zero out any trailing bits
  if (buf->size() > 0) {
    buf->mutable_data()[buf->size() - 1] = 0;
  }
  return std::move(buf);
}

Result<std::shared_ptr<Buffer>> AllocateEmptyBitmap(int64_t length, MemoryPool* pool) {
  return AllocateEmptyBitmap(length, kDefaultBufferAlignment, pool);
}

Result<std::shared_ptr<Buffer>> AllocateEmptyBitmap(int64_t length, int64_t alignment,
                                                    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto buf,
                        AllocateBuffer(bit_util::BytesForBits(length), alignment, pool));
  memset(buf->mutable_data(), 0, static_cast<size_t>(buf->size()));
  return std::move(buf);
}

Status AllocateEmptyBitmap(int64_t length, std::shared_ptr<Buffer>* out) {
  return AllocateEmptyBitmap(length).Value(out);
}

Result<std::shared_ptr<Buffer>> ConcatenateBuffers(
    const std::vector<std::shared_ptr<Buffer>>& buffers, MemoryPool* pool) {
  int64_t out_length = 0;
  for (const auto& buffer : buffers) {
    out_length += buffer->size();
  }
  ARROW_ASSIGN_OR_RAISE(auto out, AllocateBuffer(out_length, pool));
  auto out_data = out->mutable_data();
  for (const auto& buffer : buffers) {
    std::memcpy(out_data, buffer->data(), buffer->size());
    out_data += buffer->size();
  }
  return std::move(out);
}

}  // namespace arrow
