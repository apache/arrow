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

#ifndef ARROW_BUFFER_H
#define ARROW_BUFFER_H

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

// ----------------------------------------------------------------------
// Buffer classes

/// \class Buffer
/// \brief Object containing a pointer to a piece of contiguous memory with a
/// particular size.
///
/// Buffers have two related notions of length: size and capacity. Size is
/// the number of bytes that might have valid data. Capacity is the number
/// of bytes that were allocated for the buffer in total.
///
/// The Buffer base class does not own its memory, but subclasses often do.
///
/// The following invariant is always true: Size <= Capacity
class ARROW_EXPORT Buffer {
 public:
  /// \brief Construct from buffer and size without copying memory
  ///
  /// \param[in] data a memory buffer
  /// \param[in] size buffer size
  ///
  /// \note The passed memory must be kept alive through some other means
  Buffer(const uint8_t* data, int64_t size)
      : is_mutable_(false),
        data_(data),
        mutable_data_(NULLPTR),
        size_(size),
        capacity_(size) {}

  /// \brief Construct from std::string without copying memory
  ///
  /// \param[in] data a std::string object
  ///
  /// \note The std::string must stay alive for the lifetime of the Buffer, so
  /// temporary rvalue strings must be stored in an lvalue somewhere
  explicit Buffer(const std::string& data)
      : Buffer(reinterpret_cast<const uint8_t*>(data.c_str()),
               static_cast<int64_t>(data.size())) {}

  virtual ~Buffer() = default;

  /// An offset into data that is owned by another buffer, but we want to be
  /// able to retain a valid pointer to it even after other shared_ptr's to the
  /// parent buffer have been destroyed
  ///
  /// This method makes no assertions about alignment or padding of the buffer but
  /// in general we expected buffers to be aligned and padded to 64 bytes.  In the future
  /// we might add utility methods to help determine if a buffer satisfies this contract.
  Buffer(const std::shared_ptr<Buffer>& parent, const int64_t offset, const int64_t size)
      : Buffer(parent->data() + offset, size) {
    parent_ = parent;
  }

  bool is_mutable() const { return is_mutable_; }

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer& other, int64_t nbytes) const;

  /// Return true if both buffers are the same size and contain the same bytes
  bool Equals(const Buffer& other) const;

  /// Copy a section of the buffer into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes, MemoryPool* pool,
              std::shared_ptr<Buffer>* out) const;

  /// Copy a section of the buffer using the default memory pool into a new Buffer.
  Status Copy(const int64_t start, const int64_t nbytes,
              std::shared_ptr<Buffer>* out) const;

  /// Zero bytes in padding, i.e. bytes between size_ and capacity_.
  void ZeroPadding() {
#ifndef NDEBUG
    CheckMutable();
#endif
    memset(mutable_data_ + size_, 0, static_cast<size_t>(capacity_ - size_));
  }

  /// \brief Construct a new buffer that owns its memory from a std::string
  ///
  /// \param[in] data a std::string object
  /// \param[in] pool a memory pool
  /// \param[out] out the created buffer
  ///
  /// \return Status message
  static Status FromString(const std::string& data, MemoryPool* pool,
                           std::shared_ptr<Buffer>* out);

  /// \brief Construct a new buffer that owns its memory from a std::string
  /// using the default memory pool
  static Status FromString(const std::string& data, std::shared_ptr<Buffer>* out);

  /// \brief Construct an immutable buffer that takes ownership of the contents
  /// of an std::string
  /// \param[in] data an rvalue-reference of a string
  /// \return a new Buffer instance
  static std::shared_ptr<Buffer> FromString(std::string&& data);

  /// \brief Create buffer referencing typed memory with some length without
  /// copying
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(const T* data, SizeType length) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data),
                                    static_cast<int64_t>(sizeof(T) * length));
  }

  /// \brief Create buffer referencing std::vector with some length without
  /// copying
  /// \param[in] data the vector to be referenced. If this vector is changed,
  /// the buffer may become invalid
  /// \return a new shared_ptr<Buffer>
  template <typename T>
  static std::shared_ptr<Buffer> Wrap(const std::vector<T>& data) {
    return std::make_shared<Buffer>(reinterpret_cast<const uint8_t*>(data.data()),
                                    static_cast<int64_t>(sizeof(T) * data.size()));
  }

  /// \brief Copy buffer contents into a new std::string
  /// \return std::string
  /// \note Can throw std::bad_alloc if buffer is large
  std::string ToString() const;

  /// \brief Return a pointer to the buffer's data
  const uint8_t* data() const { return data_; }
  /// \brief Return a writable pointer to the buffer's data
  ///
  /// The buffer has to be mutable.  Otherwise, an assertion may be thrown
  /// or a null pointer may be returned.
  uint8_t* mutable_data() {
#ifndef NDEBUG
    CheckMutable();
#endif
    return mutable_data_;
  }

  /// \brief Return the buffer's size in bytes
  int64_t size() const { return size_; }

  /// \brief Return the buffer's capacity (number of allocated bytes)
  int64_t capacity() const { return capacity_; }

  std::shared_ptr<Buffer> parent() const { return parent_; }

 protected:
  bool is_mutable_;
  const uint8_t* data_;
  uint8_t* mutable_data_;
  int64_t size_;
  int64_t capacity_;

  // null by default, but may be set
  std::shared_ptr<Buffer> parent_;

  void CheckMutable() const;

 private:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Buffer);
};

/// \defgroup buffer-slicing-functions Functions for slicing buffers
///
/// @{

/// \brief Construct a view on a buffer at the given offset and length.
///
/// This function cannot fail and does not check for errors (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset,
                                                  const int64_t length) {
  return std::make_shared<Buffer>(buffer, offset, length);
}

/// \brief Construct a view on a buffer at the given offset, up to the buffer's end.
///
/// This function cannot fail and does not check for errors (except in debug builds)
static inline std::shared_ptr<Buffer> SliceBuffer(const std::shared_ptr<Buffer>& buffer,
                                                  const int64_t offset) {
  int64_t length = buffer->size() - offset;
  return SliceBuffer(buffer, offset, length);
}

/// \brief Like SliceBuffer, but construct a mutable buffer slice.
///
/// If the parent buffer is not mutable, behavior is undefined (it may abort
/// in debug builds).
ARROW_EXPORT
std::shared_ptr<Buffer> SliceMutableBuffer(const std::shared_ptr<Buffer>& buffer,
                                           const int64_t offset, const int64_t length);

/// @}

/// \class MutableBuffer
/// \brief A Buffer whose contents can be mutated. May or may not own its data.
class ARROW_EXPORT MutableBuffer : public Buffer {
 public:
  MutableBuffer(uint8_t* data, const int64_t size) : Buffer(data, size) {
    mutable_data_ = data;
    is_mutable_ = true;
  }

  MutableBuffer(const std::shared_ptr<Buffer>& parent, const int64_t offset,
                const int64_t size);

  /// \brief Create buffer referencing typed memory with some length
  /// \param[in] data the typed memory as C array
  /// \param[in] length the number of values in the array
  /// \return a new shared_ptr<Buffer>
  template <typename T, typename SizeType = int64_t>
  static std::shared_ptr<Buffer> Wrap(T* data, SizeType length) {
    return std::make_shared<MutableBuffer>(reinterpret_cast<uint8_t*>(data),
                                           static_cast<int64_t>(sizeof(T) * length));
  }

 protected:
  MutableBuffer() : Buffer(NULLPTR, 0) {}
};

/// \class ResizableBuffer
/// \brief A mutable buffer that can be resized
class ARROW_EXPORT ResizableBuffer : public MutableBuffer {
 public:
  /// Change buffer reported size to indicated size, allocating memory if
  /// necessary.  This will ensure that the capacity of the buffer is a multiple
  /// of 64 bytes as defined in Layout.md.
  /// Consider using ZeroPadding afterwards, in case you return buffer to a reader.
  ///
  /// @param shrink_to_fit On deactivating this option, the capacity of the Buffer won't
  /// decrease.
  virtual Status Resize(const int64_t new_size, bool shrink_to_fit = true) = 0;

  /// Ensure that buffer has enough memory allocated to fit the indicated
  /// capacity (and meets the 64 byte padding requirement in Layout.md).
  /// It does not change buffer's reported size and doesn't zero the padding.
  virtual Status Reserve(const int64_t new_capacity) = 0;

  template <class T>
  Status TypedResize(const int64_t new_nb_elements, bool shrink_to_fit = true) {
    return Resize(sizeof(T) * new_nb_elements, shrink_to_fit);
  }

  template <class T>
  Status TypedReserve(const int64_t new_nb_elements) {
    return Reserve(sizeof(T) * new_nb_elements);
  }

 protected:
  ResizableBuffer(uint8_t* data, int64_t size) : MutableBuffer(data, size) {}
};

/// \defgroup buffer-allocation-functions Functions for allocating buffers
///
/// @{

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed size mutable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(MemoryPool* pool, const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::shared_ptr<Buffer>* out);

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return Status message
ARROW_EXPORT
Status AllocateBuffer(const int64_t size, std::unique_ptr<Buffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from a memory pool, zero its padding.
///
/// \param[in] pool a memory pool
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(MemoryPool* pool, const int64_t size,
                               std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::shared_ptr<ResizableBuffer>* out);

/// \brief Allocate a resizeable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateResizableBuffer(const int64_t size, std::unique_ptr<ResizableBuffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from a memory pool
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(MemoryPool* pool, int64_t length,
                           std::shared_ptr<Buffer>* out);

/// \brief Allocate a zero-initialized bitmap buffer from the default memory pool
///
/// \param[in] length size in bits of bitmap to allocate
/// \param[out] out the resulting buffer
///
/// \return Status message
ARROW_EXPORT
Status AllocateEmptyBitmap(int64_t length, std::shared_ptr<Buffer>* out);

/// @}

// ----------------------------------------------------------------------
// Buffer builder classes

/// \class BufferBuilder
/// \brief A class for incrementally building a contiguous chunk of in-memory data
class BufferBuilder {
 public:
  explicit BufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : pool_(pool), data_(NULLPTR), capacity_(0), size_(0) {}

  /// \brief Resize the buffer to the nearest multiple of 64 bytes
  ///
  /// \param new_capacity the new capacity of the of the builder. Will be rounded
  /// up to a multiple of 64 bytes for padding
  /// \param shrink_to_fit if new capacity is smaller than the existing size,
  /// reallocate internal buffer. Set to false to avoid reallocations when
  /// shrinking the builder.
  /// \return Status
  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    // Resize(0) is a no-op
    if (new_capacity == 0) {
      return Status::OK();
    }
    int64_t old_capacity = capacity_;

    if (buffer_ == NULLPTR) {
      ARROW_RETURN_NOT_OK(AllocateResizableBuffer(pool_, new_capacity, &buffer_));
    } else {
      ARROW_RETURN_NOT_OK(buffer_->Resize(new_capacity, shrink_to_fit));
    }
    capacity_ = buffer_->capacity();
    data_ = buffer_->mutable_data();
    if (capacity_ > old_capacity) {
      memset(data_ + old_capacity, 0, capacity_ - old_capacity);
    }
    return Status::OK();
  }

  /// \brief Ensure that builder can accommodate the additional number of bytes
  /// without the need to perform allocations
  ///
  /// \param additional_bytes number of additional bytes to make space for
  /// \return Status
  Status Reserve(const int64_t additional_bytes) {
    return Resize(size_ + additional_bytes, false);
  }

  /// \brief Return a capacity expanded by a growth factor of 2
  static int64_t GrowByFactor(const int64_t min_capacity) {
    // If the capacity was not already a multiple of 2, do so here
    // TODO(emkornfield) doubling isn't great default allocation practice
    // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
    // for discussion
    return BitUtil::NextPower2(min_capacity);
  }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  Status Append(const void* data, const int64_t length) {
    ARROW_RETURN_NOT_OK(Resize(GrowByFactor(length + size_), false));
    UnsafeAppend(data, length);
    return Status::OK();
  }

  /// \brief Append copies of a value to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  Status Append(const int64_t num_copies, uint8_t value) {
    ARROW_RETURN_NOT_OK(Resize(GrowByFactor(num_copies + size_), false));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  /// \brief Append the given data to the buffer
  ///
  /// The buffer is automatically expanded if necessary.
  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& data) {
    constexpr auto nbytes = static_cast<int64_t>(NBYTES);
    ARROW_RETURN_NOT_OK(Resize(GrowByFactor(nbytes + size_), false));
    std::copy(data.cbegin(), data.cend(), data_ + size_);
    size_ += nbytes;
    return Status::OK();
  }

  // Advance pointer and zero out memory
  Status Advance(const int64_t length) { return Append(length, 0); }

  // Unsafe methods don't check existing size
  void UnsafeAppend(const void* data, const int64_t length) {
    memcpy(data_ + size_, data, static_cast<size_t>(length));
    size_ += length;
  }

  void UnsafeAppend(const int64_t num_copies, uint8_t value) {
    memset(data_ + size_, value, static_cast<size_t>(num_copies));
    size_ += num_copies;
  }

  /// \brief Return result of builder as a Buffer object.
  ///
  /// The builder is reset and can be reused afterwards.
  ///
  /// \param[out] out the finalized Buffer object
  /// \param shrink_to_fit if the buffer size is smaller than its capacity,
  /// reallocate to fit more tightly in memory. Set to false to avoid
  /// a reallocation, at the expense of potentially more memory consumption.
  /// \return Status
  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    ARROW_RETURN_NOT_OK(Resize(size_, shrink_to_fit));
    *out = buffer_;
    Reset();
    return Status::OK();
  }

  void Reset() {
    buffer_ = NULLPTR;
    capacity_ = size_ = 0;
  }

  int64_t capacity() const { return capacity_; }
  int64_t length() const { return size_; }
  const uint8_t* data() const { return data_; }
  uint8_t* mutable_data() { return data_; }

 private:
  std::shared_ptr<ResizableBuffer> buffer_;
  MemoryPool* pool_;
  uint8_t* data_;
  int64_t capacity_;
  int64_t size_;
};

template <typename T, typename Enable = void>
class TypedBufferBuilder;

/// \brief A BufferBuilder for building a buffer of arithmetic elements
template <typename T>
class TypedBufferBuilder<T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : bytes_builder_(pool) {}

  Status Append(T value) {
    return bytes_builder_.Append(reinterpret_cast<uint8_t*>(&value), sizeof(T));
  }

  Status Append(const T* values, int64_t num_elements) {
    return bytes_builder_.Append(reinterpret_cast<const uint8_t*>(values),
                                 num_elements * sizeof(T));
  }

  void Append(const int64_t num_copies, T value) {
    ARROW_RETURN_NOT_OK(Resize(GrowByFactor(num_copies + size_), false));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  void UnsafeAppend(T value) {
    bytes_builder_.UnsafeAppend(reinterpret_cast<uint8_t*>(&value), sizeof(T));
  }

  void UnsafeAppend(const T* values, int64_t num_elements) {
    bytes_builder_.UnsafeAppend(reinterpret_cast<const uint8_t*>(values),
                                num_elements * sizeof(T));
  }

  void UnsafeAppend(const int64_t num_copies, T value) {
    auto data = mutable_data() + length();
    bytes_builder_.UnsafeAppend(num_copies * sizeof(T), 0);
    for (const auto end = data + num_copies; data != end; ++data) {
      *data = value;
    }
  }

  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    return bytes_builder_.Resize(new_capacity * sizeof(T), shrink_to_fit);
  }

  Status Reserve(const int64_t additional_elements) {
    return bytes_builder_.Reserve(additional_elements * sizeof(T));
  }

  Status Advance(const int64_t length) {
    return bytes_builder_.Advance(length * sizeof(T));
  }

  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    return bytes_builder_.Finish(out, shrink_to_fit);
  }

  void Reset() { bytes_builder_.Reset(); }

  int64_t length() const { return bytes_builder_.length() / sizeof(T); }
  int64_t capacity() const { return bytes_builder_.capacity() / sizeof(T); }
  const T* data() const { return reinterpret_cast<const T*>(bytes_builder_.data()); }
  T* mutable_data() { return reinterpret_cast<T*>(bytes_builder_.mutable_data()); }

 private:
  BufferBuilder bytes_builder_;
};

/// \brief A BufferBuilder for building a buffer containing a bitmap
template <>
class TypedBufferBuilder<bool> {
 public:
  explicit TypedBufferBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : bytes_builder_(pool) {}

  Status Append(bool value) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + 1));
    UnsafeAppend(value);
    return Status::OK();
  }

  Status Append(const uint8_t* valid_bytes, int64_t num_elements) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + num_elements));
    UnsafeAppend(valid_bytes, num_elements);
    return Status::OK();
  }

  Status Append(const int64_t num_copies, bool value) {
    ARROW_RETURN_NOT_OK(ResizeWithGrowthFactor(bit_length_ + num_copies));
    UnsafeAppend(num_copies, value);
    return Status::OK();
  }

  void UnsafeAppend(bool value) {
    BitUtil::SetBitTo(mutable_data(), bit_length_, value);
    if (!value) {
      ++false_count_;
    }
    ++bit_length_;
  }

  void UnsafeAppend(const uint8_t* bytes, int64_t num_elements) {
    if (num_elements == 0) return;
    internal::FirstTimeBitmapWriter writer(mutable_data(), bit_length_, num_elements);
    for (int64_t i = 0; i != num_elements; ++i) {
      if (bytes[i]) {
        writer.Set();
      } else {
        writer.Clear();
        ++false_count_;
      }
      writer.Next();
    }
    writer.Finish();
    bit_length_ += num_elements;
  }

  void UnsafeAppend(const int64_t num_copies, bool value) {
    const int64_t new_length = bit_length_ + num_copies;
    BitUtil::SetBitsTo(mutable_data(), bit_length_, new_length, value);
    if (!value) {
      false_count_ += num_copies;
    }
    bit_length_ = new_length;
  }

  Status Resize(const int64_t new_capacity, bool shrink_to_fit = true) {
    const int64_t old_byte_capacity = bytes_builder_.capacity();
    const int64_t new_byte_capacity = BitUtil::BytesForBits(new_capacity);
    ARROW_RETURN_NOT_OK(bytes_builder_.Resize(new_byte_capacity, shrink_to_fit));
    if (new_byte_capacity > old_byte_capacity) {
      memset(mutable_data() + old_byte_capacity, 0,
             static_cast<size_t>(new_byte_capacity - old_byte_capacity));
    }
    return Status::OK();
  }

  Status Reserve(const int64_t additional_elements) {
    return Resize(bit_length_ + additional_elements, false);
  }

  Status Advance(const int64_t length) {
    bit_length_ += length;
    false_count_ += length;
    return ResizeWithGrowthFactor(bit_length_);
  }

  Status Finish(std::shared_ptr<Buffer>* out, bool shrink_to_fit = true) {
    bit_length_ = false_count_ = 0;
    return bytes_builder_.Finish(out, shrink_to_fit);
  }

  void Reset() {
    bytes_builder_.Reset();
    bit_length_ = false_count_ = 0;
  }

  int64_t length() const { return bit_length_; }
  int64_t capacity() const { return bytes_builder_.capacity() * 8; }
  const uint8_t* data() const { return bytes_builder_.data(); }
  uint8_t* mutable_data() { return bytes_builder_.mutable_data(); }
  int64_t false_count() const { return false_count_; }

 private:
  Status ResizeWithGrowthFactor(const int64_t min_capacity) {
    return Resize(BufferBuilder::GrowByFactor(min_capacity), false);
  }
  BufferBuilder bytes_builder_;
  int64_t bit_length_ = 0;
  int64_t false_count_ = 0;
};

}  // namespace arrow

#endif  // ARROW_BUFFER_H
