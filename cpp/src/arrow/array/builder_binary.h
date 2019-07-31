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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/buffer-builder.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"  // IWYU pragma: export

namespace arrow {

constexpr int64_t kBinaryMemoryLimit = std::numeric_limits<int32_t>::max() - 1;

// ----------------------------------------------------------------------
// Binary and String

template <typename TYPE>
class BaseBinaryBuilder : public ArrayBuilder {
 public:
  using TypeClass = TYPE;
  using offset_type = typename TypeClass::offset_type;

  BaseBinaryBuilder(const std::shared_ptr<DataType>& type, MemoryPool* pool)
      : ArrayBuilder(type, pool), offsets_builder_(pool), value_data_builder_(pool) {}

  Status Append(const uint8_t* value, offset_type length) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    ARROW_RETURN_NOT_OK(AppendNextOffset());
    // Safety check for UBSAN.
    if (ARROW_PREDICT_TRUE(length > 0)) {
      ARROW_RETURN_NOT_OK(value_data_builder_.Append(value, length));
    }

    UnsafeAppendToBitmap(true);
    return Status::OK();
  }

  Status Append(const char* value, offset_type length) {
    return Append(reinterpret_cast<const uint8_t*>(value), length);
  }

  Status Append(util::string_view value) {
    return Append(value.data(), static_cast<offset_type>(value.size()));
  }

  Status AppendNulls(int64_t length) final {
    const int64_t num_bytes = value_data_builder_.length();
    if (ARROW_PREDICT_FALSE(num_bytes > memory_limit())) {
      return AppendOverflow(num_bytes);
    }
    ARROW_RETURN_NOT_OK(Reserve(length));
    for (int64_t i = 0; i < length; ++i) {
      offsets_builder_.UnsafeAppend(static_cast<offset_type>(num_bytes));
    }
    UnsafeAppendToBitmap(length, false);
    return Status::OK();
  }

  Status AppendNull() final {
    ARROW_RETURN_NOT_OK(AppendNextOffset());
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  /// \brief Append without checking capacity
  ///
  /// Offsets and data should have been presized using Reserve() and
  /// ReserveData(), respectively.
  void UnsafeAppend(const uint8_t* value, offset_type length) {
    UnsafeAppendNextOffset();
    value_data_builder_.UnsafeAppend(value, length);
    UnsafeAppendToBitmap(true);
  }

  void UnsafeAppend(const char* value, offset_type length) {
    UnsafeAppend(reinterpret_cast<const uint8_t*>(value), length);
  }

  void UnsafeAppend(const std::string& value) {
    UnsafeAppend(value.c_str(), static_cast<offset_type>(value.size()));
  }

  void UnsafeAppend(util::string_view value) {
    UnsafeAppend(value.data(), static_cast<offset_type>(value.size()));
  }

  void UnsafeAppendNull() {
    const int64_t num_bytes = value_data_builder_.length();
    offsets_builder_.UnsafeAppend(static_cast<offset_type>(num_bytes));
    UnsafeAppendToBitmap(false);
  }

  /// \brief Append a sequence of strings in one shot.
  ///
  /// \param[in] values a vector of strings
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const std::vector<std::string>& values,
                      const uint8_t* valid_bytes = NULLPTR) {
    std::size_t total_length = std::accumulate(
        values.begin(), values.end(), 0ULL,
        [](uint64_t sum, const std::string& str) { return sum + str.size(); });
    ARROW_RETURN_NOT_OK(Reserve(values.size()));
    ARROW_RETURN_NOT_OK(value_data_builder_.Reserve(total_length));
    ARROW_RETURN_NOT_OK(offsets_builder_.Reserve(values.size()));

    if (valid_bytes != NULLPTR) {
      for (std::size_t i = 0; i < values.size(); ++i) {
        UnsafeAppendNextOffset();
        if (valid_bytes[i]) {
          value_data_builder_.UnsafeAppend(
              reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size());
        }
      }
    } else {
      for (std::size_t i = 0; i < values.size(); ++i) {
        UnsafeAppendNextOffset();
        value_data_builder_.UnsafeAppend(
            reinterpret_cast<const uint8_t*>(values[i].data()), values[i].size());
      }
    }

    UnsafeAppendToBitmap(valid_bytes, values.size());
    return Status::OK();
  }

  /// \brief Append a sequence of nul-terminated strings in one shot.
  ///        If one of the values is NULL, it is processed as a null
  ///        value even if the corresponding valid_bytes entry is 1.
  ///
  /// \param[in] values a contiguous C array of nul-terminated char *
  /// \param[in] length the number of values to append
  /// \param[in] valid_bytes an optional sequence of bytes where non-zero
  /// indicates a valid (non-null) value
  /// \return Status
  Status AppendValues(const char** values, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR) {
    std::size_t total_length = 0;
    std::vector<std::size_t> value_lengths(length);
    bool have_null_value = false;
    for (int64_t i = 0; i < length; ++i) {
      if (values[i] != NULLPTR) {
        auto value_length = strlen(values[i]);
        value_lengths[i] = value_length;
        total_length += value_length;
      } else {
        have_null_value = true;
      }
    }
    ARROW_RETURN_NOT_OK(Reserve(length));
    ARROW_RETURN_NOT_OK(ReserveData(total_length));

    if (valid_bytes) {
      int64_t valid_bytes_offset = 0;
      for (int64_t i = 0; i < length; ++i) {
        UnsafeAppendNextOffset();
        if (valid_bytes[i]) {
          if (values[i]) {
            value_data_builder_.UnsafeAppend(reinterpret_cast<const uint8_t*>(values[i]),
                                             value_lengths[i]);
          } else {
            UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset,
                                 i - valid_bytes_offset);
            UnsafeAppendToBitmap(false);
            valid_bytes_offset = i + 1;
          }
        }
      }
      UnsafeAppendToBitmap(valid_bytes + valid_bytes_offset, length - valid_bytes_offset);
    } else {
      if (have_null_value) {
        std::vector<uint8_t> valid_vector(length, 0);
        for (int64_t i = 0; i < length; ++i) {
          UnsafeAppendNextOffset();
          if (values[i]) {
            value_data_builder_.UnsafeAppend(reinterpret_cast<const uint8_t*>(values[i]),
                                             value_lengths[i]);
            valid_vector[i] = 1;
          }
        }
        UnsafeAppendToBitmap(valid_vector.data(), length);
      } else {
        for (int64_t i = 0; i < length; ++i) {
          UnsafeAppendNextOffset();
          value_data_builder_.UnsafeAppend(reinterpret_cast<const uint8_t*>(values[i]),
                                           value_lengths[i]);
        }
        UnsafeAppendToBitmap(NULLPTR, length);
      }
    }
    return Status::OK();
  }

  void Reset() override {
    ArrayBuilder::Reset();
    offsets_builder_.Reset();
    value_data_builder_.Reset();
  }

  Status Resize(int64_t capacity) override {
    // XXX Why is this check necessary?  There is no reason to disallow, say,
    // binary arrays with more than 2**31 empty or null values.
    if (capacity > memory_limit()) {
      return Status::CapacityError("BinaryBuilder cannot reserve space for more than ",
                                   memory_limit(), " child elements, got ", capacity);
    }
    ARROW_RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

    // One more than requested for offsets
    ARROW_RETURN_NOT_OK(offsets_builder_.Resize(capacity + 1));
    return ArrayBuilder::Resize(capacity);
  }

  /// \brief Ensures there is enough allocated capacity to append the indicated
  /// number of bytes to the value data buffer without additional allocations
  Status ReserveData(int64_t elements) {
    const int64_t size = value_data_length() + elements;
    ARROW_RETURN_IF(size > memory_limit(),
                    Status::CapacityError("Cannot reserve capacity larger than ",
                                          memory_limit(), " bytes"));
    return (size > value_data_capacity()) ? value_data_builder_.Reserve(elements)
                                          : Status::OK();
  }

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    // Write final offset (values length)
    ARROW_RETURN_NOT_OK(AppendNextOffset());

    // These buffers' padding zeroed by BufferBuilder
    std::shared_ptr<Buffer> offsets, value_data, null_bitmap;
    ARROW_RETURN_NOT_OK(offsets_builder_.Finish(&offsets));
    ARROW_RETURN_NOT_OK(value_data_builder_.Finish(&value_data));
    ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

    *out = ArrayData::Make(type_, length_, {null_bitmap, offsets, value_data},
                           null_count_, 0);
    Reset();
    return Status::OK();
  }

  /// \return size of values buffer so far
  int64_t value_data_length() const { return value_data_builder_.length(); }
  /// \return capacity of values buffer
  int64_t value_data_capacity() const { return value_data_builder_.capacity(); }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i, offset_type* out_length) const {
    const offset_type* offsets = offsets_builder_.data();
    const auto offset = offsets[i];
    if (i == (length_ - 1)) {
      *out_length = static_cast<offset_type>(value_data_builder_.length()) - offset;
    } else {
      *out_length = offsets[i + 1] - offset;
    }
    return value_data_builder_.data() + offset;
  }

  /// Temporary access to a value.
  ///
  /// This view becomes invalid on the next modifying operation.
  util::string_view GetView(int64_t i) const {
    offset_type value_length;
    const uint8_t* value_data = GetValue(i, &value_length);
    return util::string_view(reinterpret_cast<const char*>(value_data), value_length);
  }

  // Cannot make this a static attribute because of linking issues
  static constexpr int64_t memory_limit() {
    return std::numeric_limits<offset_type>::max() - 1;
  }

 protected:
  TypedBufferBuilder<offset_type> offsets_builder_;
  TypedBufferBuilder<uint8_t> value_data_builder_;

  Status AppendOverflow(int64_t num_bytes) {
    return Status::CapacityError("array cannot contain more than ", memory_limit(),
                                 " bytes, have ", num_bytes);
  }

  Status AppendNextOffset() {
    const int64_t num_bytes = value_data_builder_.length();
    if (ARROW_PREDICT_FALSE(num_bytes > memory_limit())) {
      return AppendOverflow(num_bytes);
    }
    return offsets_builder_.Append(static_cast<offset_type>(num_bytes));
  }

  void UnsafeAppendNextOffset() {
    const int64_t num_bytes = value_data_builder_.length();
    offsets_builder_.UnsafeAppend(static_cast<offset_type>(num_bytes));
  }
};

/// \class BinaryBuilder
/// \brief Builder class for variable-length binary data
class ARROW_EXPORT BinaryBuilder : public BaseBinaryBuilder<BinaryType> {
 public:
  explicit BinaryBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<BinaryArray>* out) { return FinishTyped(out); }

 protected:
  using BaseBinaryBuilder::BaseBinaryBuilder;
};

/// \class StringBuilder
/// \brief Builder class for UTF8 strings
class ARROW_EXPORT StringBuilder : public BinaryBuilder {
 public:
  using BinaryBuilder::BinaryBuilder;
  explicit StringBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<StringArray>* out) { return FinishTyped(out); }
};

/// \class LargeBinaryBuilder
/// \brief Builder class for large variable-length binary data
class ARROW_EXPORT LargeBinaryBuilder : public BaseBinaryBuilder<LargeBinaryType> {
 public:
  explicit LargeBinaryBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<LargeBinaryArray>* out) { return FinishTyped(out); }

 protected:
  using BaseBinaryBuilder::BaseBinaryBuilder;
};

/// \class LargeStringBuilder
/// \brief Builder class for large UTF8 strings
class ARROW_EXPORT LargeStringBuilder : public LargeBinaryBuilder {
 public:
  using LargeBinaryBuilder::LargeBinaryBuilder;
  explicit LargeStringBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<LargeStringArray>* out) { return FinishTyped(out); }
};

// ----------------------------------------------------------------------
// FixedSizeBinaryBuilder

class ARROW_EXPORT FixedSizeBinaryBuilder : public ArrayBuilder {
 public:
  using TypeClass = FixedSizeBinaryType;

  FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                         MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  Status Append(const uint8_t* value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(value);
    return Status::OK();
  }

  Status Append(const char* value) {
    return Append(reinterpret_cast<const uint8_t*>(value));
  }

  Status Append(const util::string_view& view) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(view);
    return Status::OK();
  }

  Status Append(const std::string& s) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(s);
    return Status::OK();
  }

  template <size_t NBYTES>
  Status Append(const std::array<uint8_t, NBYTES>& value) {
    ARROW_RETURN_NOT_OK(Reserve(1));
    UnsafeAppend(
        util::string_view(reinterpret_cast<const char*>(value.data()), value.size()));
    return Status::OK();
  }

  Status AppendValues(const uint8_t* data, int64_t length,
                      const uint8_t* valid_bytes = NULLPTR);

  Status AppendNull() final;

  Status AppendNulls(int64_t length) final;

  void UnsafeAppend(const uint8_t* value) {
    UnsafeAppendToBitmap(true);
    if (ARROW_PREDICT_TRUE(byte_width_ > 0)) {
      byte_builder_.UnsafeAppend(value, byte_width_);
    }
  }

  void UnsafeAppend(util::string_view value) {
#ifndef NDEBUG
    CheckValueSize(static_cast<size_t>(value.size()));
#endif
    UnsafeAppend(reinterpret_cast<const uint8_t*>(value.data()));
  }

  void UnsafeAppendNull() {
    UnsafeAppendToBitmap(false);
    byte_builder_.UnsafeAdvance(byte_width_);
  }

  void Reset() override;
  Status Resize(int64_t capacity) override;
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<FixedSizeBinaryArray>* out) { return FinishTyped(out); }

  /// \return size of values buffer so far
  int64_t value_data_length() const { return byte_builder_.length(); }

  int32_t byte_width() const { return byte_width_; }

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  const uint8_t* GetValue(int64_t i) const;

  /// Temporary access to a value.
  ///
  /// This view becomes invalid on the next modifying operation.
  util::string_view GetView(int64_t i) const;

  static constexpr int64_t memory_limit() {
    return std::numeric_limits<int64_t>::max() - 1;
  }

 protected:
  int32_t byte_width_;
  BufferBuilder byte_builder_;

  /// Temporary access to a value.
  ///
  /// This pointer becomes invalid on the next modifying operation.
  uint8_t* GetMutableValue(int64_t i) {
    uint8_t* data_ptr = byte_builder_.mutable_data();
    return data_ptr + i * byte_width_;
  }

#ifndef NDEBUG
  void CheckValueSize(int64_t size);
#endif
};

// ----------------------------------------------------------------------
// Chunked builders: build a sequence of BinaryArray or StringArray that are
// limited to a particular size (to the upper limit of 2GB)

namespace internal {

class ARROW_EXPORT ChunkedBinaryBuilder {
 public:
  ChunkedBinaryBuilder(int32_t max_chunk_value_length,
                       MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  ChunkedBinaryBuilder(int32_t max_chunk_value_length, int32_t max_chunk_length,
                       MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  virtual ~ChunkedBinaryBuilder() = default;

  Status Append(const uint8_t* value, int32_t length) {
    if (ARROW_PREDICT_FALSE(length + builder_->value_data_length() >
                            max_chunk_value_length_)) {
      if (builder_->value_data_length() == 0) {
        // The current item is larger than max_chunk_size_;
        // this chunk will be oversize and hold *only* this item
        ARROW_RETURN_NOT_OK(builder_->Append(value, length));
        return NextChunk();
      }
      // The current item would cause builder_->value_data_length() to exceed
      // max_chunk_size_, so finish this chunk and append the current item to the next
      // chunk
      ARROW_RETURN_NOT_OK(NextChunk());
      return Append(value, length);
    }

    if (ARROW_PREDICT_FALSE(builder_->length() == max_chunk_length_)) {
      // The current item would cause builder_->value_data_length() to exceed
      // max_chunk_size_, so finish this chunk and append the current item to the next
      // chunk
      ARROW_RETURN_NOT_OK(NextChunk());
    }

    return builder_->Append(value, length);
  }

  Status Append(const util::string_view& value) {
    return Append(reinterpret_cast<const uint8_t*>(value.data()),
                  static_cast<int32_t>(value.size()));
  }

  Status AppendNull() {
    if (ARROW_PREDICT_FALSE(builder_->length() == max_chunk_length_)) {
      ARROW_RETURN_NOT_OK(NextChunk());
    }
    return builder_->AppendNull();
  }

  Status Reserve(int64_t values);

  virtual Status Finish(ArrayVector* out);

 protected:
  Status NextChunk();

  // maximum total character data size per chunk
  int64_t max_chunk_value_length_;

  // maximum elements allowed per chunk
  int64_t max_chunk_length_ = kListMaximumElements;

  // when Reserve() would cause builder_ to exceed its max_chunk_length_,
  // add to extra_capacity_ instead and wait to reserve until the next chunk
  int64_t extra_capacity_ = 0;

  std::unique_ptr<BinaryBuilder> builder_;
  std::vector<std::shared_ptr<Array>> chunks_;
};

class ARROW_EXPORT ChunkedStringBuilder : public ChunkedBinaryBuilder {
 public:
  using ChunkedBinaryBuilder::ChunkedBinaryBuilder;

  Status Finish(ArrayVector* out) override;
};

}  // namespace internal

}  // namespace arrow
