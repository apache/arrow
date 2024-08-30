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

#include <cstdint>

#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

struct ARROW_EXPORT KeyEncoder {
  // the first byte of an encoded key is used to indicate nullity
  static constexpr bool kExtraByteForNull = true;

  static constexpr uint8_t kNullByte = 1;
  static constexpr uint8_t kValidByte = 0;

  virtual ~KeyEncoder() = default;

  // Increment the values in the lengths array by the length of the encoded key for the corresponding value in the given column.
  //
  // Generally if Encoder is for a fixed-width type, the length of the encoded key
  // would add ExtraByteForNull + byte_width.
  // If Encoder is for a variable-width type, the length would add ExtraByteForNull +
  // sizeof(Offset) + buffer_size.
  // If Encoder is for null type, the length would add 0.
  virtual void AddLength(const ExecValue& value, int64_t batch_length,
                         int32_t* lengths) = 0;

  // Increment the length by the length of an encoded null value.
  // It's a special case for AddLength like `AddLength(Null-Scalar, 1, lengths)`.
  virtual void AddLengthNull(int32_t* length) = 0;

  // Encode the column into the encoded_bytes, which is an array of pointers to each row buffer.
  //
  // If value is an array, the array-size should be batch_length.
  // If value is a scalar, the value would repeat batch_length times.
  // NB: The pointers in the encoded_bytes will be advanced as values being encoded into.
  virtual Status Encode(const ExecValue&, int64_t batch_length,
                        uint8_t** encoded_bytes) = 0;

  // Encode a null value into the encoded_bytes, which is an array of pointers to each row buffer.
  //
  // It's a special case for Encode like `Encode(Null-Scalar, 1, encoded_bytes)`.
  // NB: The pointers in the encoded_bytes will be advanced as values being encoded into.
  virtual void EncodeNull(uint8_t** encoded_bytes) = 0;

  // Decode the encoded key from the encoded_bytes, which is an array of pointers to each row buffer, into an ArrayData.
  //
  // NB: The pointers in the encoded_bytes will be advanced as values being decoded from.
  virtual Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes,
                                                    int32_t length, MemoryPool*) = 0;

  // extract the null bitmap from the leading nullity bytes of encoded keys
  static Status DecodeNulls(MemoryPool* pool, int32_t length, uint8_t** encoded_bytes,
                            std::shared_ptr<Buffer>* null_bitmap, int32_t* null_count);

  static bool IsNull(const uint8_t* encoded_bytes) {
    return encoded_bytes[0] == kNullByte;
  }
};

struct ARROW_EXPORT BooleanKeyEncoder : KeyEncoder {
  static constexpr int kByteWidth = 1;

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override;

  void AddLengthNull(int32_t* length) override;

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override;

  void EncodeNull(uint8_t** encoded_bytes) override;

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override;
};

struct ARROW_EXPORT FixedWidthKeyEncoder : KeyEncoder {
  explicit FixedWidthKeyEncoder(std::shared_ptr<DataType> type)
      : type_(std::move(type)),
        byte_width_(checked_cast<const FixedWidthType&>(*type_).bit_width() / 8) {}

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override;

  void AddLengthNull(int32_t* length) override;

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override;

  void EncodeNull(uint8_t** encoded_bytes) override;

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override;

  std::shared_ptr<DataType> type_;
  int byte_width_;
};

struct ARROW_EXPORT DictionaryKeyEncoder : FixedWidthKeyEncoder {
  DictionaryKeyEncoder(std::shared_ptr<DataType> type, MemoryPool* pool)
      : FixedWidthKeyEncoder(std::move(type)), pool_(pool) {}

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override;

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override;

  MemoryPool* pool_;
  std::shared_ptr<Array> dictionary_;
};

template <typename T>
struct ARROW_EXPORT VarLengthKeyEncoder : KeyEncoder {
  using Offset = typename T::offset_type;

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override {
    if (data.is_array()) {
      int64_t i = 0;
      VisitArraySpanInline<T>(
          data.array,
          [&](std::string_view bytes) {
            lengths[i++] +=
                kExtraByteForNull + sizeof(Offset) + static_cast<int32_t>(bytes.size());
          },
          [&] { lengths[i++] += kExtraByteForNull + sizeof(Offset); });
    } else {
      const Scalar& scalar = *data.scalar;
      const int32_t buffer_size =
          scalar.is_valid ? static_cast<int32_t>(UnboxScalar<T>::Unbox(scalar).size())
                          : 0;
      for (int64_t i = 0; i < batch_length; i++) {
        lengths[i] += kExtraByteForNull + sizeof(Offset) + buffer_size;
      }
    }
  }

  void AddLengthNull(int32_t* length) override {
    *length += kExtraByteForNull + sizeof(Offset);
  }

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override {
    auto handle_next_valid_value = [&encoded_bytes](std::string_view bytes) {
      auto& encoded_ptr = *encoded_bytes++;
      *encoded_ptr++ = kValidByte;
      util::SafeStore(encoded_ptr, static_cast<Offset>(bytes.size()));
      encoded_ptr += sizeof(Offset);
      memcpy(encoded_ptr, bytes.data(), bytes.size());
      encoded_ptr += bytes.size();
    };
    auto handle_next_null_value = [&encoded_bytes]() {
      auto& encoded_ptr = *encoded_bytes++;
      *encoded_ptr++ = kNullByte;
      util::SafeStore(encoded_ptr, static_cast<Offset>(0));
      encoded_ptr += sizeof(Offset);
    };
    if (data.is_array()) {
      VisitArraySpanInline<T>(data.array, handle_next_valid_value,
                              handle_next_null_value);
    } else {
      const auto& scalar = data.scalar_as<BaseBinaryScalar>();
      if (scalar.is_valid) {
        const auto bytes = std::string_view{*scalar.value};
        for (int64_t i = 0; i < batch_length; i++) {
          handle_next_valid_value(bytes);
        }
      } else {
        for (int64_t i = 0; i < batch_length; i++) {
          handle_next_null_value();
        }
      }
    }
    return Status::OK();
  }

  void EncodeNull(uint8_t** encoded_bytes) override {
    auto& encoded_ptr = *encoded_bytes;
    *encoded_ptr++ = kNullByte;
    util::SafeStore(encoded_ptr, static_cast<Offset>(0));
    encoded_ptr += sizeof(Offset);
  }

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    std::shared_ptr<Buffer> null_buf;
    int32_t null_count;
    ARROW_RETURN_NOT_OK(DecodeNulls(pool, length, encoded_bytes, &null_buf, &null_count));

    Offset length_sum = 0;
    for (int32_t i = 0; i < length; ++i) {
      length_sum += util::SafeLoadAs<Offset>(encoded_bytes[i]);
    }

    ARROW_ASSIGN_OR_RAISE(auto offset_buf,
                          AllocateBuffer(sizeof(Offset) * (1 + length), pool));
    ARROW_ASSIGN_OR_RAISE(auto key_buf, AllocateBuffer(length_sum));

    auto raw_offsets = reinterpret_cast<Offset*>(offset_buf->mutable_data());
    auto raw_keys = key_buf->mutable_data();

    Offset current_offset = 0;
    for (int32_t i = 0; i < length; ++i) {
      raw_offsets[i] = current_offset;

      auto key_length = util::SafeLoadAs<Offset>(encoded_bytes[i]);
      encoded_bytes[i] += sizeof(Offset);

      memcpy(raw_keys + current_offset, encoded_bytes[i], key_length);
      encoded_bytes[i] += key_length;

      current_offset += key_length;
    }
    raw_offsets[length] = current_offset;

    return ArrayData::Make(
        type_, length, {std::move(null_buf), std::move(offset_buf), std::move(key_buf)},
        null_count);
  }

  explicit VarLengthKeyEncoder(std::shared_ptr<DataType> type) : type_(std::move(type)) {}

  std::shared_ptr<DataType> type_;
};

struct ARROW_EXPORT NullKeyEncoder : KeyEncoder {
  void AddLength(const ExecValue&, int64_t batch_length, int32_t* lengths) override {}

  void AddLengthNull(int32_t* length) override {}

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override {
    return Status::OK();
  }

  void EncodeNull(uint8_t** encoded_bytes) override {}

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override {
    return ArrayData::Make(null(), length, {NULLPTR}, length);
  }
};

/// RowEncoder encodes ExecSpan to a variable length byte sequence
/// created by concatenating the encoded form of each column. The encoding
/// for each column depends on its data type.
///
/// This is used to encode columns into row-major format, which will be beneficial for grouping and joining operations.
///
/// Unlike DuckDB and arrow-rs, currently this row format can not help
/// sortings because the row-format is uncomparable.
///
/// The row format is composed of the the KeyColumn encodings for each,
/// and the column is encoded as follows:
/// 1. A null byte for each column, indicating whether the column is null.
///    "1" for null, "0" for non-null.
/// 2. The "fixed width" encoding for the column, it would exist whether
///    the column is null or not.
/// 3. The "variable width" encoding for the column, it would exists only
///    for non-null string/binary columns.
/// 4. Specially, if all columns in a row are null, the caller may decide to refer to kRowIdForNulls instead of actually encoding/decoding it. See the comment for encoded_nulls_.
///
/// ## Null Type
///
/// Null Type is a special case, it doesn't occupy any space in the encoded row.
///
/// ## Fixed Width Type
///
/// Fixed Width Type is encoded as a fixed-width byte sequence. For example:
/// ```
/// Int8: [5, null, 6]
/// ```
/// Would be encoded as [0 5 1 0 0 6].
///
/// ### Dictionary Type
///
/// Dictionary Type is encoded as a fixed-width byte sequence using dictionary
/// indices, the dictionary should be identical for all rows.
///
/// ## Variable Width Type
///
/// Variable Width Type is encoded as:
/// [null byte, variable-byte length, variable bytes]. For example:
///
/// String "abc" Would be encoded as:
/// [0 0 0 0 3 'a' 'b' 'c']
///
/// String null Would be encoded as:
/// [1 0 0 0 0]
class ARROW_EXPORT RowEncoder {
 public:
  static constexpr int kRowIdForNulls() { return -1; }

  void Init(const std::vector<TypeHolder>& column_types, ExecContext* ctx);
  void Clear();
  Status EncodeAndAppend(const ExecSpan& batch);
  Result<ExecBatch> Decode(int64_t num_rows, const int32_t* row_ids);

  // Return the encoded row at the given index as a string
  inline std::string encoded_row(int32_t i) const {
    if (i == kRowIdForNulls()) {
      return std::string(reinterpret_cast<const char*>(encoded_nulls_.data()),
                         encoded_nulls_.size());
    }
    int32_t row_length = offsets_[i + 1] - offsets_[i];
    return std::string(reinterpret_cast<const char*>(bytes_.data() + offsets_[i]),
                       row_length);
  }

  int32_t num_rows() const {
    return offsets_.empty() ? 0 : static_cast<int32_t>(offsets_.size() - 1);
  }

 private:
  ExecContext* ctx_{nullptr};
  std::vector<std::shared_ptr<KeyEncoder>> encoders_;
  // The offsets of each row in the encoded bytes.
  // The size would be num_rows + 1 if not empty.
  std::vector<int32_t> offsets_;
  std::vector<uint8_t> bytes_;
  // A constant row with all its columns encoded as null. Useful when the caller is certain that an entire row is null and then uses kRowIdForNulls to refer to it.
  std::vector<uint8_t> encoded_nulls_;
  std::vector<std::shared_ptr<ExtensionType>> extension_types_;
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
