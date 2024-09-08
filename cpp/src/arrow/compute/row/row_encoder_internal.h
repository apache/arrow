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
#include <iostream>

#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/visibility.h"
#include "arrow/visit_data_inline.h"

#include <arrow/array/concatenate.h>

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

struct ARROW_COMPUTE_EXPORT KeyEncoder {
  // the first byte of an encoded key is used to indicate nullity
  static constexpr bool kExtraByteForNull = true;

  static constexpr uint8_t kNullByte = 1;
  static constexpr uint8_t kValidByte = 0;

  virtual ~KeyEncoder() = default;

  // Increment the values in the lengths array by the length of the encoded key for the
  // corresponding value in the given column.
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

  // Encode the column into the encoded_bytes, which is an array of pointers to each row
  // buffer.
  //
  // If value is an array, the array-size should be batch_length.
  // If value is a scalar, the value would repeat batch_length times.
  // NB: The pointers in the encoded_bytes will be advanced as values being encoded into.
  virtual Status Encode(const ExecValue&, int64_t batch_length,
                        uint8_t** encoded_bytes) = 0;

  // Encode a null value into the encoded_bytes, which is an array of pointers to each row
  // buffer.
  //
  // It's a special case for Encode like `Encode(Null-Scalar, 1, encoded_bytes)`.
  // NB: The pointers in the encoded_bytes will be advanced as values being encoded into.
  virtual void EncodeNull(uint8_t** encoded_bytes) = 0;

  // Decode the encoded key from the encoded_bytes, which is an array of pointers to each
  // row buffer, into an ArrayData.
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

struct ARROW_COMPUTE_EXPORT BooleanKeyEncoder : KeyEncoder {
  static constexpr int kByteWidth = 1;

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override;

  void AddLengthNull(int32_t* length) override;

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override;

  void EncodeNull(uint8_t** encoded_bytes) override;

  Result<std::shared_ptr<ArrayData>> Decode(uint8_t** encoded_bytes, int32_t length,
                                            MemoryPool* pool) override;
};

struct ARROW_COMPUTE_EXPORT FixedWidthKeyEncoder : KeyEncoder {
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
  const int byte_width_;
};

struct ARROW_COMPUTE_EXPORT DictionaryKeyEncoder : FixedWidthKeyEncoder {
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
struct VarLengthKeyEncoder : KeyEncoder {
  using Offset = typename T::offset_type;

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override {
    if (data.is_array()) {
      int64_t i = 0;
      ARROW_DCHECK_EQ(data.array.length, batch_length);
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
      ARROW_DCHECK_EQ(data.length(), batch_length);
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

struct ARROW_COMPUTE_EXPORT NullKeyEncoder : KeyEncoder {
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

template <typename ListType>
struct ARROW_EXPORT ListKeyEncoder : KeyEncoder {
  static_assert(is_list_like_type<ListType>(), "ListKeyEncoder only supports ListType");
  using Offset = typename ListType::offset_type;

  ListKeyEncoder(std::shared_ptr<DataType> self_type,
                 std::shared_ptr<DataType> element_type,
                 std::shared_ptr<KeyEncoder> element_encoder)
      : self_type_(std::move(self_type)),
        element_type_(std::move(element_type)),
        element_encoder_(std::move(element_encoder)) {}

  void AddLength(const ExecValue& data, int64_t batch_length, int32_t* lengths) override {
    if (data.is_array()) {
      ARROW_DCHECK_EQ(data.array.length, batch_length);
      const uint8_t* validity = data.array.buffers[0].data;
      const auto* offsets = data.array.GetValues<Offset>(1);
      // AddLength for each list
      std::vector<int32_t> child_lengthes;
      int32_t index{0};
      ArraySpan tmp_child_data(data.array.child_data[0]);
      VisitBitBlocksVoid(
          validity, data.array.offset, data.array.length,
          [&](int64_t i) {
            ARROW_UNUSED(i);
            child_lengthes.clear();
            Offset list_length = offsets[i + 1] - offsets[i];
            if (list_length == 0) {
              lengths[index] += kExtraByteForNull + sizeof(Offset);
              ++index;
              return;
            }
            child_lengthes.resize(list_length, 0);
            tmp_child_data.SetSlice(offsets[i], list_length);
            this->element_encoder_->AddLength(ExecValue{tmp_child_data}, list_length,
                                              child_lengthes.data());
            lengths[index] += kExtraByteForNull + sizeof(Offset);
            for (int32_t j = 0; j < list_length; j++) {
              lengths[index] += child_lengthes[j];
            }
            ++index;
          },
          [&]() {
            lengths[index] = kExtraByteForNull + sizeof(Offset);
            ++index;
          });
    } else {
      const auto& list_scalar = data.scalar_as<BaseListScalar>();
      int32_t accum_length = 0;
      // Counting the size of the encoded list if the list is valid
      if (list_scalar.is_valid && list_scalar.value->length() > 0) {
        auto element_count = static_cast<int32_t>(list_scalar.value->length());
        // Counting the size of the encoded list
        std::vector<int32_t> child_lengthes(element_count, 0);
        this->element_encoder_->AddLength(ExecValue{*list_scalar.value->data()},
                                          element_count, child_lengthes.data());
        for (int32_t i = 0; i < element_count; i++) {
          accum_length += child_lengthes[i];
        }
      }
      for (int64_t i = 0; i < batch_length; i++) {
        lengths[i] += kExtraByteForNull + sizeof(Offset) + accum_length;
      }
    }
  }

  void AddLengthNull(int32_t* length) override {
    *length += kExtraByteForNull + sizeof(Offset);
  }

  Status Encode(const ExecValue& data, int64_t batch_length,
                uint8_t** encoded_bytes) override {
    auto handle_null_value = [&encoded_bytes]() {
      auto& encoded_ptr = *encoded_bytes++;
      *encoded_ptr++ = kNullByte;
      util::SafeStore(encoded_ptr, static_cast<Offset>(0));
      encoded_ptr += sizeof(Offset);
    };
    auto handle_valid_value = [&encoded_bytes,
                               this](const ArraySpan& child_array) -> Status {
      auto& encoded_ptr = *encoded_bytes++;
      *encoded_ptr++ = kValidByte;
      util::SafeStore(encoded_ptr, static_cast<Offset>(child_array.length));
      encoded_ptr += sizeof(Offset);
      // handling the child data
      for (int64_t i = 0; i < child_array.length; i++) {
        ArraySpan tmp_child_data(child_array);
        tmp_child_data.SetSlice(child_array.offset + i, 1);
        RETURN_NOT_OK(
            this->element_encoder_->Encode(ExecValue{tmp_child_data}, 1, &encoded_ptr));
      }
      return Status::OK();
    };
    if (data.is_array()) {
      ARROW_DCHECK_EQ(data.array.length, batch_length);
      const uint8_t* validity = data.array.buffers[0].data;
      const auto* offsets = data.array.GetValues<Offset>(1);
      ArraySpan tmp_child_data(data.array.child_data[0]);
      RETURN_NOT_OK(VisitBitBlocks(
          validity, data.array.offset, data.array.length,
          [&](int64_t i) {
            ARROW_UNUSED(i);
            Offset list_length = offsets[i + 1] - offsets[i];
            tmp_child_data.SetSlice(offsets[i], list_length);
            return handle_valid_value(tmp_child_data);
          },
          [&]() {
            handle_null_value();
            return Status::OK();
          }));
    } else {
      const auto& list_scalar = data.scalar_as<BaseListScalar>();
      if (list_scalar.is_valid) {
        ArraySpan span(*list_scalar.value->data());
        for (int64_t i = 0; i < batch_length; i++) {
          RETURN_NOT_OK(handle_valid_value(span));
        }
      } else {
        for (int64_t i = 0; i < batch_length; i++) {
          handle_null_value();
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

    // Build the offsets buffer of the ListArray
    ARROW_ASSIGN_OR_RAISE(auto offset_buf,
                          AllocateBuffer(sizeof(Offset) * (1 + length), pool));
    auto raw_offsets = offset_buf->mutable_span_as<Offset>();
    Offset element_sum = 0;
    raw_offsets[0] = 0;
    std::vector<std::shared_ptr<Array>> child_datas;
    for (int32_t i = 0; i < length; ++i) {
      Offset element_count = util::SafeLoadAs<Offset>(encoded_bytes[i]);
      element_sum += element_count;
      raw_offsets[i + 1] = element_sum;
      encoded_bytes[i] += sizeof(Offset);
      for (Offset j = 0; j < element_count; ++j) {
        ARROW_ASSIGN_OR_RAISE(
            auto child_data,
            element_encoder_->Decode(encoded_bytes + i, /*length=*/1, pool));
        ArraySpan array_span(*child_data);
        child_datas.push_back(array_span.ToArray());
      }
    }
    std::shared_ptr<ArrayData> element_data;
    if (!child_datas.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto element_array, ::arrow::Concatenate(child_datas, pool));
      element_data = element_array->data();
    } else {
      // If there are no elements, we need to create an empty array
      std::unique_ptr<ArrayBuilder> tmp;
      RETURN_NOT_OK(MakeBuilder(pool, element_type_, &tmp));
      std::shared_ptr<Array> array;
      RETURN_NOT_OK(tmp->Finish(&array));
      element_data = array->data();
    }
    return ArrayData::Make(self_type_, length,
                           {std::move(null_buf), std::move(offset_buf)}, {element_data},
                           null_count);
  }

  std::shared_ptr<DataType> self_type_;
  std::shared_ptr<DataType> element_type_;
  std::shared_ptr<KeyEncoder> element_encoder_;
  // extension_type_ is used to store the extension type of the list element.
  // It would be nullptr if the list element is not an extension type.
  std::shared_ptr<ExtensionType> extension_type_;
};

/// RowEncoder encodes ExecSpan to a variable length byte sequence
/// created by concatenating the encoded form of each column. The encoding
/// for each column depends on its data type.
///
/// This is used to encode columns into row-major format, which will be
/// beneficial for grouping and joining operations.
///
/// Unlike DuckDB and arrow-rs, currently this row format can not help
/// sortings because the row-format is uncomparable.
///
/// # Key Column Encoding
///
/// The row format is composed of the the KeyColumn encodings for each,
/// and the column is encoded as follows:
/// 1. A null byte for each column, indicating whether the column is null.
///    "1" for null, "0" for non-null.
/// 2. The "fixed width" encoding for the column, it would exist whether
///    the column is null or not.
/// 3. The "variable payload" encoding for the column, it would exists only
///    for non-null string/binary columns.
///    For string/binary columns, the length of the payload is in
///    "fixed width" part, and the binary contents are in the
///    "variable payload" part.
/// 4. Specially, if all columns in a row are null, the caller may decide
///    to refer to kRowIdForNulls instead of actually encoding/decoding
///    it using any KeyEncoder. See the comment for encoded_nulls_.
///
/// The endianness of the encoded bytes is platform-dependent.
///
/// ## Null Type
///
/// Null Type is a special case, it doesn't occupy any space in the
/// encoded row.
///
/// ## Fixed Width Type
///
/// Fixed Width Type is encoded as a fixed-width byte sequence. For example:
/// ```
/// Int8: 5, null, 6
/// ```
/// Would be encoded as [0 5], [1 0], [0 6].
///
/// ### Dictionary Type
///
/// Dictionary Type is encoded as a fixed-width byte sequence using
/// dictionary  indices, the dictionary should be identical for all
/// rows.
///
/// ## Variable Width Type
///
/// Variable Width Type is encoded as:
/// [null byte, variable-byte length, variable bytes]. For example:
///
/// String "abc" Would be encoded as:
/// 0 ( 1 byte for not null) + 3 ( 4 bytes for length ) + "abc" (payload)
///
/// Null string Would be encoded as:
/// 1 ( 1 byte for null) + 0 ( 4 bytes for length )
///
/// The size of the "fixed-width" part is defined by the `offset_type`
/// of the variable-width type. For example, it would be 4 bytes for
/// String/Binary type and 8 bytes for LargeString/LargeBinary type.
///
/// ## List Type
///
/// List Type is encoded as:
/// [null byte, list element count, [element 1, element 2, ...]]
/// Element count uses `Offset` bytes.
///
/// Currently, we only support encoding of primitive types, dictionary types
/// in the list, the nested list is not supported.
///
/// # Row Encoding
///
/// The row format is the concatenation of the encodings of each column.
class ARROW_COMPUTE_EXPORT RowEncoder {
 public:
  static constexpr int kRowIdForNulls() { return -1; }

  Status Init(const std::vector<TypeHolder>& column_types, ExecContext* ctx);
  void Clear();
  Status EncodeAndAppend(const ExecSpan& batch);
  Result<ExecBatch> Decode(int64_t num_rows, const int32_t* row_ids);

  // Returns the encoded representation of the row at index i.
  // If i is kRowIdForNulls, it returns the pre-encoded all-nulls
  // row.
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
  // offsets_ vector stores the starting position (offset) of each encoded row
  // within the bytes_ vector. This allows for quick access to individual rows.
  //
  // The size would be num_rows + 1 if not empty, the last element is the total
  // length of the bytes_ vector.
  std::vector<int32_t> offsets_;
  // The encoded bytes of all non "kRowIdForNulls" rows.
  std::vector<uint8_t> bytes_;
  // A pre-encoded constant row with all its columns being null. Useful when
  // the caller is certain that an entire row is null and then uses kRowIdForNulls
  // to refer to it.
  //
  // EncodeAndAppend would never append this row, but encoded_row and Decode would
  // return this row when kRowIdForNulls is passed.
  std::vector<uint8_t> encoded_nulls_;
  std::vector<std::shared_ptr<ExtensionType>> extension_types_;
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
