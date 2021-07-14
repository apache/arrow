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

#include "arrow/array/util.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/extension_type.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Loading from ArrayData

namespace {

class ArrayDataWrapper {
 public:
  ArrayDataWrapper(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out)
      : data_(data), out_(out) {}

  template <typename T>
  Status Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    *out_ = std::make_shared<ArrayType>(data_);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    *out_ = type.MakeArray(data_);
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<Array>* out_;
};

class ArrayDataEndianSwapper {
 public:
  ArrayDataEndianSwapper(const std::shared_ptr<ArrayData>& data, int64_t length)
      : data_(data), length_(length) {
    out_ = data->Copy();
  }

  Status SwapType(const DataType& type) {
    RETURN_NOT_OK(VisitTypeInline(type, this));
    RETURN_NOT_OK(SwapChildren(type.fields()));
    if (internal::HasValidityBitmap(type.id())) {
      // Copy null bitmap
      out_->buffers[0] = data_->buffers[0];
    }
    return Status::OK();
  }

  Status SwapChildren(const FieldVector& child_fields) {
    for (size_t i = 0; i < child_fields.size(); i++) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i],
                            internal::SwapEndianArrayData(data_->child_data[i]));
    }
    return Status::OK();
  }

  template <typename T>
  Result<std::shared_ptr<Buffer>> ByteSwapBuffer(
      const std::shared_ptr<Buffer>& in_buffer) {
    if (sizeof(T) == 1) {
      // if data size is 1, element is not swapped. We can use the original buffer
      return in_buffer;
    }
    auto in_data = reinterpret_cast<const T*>(in_buffer->data());
    ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateBuffer(in_buffer->size()));
    auto out_data = reinterpret_cast<T*>(out_buffer->mutable_data());
    int64_t length = in_buffer->size() / sizeof(T);
    for (int64_t i = 0; i < length; i++) {
      out_data[i] = BitUtil::ByteSwap(in_data[i]);
    }
    return std::move(out_buffer);
  }

  template <typename VALUE_TYPE>
  Status SwapOffsets(int index) {
    if (data_->buffers[index] == nullptr || data_->buffers[index]->size() == 0) {
      out_->buffers[index] = data_->buffers[index];
      return Status::OK();
    }
    // Except union, offset has one more element rather than data->length
    ARROW_ASSIGN_OR_RAISE(out_->buffers[index],
                          ByteSwapBuffer<VALUE_TYPE>(data_->buffers[index]));
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_base_of<FixedWidthType, T>::value &&
                  !std::is_base_of<FixedSizeBinaryType, T>::value &&
                  !std::is_base_of<DictionaryType, T>::value,
              Status>
  Visit(const T& type) {
    using value_type = typename T::c_type;
    ARROW_ASSIGN_OR_RAISE(out_->buffers[1],
                          ByteSwapBuffer<value_type>(data_->buffers[1]));
    return Status::OK();
  }

  Status Visit(const Decimal128Type& type) {
    auto data = reinterpret_cast<const uint64_t*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto new_data = reinterpret_cast<uint64_t*>(new_buffer->mutable_data());
    int64_t length = length_;
    length = data_->buffers[1]->size() / (sizeof(uint64_t) * 2);
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp;
      auto idx = i * 2;
#if ARROW_LITTLE_ENDIAN
      tmp = BitUtil::FromBigEndian(data[idx]);
      new_data[idx] = BitUtil::FromBigEndian(data[idx + 1]);
      new_data[idx + 1] = tmp;
#else
      tmp = BitUtil::FromLittleEndian(data[idx]);
      new_data[idx] = BitUtil::FromLittleEndian(data[idx + 1]);
      new_data[idx + 1] = tmp;
#endif
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const Decimal256Type& type) {
    auto data = reinterpret_cast<const uint64_t*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer, AllocateBuffer(data_->buffers[1]->size()));
    auto new_data = reinterpret_cast<uint64_t*>(new_buffer->mutable_data());
    int64_t length = length_;
    length = data_->buffers[1]->size() / (sizeof(uint64_t) * 4);
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp0, tmp1, tmp2;
      auto idx = i * 4;
#if ARROW_LITTLE_ENDIAN
      tmp0 = BitUtil::FromBigEndian(data[idx]);
      tmp1 = BitUtil::FromBigEndian(data[idx + 1]);
      tmp2 = BitUtil::FromBigEndian(data[idx + 2]);
      new_data[idx] = BitUtil::FromBigEndian(data[idx + 3]);
      new_data[idx + 1] = tmp2;
      new_data[idx + 2] = tmp1;
      new_data[idx + 3] = tmp0;
#else
      tmp0 = BitUtil::FromLittleEndian(data[idx]);
      tmp1 = BitUtil::FromLittleEndian(data[idx + 1]);
      tmp2 = BitUtil::FromLittleEndian(data[idx + 2]);
      new_data[idx] = BitUtil::FromLittleEndian(data[idx + 3]);
      new_data[idx + 1] = tmp2;
      new_data[idx + 2] = tmp1;
      new_data[idx + 3] = tmp0;
#endif
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const DayTimeIntervalType& type) {
    ARROW_ASSIGN_OR_RAISE(out_->buffers[1], ByteSwapBuffer<uint32_t>(data_->buffers[1]));
    return Status::OK();
  }

  Status Visit(const NullType& type) { return Status::OK(); }
  Status Visit(const BooleanType& type) { return Status::OK(); }
  Status Visit(const Int8Type& type) { return Status::OK(); }
  Status Visit(const UInt8Type& type) { return Status::OK(); }
  Status Visit(const FixedSizeBinaryType& type) { return Status::OK(); }
  Status Visit(const FixedSizeListType& type) { return Status::OK(); }
  Status Visit(const StructType& type) { return Status::OK(); }
  Status Visit(const UnionType& type) {
    out_->buffers[1] = data_->buffers[1];
    if (type.mode() == UnionMode::DENSE) {
      RETURN_NOT_OK(SwapOffsets<int32_t>(2));
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_same<BinaryType, T>::value || std::is_same<StringType, T>::value,
              Status>
  Visit(const T& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    out_->buffers[2] = data_->buffers[2];
    return Status::OK();
  }

  template <typename T>
  enable_if_t<std::is_same<LargeBinaryType, T>::value ||
                  std::is_same<LargeStringType, T>::value,
              Status>
  Visit(const T& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    out_->buffers[2] = data_->buffers[2];
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    return Status::OK();
  }
  Status Visit(const LargeListType& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // dictionary was already swapped in ReadDictionary() in ipc/reader.cc
    RETURN_NOT_OK(SwapType(*type.index_type()));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    RETURN_NOT_OK(SwapType(*type.storage_type()));
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  int64_t length_;
  std::shared_ptr<ArrayData> out_;
};

}  // namespace

namespace internal {

Result<std::shared_ptr<ArrayData>> SwapEndianArrayData(
    const std::shared_ptr<ArrayData>& data) {
  if (data->offset != 0) {
    return Status::Invalid("Unsupported data format: data.offset != 0");
  }
  ArrayDataEndianSwapper swapper(data, data->length);
  RETURN_NOT_OK(swapper.SwapType(*data->type));
  return std::move(swapper.out_);
}

}  // namespace internal

std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data) {
  std::shared_ptr<Array> out;
  ArrayDataWrapper wrapper_visitor(data, &out);
  DCHECK_OK(VisitTypeInline(*data->type, &wrapper_visitor));
  DCHECK(out);
  return out;
}

// ----------------------------------------------------------------------
// Misc APIs

namespace {

// get the maximum buffer length required, then allocate a single zeroed buffer
// to use anywhere a buffer is required
class NullArrayFactory {
 public:
  struct GetBufferLength {
    GetBufferLength(const std::shared_ptr<DataType>& type, int64_t length)
        : type_(*type), length_(length), buffer_length_(BitUtil::BytesForBits(length)) {}

    Result<int64_t> Finish() && {
      RETURN_NOT_OK(VisitTypeInline(type_, this));
      return buffer_length_;
    }

    template <typename T, typename = decltype(TypeTraits<T>::bytes_required(0))>
    Status Visit(const T&) {
      return MaxOf(TypeTraits<T>::bytes_required(length_));
    }

    template <typename T>
    enable_if_var_size_list<T, Status> Visit(const T&) {
      // values array may be empty, but there must be at least one offset of 0
      return MaxOf(sizeof(typename T::offset_type) * (length_ + 1));
    }

    template <typename T>
    enable_if_base_binary<T, Status> Visit(const T&) {
      // values buffer may be empty, but there must be at least one offset of 0
      return MaxOf(sizeof(typename T::offset_type) * (length_ + 1));
    }

    Status Visit(const FixedSizeListType& type) {
      return MaxOf(GetBufferLength(type.value_type(), type.list_size() * length_));
    }

    Status Visit(const FixedSizeBinaryType& type) {
      return MaxOf(type.byte_width() * length_);
    }

    Status Visit(const StructType& type) {
      for (const auto& child : type.fields()) {
        RETURN_NOT_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const UnionType& type) {
      // type codes
      RETURN_NOT_OK(MaxOf(length_));
      if (type.mode() == UnionMode::DENSE) {
        // offsets
        RETURN_NOT_OK(MaxOf(sizeof(int32_t) * length_));
      }
      for (const auto& child : type.fields()) {
        RETURN_NOT_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const DictionaryType& type) {
      RETURN_NOT_OK(MaxOf(GetBufferLength(type.value_type(), length_)));
      return MaxOf(GetBufferLength(type.index_type(), length_));
    }

    Status Visit(const ExtensionType& type) {
      // XXX is an extension array's length always == storage length
      return MaxOf(GetBufferLength(type.storage_type(), length_));
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("construction of all-null ", type);
    }

   private:
    Status MaxOf(GetBufferLength&& other) {
      ARROW_ASSIGN_OR_RAISE(int64_t buffer_length, std::move(other).Finish());
      return MaxOf(buffer_length);
    }

    Status MaxOf(int64_t buffer_length) {
      if (buffer_length > buffer_length_) {
        buffer_length_ = buffer_length;
      }
      return Status::OK();
    }

    const DataType& type_;
    int64_t length_, buffer_length_;
  };

  NullArrayFactory(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   int64_t length)
      : pool_(pool), type_(type), length_(length) {}

  Status CreateBuffer() {
    ARROW_ASSIGN_OR_RAISE(int64_t buffer_length,
                          GetBufferLength(type_, length_).Finish());
    ARROW_ASSIGN_OR_RAISE(buffer_, AllocateBuffer(buffer_length, pool_));
    std::memset(buffer_->mutable_data(), 0, buffer_->size());
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> Create() {
    if (buffer_ == nullptr) {
      RETURN_NOT_OK(CreateBuffer());
    }
    std::vector<std::shared_ptr<ArrayData>> child_data(type_->num_fields());
    out_ = ArrayData::Make(type_, length_, {buffer_}, child_data, length_, 0);
    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return out_;
  }

  Status Visit(const NullType&) {
    out_->buffers.resize(1, nullptr);
    return Status::OK();
  }

  Status Visit(const FixedWidthType&) {
    out_->buffers.resize(2, buffer_);
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    out_->buffers.resize(3, buffer_);
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    out_->buffers.resize(2, buffer_);
    ARROW_ASSIGN_OR_RAISE(out_->child_data[0], CreateChild(0, /*length=*/0));
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    ARROW_ASSIGN_OR_RAISE(out_->child_data[0],
                          CreateChild(0, length_ * type.list_size()));
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type_->num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i], CreateChild(i, length_));
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    out_->buffers.resize(2);

    // First buffer is always null
    out_->buffers[0] = nullptr;

    // Type codes are all zero, so we can use buffer_ which has had it's memory
    // zeroed
    out_->buffers[1] = buffer_;

    // For sparse unions, we now create children with the same length as the
    // parent
    int64_t child_length = length_;
    if (type.mode() == UnionMode::DENSE) {
      // For dense unions, we set the offsets to all zero and create children
      // with length 1
      out_->buffers.resize(3);
      out_->buffers[2] = buffer_;

      child_length = 1;
    }
    for (int i = 0; i < type_->num_fields(); ++i) {
      ARROW_ASSIGN_OR_RAISE(out_->child_data[i], CreateChild(i, child_length));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    out_->buffers.resize(2, buffer_);
    ARROW_ASSIGN_OR_RAISE(auto typed_null_dict, MakeArrayOfNull(type.value_type(), 0));
    out_->dictionary = typed_null_dict->data();
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("construction of all-null ", type);
  }

  Result<std::shared_ptr<ArrayData>> CreateChild(int i, int64_t length) {
    NullArrayFactory child_factory(pool_, type_->field(i)->type(), length);
    child_factory.buffer_ = buffer_;
    return child_factory.Create();
  }

  MemoryPool* pool_;
  std::shared_ptr<DataType> type_;
  int64_t length_;
  std::shared_ptr<ArrayData> out_;
  std::shared_ptr<Buffer> buffer_;
};

class RepeatedArrayFactory {
 public:
  RepeatedArrayFactory(MemoryPool* pool, const Scalar& scalar, int64_t length)
      : pool_(pool), scalar_(scalar), length_(length) {}

  Result<std::shared_ptr<Array>> Create() {
    RETURN_NOT_OK(VisitTypeInline(*scalar_.type, this));
    return out_;
  }

  Status Visit(const NullType& type) {
    DCHECK(false);  // already forwarded to MakeArrayOfNull
    return Status::OK();
  }

  Status Visit(const BooleanType&) {
    ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBitmap(length_, pool_));
    BitUtil::SetBitsTo(buffer->mutable_data(), 0, length_,
                       checked_cast<const BooleanScalar&>(scalar_).value);
    out_ = std::make_shared<BooleanArray>(length_, buffer);
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_number_type<T>::value || is_temporal_type<T>::value, Status> Visit(
      const T&) {
    auto value = checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_).value;
    return FinishFixedWidth(&value, sizeof(value));
  }

  Status Visit(const FixedSizeBinaryType& type) {
    auto value = checked_cast<const FixedSizeBinaryScalar&>(scalar_).value;
    return FinishFixedWidth(value->data(), type.byte_width());
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    using ScalarType = typename TypeTraits<T>::ScalarType;
    auto value = checked_cast<const ScalarType&>(scalar_).value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  Status Visit(const Decimal256Type&) {
    auto value = checked_cast<const Decimal256Scalar&>(scalar_).value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    std::shared_ptr<Buffer> value =
        checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_).value;
    std::shared_ptr<Buffer> values_buffer, offsets_buffer;
    RETURN_NOT_OK(CreateBufferOf(value->data(), value->size(), &values_buffer));
    auto size = static_cast<typename T::offset_type>(value->size());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));
    out_ = std::make_shared<typename TypeTraits<T>::ArrayType>(length_, offsets_buffer,
                                                               values_buffer);
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    using ScalarType = typename TypeTraits<T>::ScalarType;
    using ArrayType = typename TypeTraits<T>::ArrayType;

    auto value = checked_cast<const ScalarType&>(scalar_).value;

    ArrayVector values(length_, value);
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    std::shared_ptr<Buffer> offsets_buffer;
    auto size = static_cast<typename T::offset_type>(value->length());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));

    out_ =
        std::make_shared<ArrayType>(scalar_.type, length_, offsets_buffer, value_array);
    return Status::OK();
  }

  Status Visit(const FixedSizeListType& type) {
    auto value = checked_cast<const FixedSizeListScalar&>(scalar_).value;

    ArrayVector values(length_, value);
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    out_ = std::make_shared<FixedSizeListArray>(scalar_.type, length_, value_array);
    return Status::OK();
  }

  Status Visit(const MapType& type) {
    auto map_scalar = checked_cast<const MapScalar&>(scalar_);
    auto struct_array = checked_cast<const StructArray*>(map_scalar.value.get());

    ArrayVector keys(length_, struct_array->field(0));
    ArrayVector values(length_, struct_array->field(1));

    ARROW_ASSIGN_OR_RAISE(auto key_array, Concatenate(keys, pool_));
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    std::shared_ptr<Buffer> offsets_buffer;
    auto size = static_cast<typename MapType::offset_type>(struct_array->length());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));

    out_ = std::make_shared<MapArray>(scalar_.type, length_, std::move(offsets_buffer),
                                      std::move(key_array), std::move(value_array));
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    const auto& value = checked_cast<const DictionaryScalar&>(scalar_).value;
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          MakeArrayFromScalar(*value.index, length_, pool_));
    out_ = std::make_shared<DictionaryArray>(scalar_.type, std::move(indices),
                                             value.dictionary);
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    ArrayVector fields;
    for (const auto& value : checked_cast<const StructScalar&>(scalar_).value) {
      fields.emplace_back();
      ARROW_ASSIGN_OR_RAISE(fields.back(), MakeArrayFromScalar(*value, length_, pool_));
    }
    out_ = std::make_shared<StructArray>(scalar_.type, length_, std::move(fields));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    return Status::NotImplemented("construction from scalar of type ", *scalar_.type);
  }

  Status Visit(const DenseUnionType& type) {
    return Status::NotImplemented("construction from scalar of type ", *scalar_.type);
  }

  Status Visit(const SparseUnionType& type) {
    return Status::NotImplemented("construction from scalar of type ", *scalar_.type);
  }

  template <typename OffsetType>
  Status CreateOffsetsBuffer(OffsetType value_length, std::shared_ptr<Buffer>* out) {
    TypedBufferBuilder<OffsetType> builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_ + 1));
    OffsetType offset = 0;
    for (int64_t i = 0; i < length_ + 1; ++i, offset += value_length) {
      builder.UnsafeAppend(offset);
    }
    return builder.Finish(out);
  }

  Status CreateBufferOf(const void* data, size_t data_length,
                        std::shared_ptr<Buffer>* out) {
    BufferBuilder builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_ * data_length));
    for (int64_t i = 0; i < length_; ++i) {
      builder.UnsafeAppend(data, data_length);
    }
    return builder.Finish(out);
  }

  Status FinishFixedWidth(const void* data, size_t data_length) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(CreateBufferOf(data, data_length, &buffer));
    out_ = MakeArray(
        ArrayData::Make(scalar_.type, length_, {nullptr, std::move(buffer)}, 0));
    return Status::OK();
  }

  MemoryPool* pool_;
  const Scalar& scalar_;
  int64_t length_;
  std::shared_ptr<Array> out_;
};

}  // namespace

Result<std::shared_ptr<Array>> MakeArrayOfNull(const std::shared_ptr<DataType>& type,
                                               int64_t length, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto data, NullArrayFactory(pool, type, length).Create());
  return MakeArray(data);
}

Result<std::shared_ptr<Array>> MakeArrayFromScalar(const Scalar& scalar, int64_t length,
                                                   MemoryPool* pool) {
  if (!scalar.is_valid) {
    return MakeArrayOfNull(scalar.type, length, pool);
  }
  return RepeatedArrayFactory(pool, scalar, length).Create();
}

namespace internal {

std::vector<ArrayVector> RechunkArraysConsistently(
    const std::vector<ArrayVector>& groups) {
  if (groups.size() <= 1) {
    return groups;
  }
  int64_t total_length = 0;
  for (const auto& array : groups.front()) {
    total_length += array->length();
  }
#ifndef NDEBUG
  for (const auto& group : groups) {
    int64_t group_length = 0;
    for (const auto& array : group) {
      group_length += array->length();
    }
    DCHECK_EQ(group_length, total_length)
        << "Array groups should have the same total number of elements";
  }
#endif
  if (total_length == 0) {
    return groups;
  }

  // Set up result vectors
  std::vector<ArrayVector> rechunked_groups(groups.size());

  // Set up progress counters
  std::vector<ArrayVector::const_iterator> current_arrays;
  std::vector<int64_t> array_offsets;
  for (const auto& group : groups) {
    current_arrays.emplace_back(group.cbegin());
    array_offsets.emplace_back(0);
  }

  // Scan all array vectors at once, rechunking along the way
  int64_t start = 0;
  while (start < total_length) {
    // First compute max possible length for next chunk
    int64_t chunk_length = std::numeric_limits<int64_t>::max();
    for (size_t i = 0; i < groups.size(); i++) {
      auto& arr_it = current_arrays[i];
      auto& offset = array_offsets[i];
      // Skip any done arrays (including 0-length arrays)
      while (offset == (*arr_it)->length()) {
        ++arr_it;
        offset = 0;
      }
      const auto& array = *arr_it;
      DCHECK_GT(array->length(), offset);
      chunk_length = std::min(chunk_length, array->length() - offset);
    }
    DCHECK_GT(chunk_length, 0);

    // Then slice all arrays along this chunk size
    for (size_t i = 0; i < groups.size(); i++) {
      const auto& array = *current_arrays[i];
      auto& offset = array_offsets[i];
      if (offset == 0 && array->length() == chunk_length) {
        // Slice spans entire array
        rechunked_groups[i].emplace_back(array);
      } else {
        DCHECK_LT(chunk_length - offset, array->length());
        rechunked_groups[i].emplace_back(array->Slice(offset, chunk_length));
      }
      offset += chunk_length;
    }
    start += chunk_length;
  }

  return rechunked_groups;
}

}  // namespace internal
}  // namespace arrow
