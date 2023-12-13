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

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/extension_type.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/logging.h"
#include "arrow/util/range.h"
#include "arrow/util/sort.h"
#include "arrow/util/span.h"
#include "arrow/visit_data_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::checked_cast;
using internal::Zip;

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
  explicit ArrayDataEndianSwapper(const std::shared_ptr<ArrayData>& data,
                                  MemoryPool* pool)
      : data_(data), pool_(pool) {
    out_ = data->Copy();
  }

  // WARNING: this facility can be called on invalid Array data by the IPC reader.
  // Do not rely on the advertised ArrayData length, instead use the physical
  // buffer sizes to avoid accessing memory out of bounds.
  //
  // (If this guarantee turns out to be difficult to maintain, we should call
  //  Validate() instead)
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
                            internal::SwapEndianArrayData(data_->child_data[i], pool_));
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
    ARROW_ASSIGN_OR_RAISE(auto out_buffer, AllocateBuffer(in_buffer->size(), pool_));
    auto out_data = reinterpret_cast<T*>(out_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    int64_t length = in_buffer->size() / sizeof(T);
    for (int64_t i = 0; i < length; i++) {
      out_data[i] = bit_util::ByteSwap(in_data[i]);
    }
    return std::move(out_buffer);
  }

  template <typename VALUE_TYPE>
  Status SwapOffsets(int index) {
    if (data_->buffers[index] == nullptr || data_->buffers[index]->size() == 0) {
      out_->buffers[index] = data_->buffers[index];
      return Status::OK();
    }
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
    ARROW_ASSIGN_OR_RAISE(auto new_buffer,
                          AllocateBuffer(data_->buffers[1]->size(), pool_));
    auto new_data = reinterpret_cast<uint64_t*>(new_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / Decimal128Type::kByteWidth;
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp;
      auto idx = i * 2;
#if ARROW_LITTLE_ENDIAN
      tmp = bit_util::FromBigEndian(data[idx]);
      new_data[idx] = bit_util::FromBigEndian(data[idx + 1]);
      new_data[idx + 1] = tmp;
#else
      tmp = bit_util::FromLittleEndian(data[idx]);
      new_data[idx] = bit_util::FromLittleEndian(data[idx + 1]);
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
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / Decimal256Type::kByteWidth;
    for (int64_t i = 0; i < length; i++) {
      uint64_t tmp0, tmp1, tmp2;
      auto idx = i * 4;
#if ARROW_LITTLE_ENDIAN
      tmp0 = bit_util::FromBigEndian(data[idx]);
      tmp1 = bit_util::FromBigEndian(data[idx + 1]);
      tmp2 = bit_util::FromBigEndian(data[idx + 2]);
      new_data[idx] = bit_util::FromBigEndian(data[idx + 3]);
      new_data[idx + 1] = tmp2;
      new_data[idx + 2] = tmp1;
      new_data[idx + 3] = tmp0;
#else
      tmp0 = bit_util::FromLittleEndian(data[idx]);
      tmp1 = bit_util::FromLittleEndian(data[idx + 1]);
      tmp2 = bit_util::FromLittleEndian(data[idx + 2]);
      new_data[idx] = bit_util::FromLittleEndian(data[idx + 3]);
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

  Status Visit(const MonthDayNanoIntervalType& type) {
    using MonthDayNanos = MonthDayNanoIntervalType::MonthDayNanos;
    auto data = reinterpret_cast<const MonthDayNanos*>(data_->buffers[1]->data());
    ARROW_ASSIGN_OR_RAISE(auto new_buffer,
                          AllocateBuffer(data_->buffers[1]->size(), pool_));
    auto new_data = reinterpret_cast<MonthDayNanos*>(new_buffer->mutable_data());
    // NOTE: data_->length not trusted (see warning above)
    const int64_t length = data_->buffers[1]->size() / sizeof(MonthDayNanos);
    for (int64_t i = 0; i < length; i++) {
      MonthDayNanos tmp = data[i];
#if ARROW_LITTLE_ENDIAN
      tmp.months = bit_util::FromBigEndian(tmp.months);
      tmp.days = bit_util::FromBigEndian(tmp.days);
      tmp.nanoseconds = bit_util::FromBigEndian(tmp.nanoseconds);
#else
      tmp.months = bit_util::FromLittleEndian(tmp.months);
      tmp.days = bit_util::FromLittleEndian(tmp.days);
      tmp.nanoseconds = bit_util::FromLittleEndian(tmp.nanoseconds);
#endif
      new_data[i] = tmp;
    }
    out_->buffers[1] = std::move(new_buffer);
    return Status::OK();
  }

  Status Visit(const NullType& type) { return Status::OK(); }
  Status Visit(const BooleanType& type) { return Status::OK(); }
  Status Visit(const Int8Type& type) { return Status::OK(); }
  Status Visit(const UInt8Type& type) { return Status::OK(); }
  Status Visit(const FixedSizeBinaryType& type) { return Status::OK(); }
  Status Visit(const FixedSizeListType& type) { return Status::OK(); }
  Status Visit(const StructType& type) { return Status::OK(); }
  Status Visit(const RunEndEncodedType& type) {
    return Status::NotImplemented("swapping endianness of run-end encoded array");
  }
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

  Status Visit(const BinaryViewType& type) {
    // TODO(GH-37879): This requires knowledge of whether the array is being swapped to
    // native endian or from it so that we know what size to trust when deciding whether
    // something is an inline view.
    return Status::NotImplemented("Swapping endianness of ", type);
  }

  Status Visit(const ListType& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    return Status::OK();
  }
  Status Visit(const LargeListType& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    return Status::OK();
  }

  Status Visit(const ListViewType& type) {
    RETURN_NOT_OK(SwapOffsets<int32_t>(1));
    RETURN_NOT_OK(SwapOffsets<int32_t>(2));
    return Status::OK();
  }
  Status Visit(const LargeListViewType& type) {
    RETURN_NOT_OK(SwapOffsets<int64_t>(1));
    RETURN_NOT_OK(SwapOffsets<int64_t>(2));
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
  MemoryPool* pool_;
  std::shared_ptr<ArrayData> out_;
};

}  // namespace

namespace internal {

Result<std::shared_ptr<ArrayData>> SwapEndianArrayData(
    const std::shared_ptr<ArrayData>& data, MemoryPool* pool) {
  if (data->offset != 0) {
    return Status::Invalid("Unsupported data format: data.offset != 0");
  }
  ArrayDataEndianSwapper swapper(data, pool);
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

static Result<std::shared_ptr<Scalar>> MakeScalarForRunEndValue(
    const DataType& run_end_type, int64_t run_end) {
  switch (run_end_type.id()) {
    case Type::INT16:
      if (run_end > std::numeric_limits<int16_t>::max()) {
        return Status::Invalid("Array construction with int16 run end type cannot fit ",
                               run_end);
      }
      return std::make_shared<Int16Scalar>(static_cast<int16_t>(run_end));
    case Type::INT32:
      if (run_end > std::numeric_limits<int32_t>::max()) {
        return Status::Invalid("Array construction with int32 run end type cannot fit ",
                               run_end);
      }
      return std::make_shared<Int32Scalar>(static_cast<int32_t>(run_end));
    default:
      break;
  }
  DCHECK_EQ(run_end_type.id(), Type::INT64);
  return std::make_shared<Int64Scalar>(run_end);
}

class NullArrayFactory {
 public:
  // For most types, every buffer in an entirely null array will contain nothing but
  // zeroes. For arrays of such types, we can allocate a single buffer and use that in
  // every position of the array data. The first stage of visitation handles assessment
  // of this buffer's size, the second uses the resulting buffer to build the null array.
  //
  // The first stage may not allocate from the MemoryPool or raise a failing status.
  //
  // In the second stage, `zero_buffer_` has been allocated and `out_` has:
  // - type = type_
  // - length = length_
  // - null_count = length_ unless current output may have direct nulls,
  //                0 otherwise
  // - offset = 0
  // - buffers = []
  // - child_data = [nullptr] * type.num_fields()
  // - dictionary = nullptr
  const bool presizing_zero_buffer_;

  NullArrayFactory(const std::shared_ptr<DataType>& type, bool nullable, int64_t length)
      : presizing_zero_buffer_{true},
        type_{type},
        nullable_{nullable},
        length_{length},
        zero_buffer_length_{MayHaveDirectNulls() ? bit_util::BytesForBits(length) : 0} {}

  NullArrayFactory(const std::shared_ptr<DataType>& type, bool nullable, int64_t length,
                   const std::shared_ptr<Buffer>& zero_buffer, MemoryPool* pool)
      : presizing_zero_buffer_{false},
        type_{type},
        nullable_{nullable},
        length_{length},
        zero_buffer_length_{MayHaveDirectNulls() ? bit_util::BytesForBits(length) : 0},
        zero_buffer_{&zero_buffer},
        pool_{pool} {}

  template <typename... Args>
  explicit NullArrayFactory(const std::shared_ptr<Field>& field, const Args&... args)
      : NullArrayFactory{field->type(), field->nullable(), args...} {}

  bool MayHaveDirectNulls() const {
    if (type_->storage_id() == Type::NA) return true;
    return nullable_ && internal::HasValidityBitmap(type_->storage_id());
  }

  void ZeroBufferMustBeAtLeast(int64_t length) {
    DCHECK(presizing_zero_buffer_);
    zero_buffer_length_ = std::max(zero_buffer_length_, length);
  }

  std::shared_ptr<Buffer> GetValidityBitmap() const {
    DCHECK(!presizing_zero_buffer_);
    return MayHaveDirectNulls() ? *zero_buffer_ : nullptr;
  }

  static int64_t GetZeroBufferLength(const std::shared_ptr<DataType>& type, bool nullable,
                                     int64_t length) {
    NullArrayFactory factory{type, nullable, length};
    DCHECK_OK(VisitTypeInline(*type, &factory));
    return factory.zero_buffer_length_;
  }

  static int64_t GetZeroBufferLength(const std::shared_ptr<Field>& field,
                                     int64_t length) {
    return GetZeroBufferLength(field->type(), field->nullable(), length);
  }

  Status Visit(const NullType&) {
    if (presizing_zero_buffer_) {
      // null needs no buffers; don't touch the zero buffer size
    } else {
      out_->buffers = {nullptr};
    }
    return Status::OK();
  }

  Status Visit(const BooleanType& type) {
    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(bit_util::BytesForBits(length_));
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap(), *zero_buffer_};
    return Status::OK();
  }

  Status Visit(const FixedWidthType& type) {
    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(type.byte_width() * length_);
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap(), *zero_buffer_};
    return Status::OK();
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    if (presizing_zero_buffer_) {
      // values buffer may be empty, but there must be at least one offset of 0
      ZeroBufferMustBeAtLeast(sizeof(typename T::offset_type) * (length_ + 1));
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap(), *zero_buffer_, *zero_buffer_};
    return Status::OK();
  }

  Status Visit(const BinaryViewType&) {
    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(sizeof(BinaryViewType::c_type) * length_);
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap(), *zero_buffer_};
    return Status::OK();
  }

  template <typename T>
  enable_if_var_length_list_like<T, Status> Visit(const T& type) {
    constexpr bool kIsView = is_list_view(T::type_id);
    if (presizing_zero_buffer_) {
      auto offsets_length = length_;
      if constexpr (!kIsView) {
        // there must be at least one offset of 0
        offsets_length += 1;
      }
      ZeroBufferMustBeAtLeast(sizeof(typename T::offset_type) * offsets_length);
      // include length required for zero length child
      ZeroBufferMustBeAtLeast(GetZeroBufferLength(type.value_field(), 0));
      return Status::OK();
    }
    if constexpr (!kIsView) {
      out_->buffers = {GetValidityBitmap(), *zero_buffer_};
    } else {
      out_->buffers = {GetValidityBitmap(), *zero_buffer_, *zero_buffer_};
    }
    return CreateChild(0, /*length=*/0);
  }

  Status Visit(const FixedSizeListType& type) {
    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(
          GetZeroBufferLength(type.value_field(), type.list_size() * length_));
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap()};
    return CreateChild(0, type.list_size() * length_);
  }

  Status Visit(const StructType& type) {
    if (presizing_zero_buffer_) {
      for (const auto& child : type.fields()) {
        ZeroBufferMustBeAtLeast(GetZeroBufferLength(child, length_));
      }
      return Status::OK();
    }
    out_->buffers = {GetValidityBitmap()};
    for (int i = 0; i < type.num_fields(); ++i) {
      RETURN_NOT_OK(CreateChild(i, length_));
    }
    return Status::OK();
  }

  static Result<int8_t> GetIdOfFirstNullableUnionMember(const UnionType& type) {
    for (auto [field, id] : Zip(type.fields(), type.type_codes())) {
      if (field->nullable()) return id;
    }
    return Status::TypeError("Cannot produce an array of null ", type,
                             " because no child field is nullable");
  }

  Status Visit(const UnionType& type) {
    // For sparse unions, we create children with the same length as the parent.
    //
    // For dense unions, we create children with length 1 and have offsets which always
    // refer to the first slot from one child.
    int64_t child_length = type.mode() == UnionMode::SPARSE ? length_ : 1;

    if (presizing_zero_buffer_) {
      // type codes
      ZeroBufferMustBeAtLeast(length_);

      if (type.mode() == UnionMode::DENSE) {
        // offsets
        ZeroBufferMustBeAtLeast(sizeof(int32_t) * length_);
      }

      for (const auto& child : type.fields()) {
        ZeroBufferMustBeAtLeast(GetZeroBufferLength(child, child_length));
      }
      return Status::OK();
    }

    // The validity bitmap is always absent for unions
    out_->buffers = {nullptr};

    // Next is the type ids buffer. We may not be able to use zero_buffer_
    // for this since 0 may not be a valid type id.
    ARROW_ASSIGN_OR_RAISE(int8_t first_nullable_id,
                          GetIdOfFirstNullableUnionMember(type));
    if (first_nullable_id == 0) {
      out_->buffers.push_back(*zero_buffer_);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto type_ids, AllocateBuffer(length_, pool_));
      std::memset(type_ids->mutable_data(), first_nullable_id, length_);
      out_->buffers.push_back(std::move(type_ids));
    }

    if (type.mode() == UnionMode::DENSE) {
      // offsets
      out_->buffers.push_back(*zero_buffer_);
    }

    for (int i = 0; i < type.num_fields(); ++i) {
      RETURN_NOT_OK(CreateChild(i, child_length));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // If the dictionary's indices are non-nullable, we can still create an array
    // by creating a dictionary which contains a single null. In this case all
    // indices will be zero to refer to that null.
    int64_t dictionary_length = nullable_ ? 0 : 1;

    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(
          GetZeroBufferLength(type.value_type(), /*nullable=*/true, dictionary_length));
      ZeroBufferMustBeAtLeast(GetZeroBufferLength(type.index_type(), nullable_, length_));
      return Status::OK();
    }

    out_->buffers = {GetValidityBitmap(), *zero_buffer_};
    ARROW_ASSIGN_OR_RAISE(
        out_->dictionary,
        CreateRelated(type.value_type(), /*nullable=*/true, dictionary_length));
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    if (presizing_zero_buffer_) {
      // Run end encoded arrays have no buffers.
      return Status::OK();
    }

    out_->buffers = {nullptr};
    if (!type.field(1)->nullable()) {
      return Status::TypeError("Cannot produce an array of null ", type,
                               " because the values field is not nullable");
    }

    std::shared_ptr<Array> run_ends, values;
    if (length_ == 0) {
      ARROW_ASSIGN_OR_RAISE(run_ends, MakeEmptyArray(type.run_end_type(), pool_));
      ARROW_ASSIGN_OR_RAISE(values, MakeEmptyArray(type.value_type(), pool_));
    } else {
      ARROW_ASSIGN_OR_RAISE(auto run_end_scalar,
                            MakeScalarForRunEndValue(*type.run_end_type(), length_));
      ARROW_ASSIGN_OR_RAISE(run_ends, MakeArrayFromScalar(*run_end_scalar, 1, pool_));
      ARROW_ASSIGN_OR_RAISE(values, MakeArrayOfNull(type.value_type(), 1, pool_));
    }
    out_->child_data = {run_ends->data(), values->data()};
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    if (presizing_zero_buffer_) {
      ZeroBufferMustBeAtLeast(
          GetZeroBufferLength(type.storage_type(), nullable_, length_));
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(out_, CreateRelated(type.storage_type(), nullable_, length_));
    out_->type = type_;
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("construction of all-null ", type);
  }

  Result<std::shared_ptr<ArrayData>> Create() && {
    DCHECK(!presizing_zero_buffer_);
    out_ = std::make_shared<ArrayData>();
    out_->type = type_;
    out_->length = length_;
    out_->null_count = MayHaveDirectNulls() ? length_ : 0;
    out_->offset = 0;
    out_->buffers = {};
    out_->child_data.resize(type_->storage_type()->num_fields());
    out_->dictionary = nullptr;
    RETURN_NOT_OK(VisitTypeInline(*type_, this));
    return std::move(out_);
  }

  Status CreateChild(int i, int64_t length) {
    DCHECK(!presizing_zero_buffer_);
    const auto& field = type_->storage_type()->field(i);
    ARROW_ASSIGN_OR_RAISE(out_->child_data[i],
                          CreateRelated(field->type(), field->nullable(), length));
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> CreateRelated(
      const std::shared_ptr<DataType>& related_type, bool nullable, int64_t length) {
    return NullArrayFactory{related_type, nullable, length, *zero_buffer_, pool_}
        .Create();
  }

  const std::shared_ptr<DataType>& type_;
  bool nullable_;
  int64_t length_;

  int64_t zero_buffer_length_ = 0;

  const std::shared_ptr<Buffer>* zero_buffer_;
  MemoryPool* pool_;

  std::shared_ptr<ArrayData> out_;
};

class RepeatedArrayFactory {
 public:
  RepeatedArrayFactory(MemoryPool* pool, const Scalar& scalar, int64_t length)
      : pool_(pool), scalar_(scalar), length_(length) {}

  template <typename T>
  const auto& scalar() const {
    return checked_cast<const typename TypeTraits<T>::ScalarType&>(scalar_);
  }

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
    bit_util::SetBitsTo(buffer->mutable_data(), 0, length_,
                        checked_cast<const BooleanScalar&>(scalar_).value);
    out_ = std::make_shared<BooleanArray>(length_, buffer);
    return Status::OK();
  }

  template <typename T>
  enable_if_t<is_number_type<T>::value || is_temporal_type<T>::value, Status> Visit(
      const T&) {
    auto value = scalar<T>().value;
    return FinishFixedWidth(&value, sizeof(value));
  }

  Status Visit(const FixedSizeBinaryType& type) {
    auto value = checked_cast<const FixedSizeBinaryScalar&>(scalar_).value;
    return FinishFixedWidth(value->data(), type.byte_width());
  }

  template <typename T>
  enable_if_decimal<T, Status> Visit(const T&) {
    auto value = scalar<T>().value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  Status Visit(const Decimal256Type&) {
    auto value = checked_cast<const Decimal256Scalar&>(scalar_).value.ToBytes();
    return FinishFixedWidth(value.data(), value.size());
  }

  template <typename T>
  enable_if_base_binary<T, Status> Visit(const T&) {
    const std::shared_ptr<Buffer>& value = scalar<T>().value;
    std::shared_ptr<Buffer> values_buffer, offsets_buffer;
    RETURN_NOT_OK(CreateBufferOf(value->data(), value->size(), &values_buffer));
    auto size = static_cast<typename T::offset_type>(value->size());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));
    out_ = std::make_shared<typename TypeTraits<T>::ArrayType>(
        length_, std::move(offsets_buffer), std::move(values_buffer));
    return Status::OK();
  }

  template <typename T>
  enable_if_binary_view_like<T, Status> Visit(const T& type) {
    std::string_view value{*scalar<T>().value};
    auto s = util::ToBinaryView(value, 0, 0);
    RETURN_NOT_OK(FinishFixedWidth(&s, sizeof(s)));
    if (!s.is_inline()) {
      out_->data()->buffers.push_back(scalar<T>().value);
    }
    return Status::OK();
  }

  template <typename T>
  enable_if_var_size_list<T, Status> Visit(const T& type) {
    using ArrayType = typename TypeTraits<T>::ArrayType;

    ArrayVector values(length_, scalar<T>().value);
    ARROW_ASSIGN_OR_RAISE(auto value_array, Concatenate(values, pool_));

    std::shared_ptr<Buffer> offsets_buffer;
    auto size = static_cast<typename T::offset_type>(scalar<T>().value->length());
    RETURN_NOT_OK(CreateOffsetsBuffer(size, &offsets_buffer));
    out_ =
        std::make_shared<ArrayType>(scalar_.type, length_, offsets_buffer, value_array);
    return Status::OK();
  }

  template <typename T>
  enable_if_list_view<T, Status> Visit(const T& type) {
    using ScalarType = typename TypeTraits<T>::ScalarType;
    using ArrayType = typename TypeTraits<T>::ArrayType;

    auto value = checked_cast<const ScalarType&>(scalar_).value;

    auto size = static_cast<typename T::offset_type>(value->length());
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          CreateIntBuffer<typename T::offset_type>(0));
    ARROW_ASSIGN_OR_RAISE(auto sizes_buffer,
                          CreateIntBuffer<typename T::offset_type>(size));
    out_ = std::make_shared<ArrayType>(scalar_.type, length_, std::move(offsets_buffer),
                                       std::move(sizes_buffer), value);
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

  Status Visit(const SparseUnionType& type) {
    const auto& union_scalar = checked_cast<const SparseUnionScalar&>(scalar_);
    const auto scalar_type_code = union_scalar.type_code;

    // Create child arrays: most of them are all-null, except for the child array
    // for the given type code (if the scalar is valid).
    ArrayVector fields;
    for (int i = 0; i < type.num_fields(); ++i) {
      fields.emplace_back();
      if (i == union_scalar.child_id && scalar_.is_valid) {
        ARROW_ASSIGN_OR_RAISE(
            fields.back(), MakeArrayFromScalar(*union_scalar.value[i], length_, pool_));
      } else {
        ARROW_ASSIGN_OR_RAISE(fields.back(),
                              MakeArrayOfNull(type.field(i)->type(), length_, pool_));
      }
    }

    ARROW_ASSIGN_OR_RAISE(auto type_codes_buffer, CreateUnionTypeCodes(scalar_type_code));

    out_ = std::make_shared<SparseUnionArray>(scalar_.type, length_, std::move(fields),
                                              std::move(type_codes_buffer));
    return Status::OK();
  }

  Status Visit(const DenseUnionType& type) {
    const auto& union_scalar = checked_cast<const DenseUnionScalar&>(scalar_);
    const auto& union_type = checked_cast<const UnionType&>(*scalar_.type);
    const auto scalar_type_code = union_scalar.type_code;
    const auto scalar_child_id = union_type.child_ids()[scalar_type_code];

    // Create child arrays: all of them are empty, except for the child array
    // for the given type code (if length > 0).
    ArrayVector fields;
    for (int i = 0; i < type.num_fields(); ++i) {
      fields.emplace_back();
      if (i == scalar_child_id && length_ > 0) {
        if (scalar_.is_valid) {
          // One valid element (will be referenced by multiple offsets)
          ARROW_ASSIGN_OR_RAISE(fields.back(),
                                MakeArrayFromScalar(*union_scalar.value, 1, pool_));
        } else {
          // One null element (will be referenced by multiple offsets)
          ARROW_ASSIGN_OR_RAISE(fields.back(),
                                MakeArrayOfNull(union_type.field(i)->type(), 1, pool_));
        }
      } else {
        // Zero element (will not be referenced by any offset)
        ARROW_ASSIGN_OR_RAISE(fields.back(),
                              MakeArrayOfNull(union_type.field(i)->type(), 0, pool_));
      }
    }

    // Create an offsets buffer with all offsets equal to 0
    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer,
                          AllocateBuffer(length_ * sizeof(int32_t), pool_));
    memset(offsets_buffer->mutable_data(), 0, offsets_buffer->size());

    ARROW_ASSIGN_OR_RAISE(auto type_codes_buffer, CreateUnionTypeCodes(scalar_type_code));

    out_ = std::make_shared<DenseUnionArray>(scalar_.type, length_, std::move(fields),
                                             std::move(type_codes_buffer),
                                             std::move(offsets_buffer));
    return Status::OK();
  }

  Status Visit(const RunEndEncodedType& type) {
    const auto& ree_scalar = checked_cast<const RunEndEncodedScalar&>(scalar_);
    ARROW_ASSIGN_OR_RAISE(auto values,
                          ree_scalar.is_valid
                              ? MakeArrayFromScalar(*ree_scalar.value, 1, pool_)
                              : MakeArrayOfNull(ree_scalar.value_type(), 1, pool_));
    ARROW_ASSIGN_OR_RAISE(auto run_end_scalar,
                          MakeScalarForRunEndValue(*ree_scalar.run_end_type(), length_));
    ARROW_ASSIGN_OR_RAISE(auto run_ends, MakeArrayFromScalar(*run_end_scalar, 1, pool_));
    ARROW_ASSIGN_OR_RAISE(out_, RunEndEncodedArray::Make(length_, run_ends, values));
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    return Status::NotImplemented("construction from scalar of type ", *scalar_.type);
  }

  Result<std::shared_ptr<Buffer>> CreateUnionTypeCodes(int8_t type_code) {
    TypedBufferBuilder<int8_t> builder(pool_);
    RETURN_NOT_OK(builder.Resize(length_));
    builder.UnsafeAppend(length_, type_code);
    return builder.Finish();
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

  template <typename IntType>
  Result<std::shared_ptr<Buffer>> CreateIntBuffer(IntType value) {
    std::shared_ptr<Buffer> buffer;
    TypedBufferBuilder<IntType> builder(pool_);
    RETURN_NOT_OK(builder.Append(/*num_copies=*/length_, value));
    RETURN_NOT_OK(builder.Finish(&buffer));
    return buffer;
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
  int64_t zero_buffer_length =
      NullArrayFactory::GetZeroBufferLength(type, /*nullable=*/true, length);
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> zero_buffer,
                        AllocateBuffer(zero_buffer_length, pool));
  std::memset(zero_buffer->mutable_data(), 0, zero_buffer->size());
  return NullArrayFactory{type, /*nullable=*/true, length, zero_buffer, pool}
      .Create()
      .Map(MakeArray);
}

Result<std::shared_ptr<Array>> MakeArrayFromScalar(const Scalar& scalar, int64_t length,
                                                   MemoryPool* pool) {
  // Null union scalars still have a type code associated
  if (!scalar.is_valid && !is_union(scalar.type->id())) {
    return MakeArrayOfNull(scalar.type, length, pool);
  }
  return RepeatedArrayFactory(pool, scalar, length).Create();
}

Result<std::shared_ptr<Array>> MakeEmptyArray(std::shared_ptr<DataType> type,
                                              MemoryPool* memory_pool) {
  if (type->id() == Type::EXTENSION) {
    const auto& ext_type = checked_cast<const ExtensionType&>(*type);
    ARROW_ASSIGN_OR_RAISE(auto storage,
                          MakeEmptyArray(ext_type.storage_type(), memory_pool));
    storage->data()->type = std::move(type);
    return ext_type.MakeArray(storage->data());
  }
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(memory_pool, type, &builder));
  RETURN_NOT_OK(builder->Resize(0));
  return builder->Finish();
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
