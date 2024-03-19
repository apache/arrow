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

#include "arrow/array/array_nested.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::CopyBitmap;

// ----------------------------------------------------------------------
// ListArray / LargeListArray / ListViewArray / LargeListViewArray (common utilities)

namespace {

/// \brief Clean offsets when their null_count is greater than 0
///
/// \pre offsets.null_count() > 0
template <typename TYPE>
Result<BufferVector> CleanListOffsets(const std::shared_ptr<Buffer>& validity_buffer,
                                      const Array& offsets, MemoryPool* pool) {
  using offset_type = typename TYPE::offset_type;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;
  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;

  DCHECK_GT(offsets.null_count(), 0);
  const int64_t num_offsets = offsets.length();

  if (!offsets.IsValid(num_offsets - 1)) {
    return Status::Invalid("Last list offset should be non-null");
  }

  ARROW_ASSIGN_OR_RAISE(auto clean_offsets,
                        AllocateBuffer(num_offsets * sizeof(offset_type), pool));

  // Copy valid bits, ignoring the final offset (since for a length N list array,
  // we have N + 1 offsets)
  ARROW_ASSIGN_OR_RAISE(
      auto clean_validity_buffer,
      CopyBitmap(pool, offsets.null_bitmap()->data(), offsets.offset(), num_offsets - 1));

  const offset_type* raw_offsets =
      checked_cast<const OffsetArrayType&>(offsets).raw_values();
  auto clean_raw_offsets = reinterpret_cast<offset_type*>(clean_offsets->mutable_data());

  // Must work backwards so we can tell how many values were in the last non-null value
  offset_type current_offset = raw_offsets[num_offsets - 1];
  for (int64_t i = num_offsets - 1; i >= 0; --i) {
    if (offsets.IsValid(i)) {
      current_offset = raw_offsets[i];
    }
    clean_raw_offsets[i] = current_offset;
  }

  return BufferVector({std::move(clean_validity_buffer), std::move(clean_offsets)});
}

template <typename TYPE>
Result<std::shared_ptr<typename TypeTraits<TYPE>::ArrayType>> ListArrayFromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& values,
    MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap = NULLPTR,
    int64_t null_count = kUnknownNullCount) {
  using offset_type = typename TYPE::offset_type;
  using ArrayType = typename TypeTraits<TYPE>::ArrayType;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;

  if (offsets.length() == 0) {
    return Status::Invalid("List offsets must have non-zero length");
  }

  if (offsets.type_id() != OffsetArrowType::type_id) {
    return Status::TypeError("List offsets must be ", OffsetArrowType::type_name());
  }

  if (null_bitmap != nullptr && offsets.null_count() > 0) {
    return Status::Invalid(
        "Ambiguous to specify both validity map and offsets with nulls");
  }

  if (null_bitmap != nullptr && offsets.offset() != 0) {
    return Status::NotImplemented("Null bitmap with offsets slice not supported.");
  }

  // Clean the offsets if they contain nulls.
  if (offsets.null_count() > 0) {
    ARROW_ASSIGN_OR_RAISE(auto buffers,
                          CleanListOffsets<TYPE>(null_bitmap, offsets, pool));
    auto data = ArrayData::Make(type, offsets.length() - 1, std::move(buffers),
                                {values.data()}, offsets.null_count(), /*offset=*/0);
    return std::make_shared<ArrayType>(std::move(data));
  }

  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;
  const auto& typed_offsets = checked_cast<const OffsetArrayType&>(offsets);
  auto buffers = BufferVector({std::move(null_bitmap), typed_offsets.values()});
  auto data = ArrayData::Make(type, offsets.length() - 1, std::move(buffers),
                              {values.data()}, null_count, offsets.offset());
  return std::make_shared<ArrayType>(std::move(data));
}

template <typename TYPE>
Result<std::shared_ptr<typename TypeTraits<TYPE>::ArrayType>> ListViewArrayFromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& sizes,
    const Array& values, MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap = NULLPTR,
    int64_t null_count = kUnknownNullCount) {
  using offset_type = typename TYPE::offset_type;
  using ArrayType = typename TypeTraits<TYPE>::ArrayType;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;

  if (offsets.type_id() != OffsetArrowType::type_id) {
    return Status::TypeError("List offsets must be ", OffsetArrowType::type_name());
  }

  if (sizes.length() != offsets.length() && sizes.length() != offsets.length() - 1) {
    return Status::Invalid(
        "List sizes must have the same length as offsets or one less than offsets");
  }
  if (sizes.type_id() != OffsetArrowType::type_id) {
    return Status::TypeError("List sizes must be ", OffsetArrowType::type_name());
  }

  if (offsets.offset() != sizes.offset()) {
    return Status::Invalid("List offsets and sizes must have the same offset");
  }
  const int64_t array_offset = sizes.offset();

  if (null_bitmap) {
    if (offsets.null_count() > 0 || sizes.null_count() > 0) {
      return Status::Invalid(
          "Ambiguous to specify both validity map and offsets or sizes with nulls");
    }
    if (array_offset != 0) {
      return Status::Invalid(
          "List offsets and sizes must not be slices if a validity map is specified");
    }
  } else {
    if (offsets.null_count() > 0 && sizes.null_count() > 0) {
      return Status::Invalid("Ambiguous to specify both offsets and sizes with nulls");
    }
  }

  DCHECK(offsets.length() == sizes.length() || offsets.length() - 1 == sizes.length());

  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;
  const auto& typed_offsets = checked_cast<const OffsetArrayType&>(offsets);
  const auto& typed_sizes = checked_cast<const OffsetArrayType&>(sizes);

  auto derived_validity_buffer = std::move(null_bitmap);
  if (offsets.null_count() > 0) {
    derived_validity_buffer = offsets.null_bitmap();
    null_count = offsets.null_count();
    // We allow construction from an offsets array containing one extra value.
    // If that is the case, we might need to discount one null from out_null_count.
    if (offsets.length() - 1 == sizes.length() && !offsets.IsValid(sizes.length())) {
      null_count -= 1;
    }
  } else if (sizes.null_count() > 0) {
    derived_validity_buffer = sizes.null_bitmap();
    null_count = sizes.null_count();
  }

  auto buffers = BufferVector({
      std::move(derived_validity_buffer),
      typed_offsets.values(),
      typed_sizes.values(),
  });
  auto data = ArrayData::Make(type, sizes.length(), std::move(buffers), {values.data()},
                              null_count, array_offset);
  return std::make_shared<ArrayType>(std::move(data));
}

static std::shared_ptr<Array> SliceArrayWithOffsets(const Array& array, int64_t begin,
                                                    int64_t end) {
  return array.Slice(begin, end - begin);
}

template <typename ListArrayT>
Result<std::shared_ptr<Array>> FlattenListArray(const ListArrayT& list_array,
                                                MemoryPool* memory_pool) {
  const int64_t list_array_length = list_array.length();
  std::shared_ptr<arrow::Array> value_array = list_array.values();

  // Shortcut: if a ListArray does not contain nulls, then simply slice its
  // value array with the first and the last offsets.
  if (list_array.null_count() == 0) {
    return SliceArrayWithOffsets(*value_array, list_array.value_offset(0),
                                 list_array.value_offset(list_array_length));
  }

  // Second shortcut: if the list array is *all* nulls, then just return
  // an empty array.
  if (list_array.null_count() == list_array.length()) {
    return MakeEmptyArray(value_array->type(), memory_pool);
  }

  // The ListArray contains nulls: there may be a non-empty sub-list behind
  // a null and it must not be contained in the result.
  std::vector<std::shared_ptr<Array>> non_null_fragments;
  int64_t valid_begin = 0;
  while (valid_begin < list_array_length) {
    int64_t valid_end = valid_begin;
    while (valid_end < list_array_length &&
           (list_array.IsValid(valid_end) || list_array.value_length(valid_end) == 0)) {
      ++valid_end;
    }
    if (valid_begin < valid_end) {
      non_null_fragments.push_back(
          SliceArrayWithOffsets(*value_array, list_array.value_offset(valid_begin),
                                list_array.value_offset(valid_end)));
    }
    valid_begin = valid_end + 1;  // skip null entry
  }

  // Final attempt to avoid invoking Concatenate().
  if (non_null_fragments.size() == 1) {
    return non_null_fragments[0];
  } else if (non_null_fragments.size() == 0) {
    return MakeEmptyArray(value_array->type(), memory_pool);
  }

  return Concatenate(non_null_fragments, memory_pool);
}

template <typename ListViewArrayT, bool HasNulls>
Result<std::shared_ptr<Array>> FlattenListViewArray(const ListViewArrayT& list_view_array,
                                                    MemoryPool* memory_pool) {
  using offset_type = typename ListViewArrayT::offset_type;
  const int64_t list_view_array_offset = list_view_array.offset();
  const int64_t list_view_array_length = list_view_array.length();
  std::shared_ptr<arrow::Array> value_array = list_view_array.values();

  if (list_view_array_length == 0) {
    return SliceArrayWithOffsets(*value_array, 0, 0);
  }

  // If the list array is *all* nulls, then just return an empty array.
  if constexpr (HasNulls) {
    if (list_view_array.null_count() == list_view_array.length()) {
      return MakeEmptyArray(value_array->type(), memory_pool);
    }
  }

  const auto* validity = list_view_array.data()->template GetValues<uint8_t>(0, 0);
  const auto* offsets = list_view_array.data()->template GetValues<offset_type>(1);
  const auto* sizes = list_view_array.data()->template GetValues<offset_type>(2);

  auto is_null_or_empty = [&](int64_t i) {
    if (HasNulls && !bit_util::GetBit(validity, list_view_array_offset + i)) {
      return true;
    }
    return sizes[i] == 0;
  };

  // Index of the first valid, non-empty list-view.
  int64_t first_i = 0;
  for (; first_i < list_view_array_length; first_i++) {
    if (!is_null_or_empty(first_i)) {
      break;
    }
  }
  // If all list-views are empty, return an empty array.
  if (first_i == list_view_array_length) {
    return MakeEmptyArray(value_array->type(), memory_pool);
  }

  std::vector<std::shared_ptr<Array>> slices;
  {
    int64_t i = first_i;
    auto begin_offset = offsets[i];
    auto end_offset = offsets[i] + sizes[i];
    i += 1;
    // Inductive invariant: slices and the always non-empty values slice
    // [begin_offset, end_offset) contains all the maximally contiguous slices of the
    // values array that are covered by all the list-views before list-view i.
    for (; i < list_view_array_length; i++) {
      if (is_null_or_empty(i)) {
        // The invariant is preserved by simply preserving the current set of slices.
      } else {
        if (offsets[i] == end_offset) {
          end_offset += sizes[i];
          // The invariant is preserved because since the non-empty list-view i
          // starts at end_offset, the current range can be extended to end at
          // offsets[i] + sizes[i] (the same as end_offset + sizes[i]).
        } else {
          // The current slice can't be extended because the list-view i either
          // shares values with the current slice or starts after the position
          // immediately after the end of the current slice.
          slices.push_back(SliceArrayWithOffsets(*value_array, begin_offset, end_offset));
          begin_offset = offsets[i];
          end_offset = offsets[i] + sizes[i];
          // The invariant is preserved because a maximally contiguous slice of
          // the values array (i.e. one that can't be extended) was added to slices
          // and [begin_offset, end_offset) is non-empty and contains the
          // current list-view i.
        }
      }
    }
    slices.push_back(SliceArrayWithOffsets(*value_array, begin_offset, end_offset));
  }

  // Final attempt to avoid invoking Concatenate().
  switch (slices.size()) {
    case 0:
      return MakeEmptyArray(value_array->type(), memory_pool);
    case 1:
      return slices[0];
  }

  return Concatenate(slices, memory_pool);
}

std::shared_ptr<Array> BoxOffsets(const std::shared_ptr<DataType>& boxed_type,
                                  const ArrayData& data) {
  const int64_t num_offsets =
      is_list_view(data.type->id()) ? data.length : data.length + 1;
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, data.buffers[1]};
  auto offsets_data =
      std::make_shared<ArrayData>(boxed_type, /*length=*/num_offsets, std::move(buffers),
                                  /*null_count=*/0, data.offset);
  return MakeArray(offsets_data);
}

std::shared_ptr<Array> BoxSizes(const std::shared_ptr<DataType>& boxed_type,
                                const ArrayData& data) {
  DCHECK(is_list_view(data.type->id()));
  std::vector<std::shared_ptr<Buffer>> buffers = {nullptr, data.buffers[2]};
  auto sizes_data =
      std::make_shared<ArrayData>(boxed_type, data.length, std::move(buffers),
                                  /*null_count=*/0, data.offset);
  return MakeArray(sizes_data);
}

template <typename DestListViewType, typename SrcListType>
Result<std::shared_ptr<ArrayData>> ListViewFromListImpl(
    const std::shared_ptr<ArrayData>& list_data, MemoryPool* pool) {
  static_assert(
      std::is_same<typename SrcListType::offset_type,
                   typename DestListViewType::offset_type>::value,
      "Offset types between list type and list-view type are expected to match");
  using offset_type = typename SrcListType::offset_type;
  const auto& list_type = checked_cast<const SrcListType&>(*list_data->type);

  // To re-use the validity and offsets buffers, a sizes buffer with enough
  // padding on the beginning is allocated and filled with the sizes after
  // list_data->offset.
  const int64_t buffer_length = list_data->offset + list_data->length;
  ARROW_ASSIGN_OR_RAISE(auto sizes_buffer,
                        AllocateBuffer(buffer_length * sizeof(offset_type), pool));
  const auto* offsets = list_data->template GetValues<offset_type>(1, 0);
  auto* sizes = sizes_buffer->mutable_data_as<offset_type>();
  // Zero the initial padding area to avoid leaking any data when buffers are
  // sent over IPC or through the C Data interface.
  memset(sizes, 0, list_data->offset * sizeof(offset_type));
  for (int64_t i = list_data->offset; i < buffer_length; i++) {
    sizes[i] = offsets[i + 1] - offsets[i];
  }
  BufferVector buffers = {list_data->buffers[0], list_data->buffers[1],
                          std::move(sizes_buffer)};

  return ArrayData::Make(std::make_shared<DestListViewType>(list_type.value_type()),
                         list_data->length, std::move(buffers),
                         {list_data->child_data[0]}, list_data->null_count,
                         list_data->offset);
}

template <typename DestListType, typename SrcListViewType>
Result<std::shared_ptr<ArrayData>> ListFromListViewImpl(
    const std::shared_ptr<ArrayData>& list_view_data, MemoryPool* pool) {
  static_assert(
      std::is_same<typename SrcListViewType::offset_type,
                   typename DestListType::offset_type>::value,
      "Offset types between list type and list-view type are expected to match");
  using offset_type = typename DestListType::offset_type;
  using ListBuilderType = typename TypeTraits<DestListType>::BuilderType;

  const auto& list_view_type =
      checked_cast<const SrcListViewType&>(*list_view_data->type);
  const auto& value_type = list_view_type.value_type();
  const auto list_type = std::make_shared<DestListType>(value_type);

  ARROW_ASSIGN_OR_RAISE(auto sum_of_list_view_sizes,
                        list_util::internal::SumOfLogicalListSizes(*list_view_data));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayBuilder> value_builder,
                        MakeBuilder(value_type, pool));
  RETURN_NOT_OK(value_builder->Reserve(sum_of_list_view_sizes));
  auto list_builder = std::make_shared<ListBuilderType>(pool, value_builder, list_type);
  RETURN_NOT_OK(list_builder->Reserve(list_view_data->length));

  ArraySpan values{*list_view_data->child_data[0]};
  const auto* in_validity_bitmap = list_view_data->GetValues<uint8_t>(0);
  const auto* in_offsets = list_view_data->GetValues<offset_type>(1);
  const auto* in_sizes = list_view_data->GetValues<offset_type>(2);
  for (int64_t i = 0; i < list_view_data->length; ++i) {
    const bool is_valid =
        !in_validity_bitmap ||
        bit_util::GetBit(in_validity_bitmap, list_view_data->offset + i);
    const int64_t size = is_valid ? in_sizes[i] : 0;
    RETURN_NOT_OK(list_builder->Append(is_valid, size));
    RETURN_NOT_OK(value_builder->AppendArraySlice(values, in_offsets[i], size));
  }
  std::shared_ptr<ArrayData> list_array_data;
  RETURN_NOT_OK(list_builder->FinishInternal(&list_array_data));
  return list_array_data;
}

}  // namespace

namespace internal {

template <typename TYPE>
inline void SetListData(VarLengthListLikeArray<TYPE>* self,
                        const std::shared_ptr<ArrayData>& data,
                        Type::type expected_type_id) {
  ARROW_CHECK_EQ(data->buffers.size(), is_list_view(TYPE::type_id) ? 3 : 2);
  ARROW_CHECK_EQ(data->type->id(), expected_type_id);
  ARROW_CHECK_EQ(data->child_data.size(), 1);

  self->Array::SetData(data);

  self->list_type_ = checked_cast<const TYPE*>(data->type.get());
  self->raw_value_offsets_ =
      data->GetValuesSafe<typename TYPE::offset_type>(1, /*offset=*/0);
  // BaseListViewArray::SetData takes care of setting raw_value_sizes_.

  ARROW_CHECK_EQ(self->list_type_->value_type()->id(), data->child_data[0]->type->id());
  DCHECK(self->list_type_->value_type()->Equals(data->child_data[0]->type));
  self->values_ = MakeArray(self->data_->child_data[0]);
}

}  // namespace internal

// ----------------------------------------------------------------------
// ListArray

ListArray::ListArray(std::shared_ptr<ArrayData> data) {
  ListArray::SetData(std::move(data));
}

ListArray::ListArray(std::shared_ptr<DataType> type, int64_t length,
                     std::shared_ptr<Buffer> value_offsets, std::shared_ptr<Array> values,
                     std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                     int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::LIST);
  auto internal_data = ArrayData::Make(
      std::move(type), length,
      BufferVector{std::move(null_bitmap), std::move(value_offsets)}, null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  SetData(std::move(internal_data));
}

void ListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  internal::SetListData(this, data);
}

Result<std::shared_ptr<ListArray>> ListArray::FromArrays(
    const Array& offsets, const Array& values, MemoryPool* pool,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  return ListArrayFromArrays<ListType>(std::make_shared<ListType>(values.type()), offsets,
                                       values, pool, null_bitmap, null_count);
}

Result<std::shared_ptr<ListArray>> ListArray::FromListView(const ListViewArray& source,
                                                           MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data, (ListFromListViewImpl<ListType, ListViewType>(source.data(), pool)));
  return std::make_shared<ListArray>(std::move(data));
}

Result<std::shared_ptr<ListArray>> ListArray::FromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& values,
    MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  if (type->id() != Type::LIST) {
    return Status::TypeError("Expected list type, got ", type->ToString());
  }
  const auto& list_type = checked_cast<const ListType&>(*type);
  if (!list_type.value_type()->Equals(values.type())) {
    return Status::TypeError("Mismatching list value type");
  }
  return ListArrayFromArrays<ListType>(std::move(type), offsets, values, pool,
                                       null_bitmap, null_count);
}

Result<std::shared_ptr<Array>> ListArray::Flatten(MemoryPool* memory_pool) const {
  return FlattenListArray(*this, memory_pool);
}

std::shared_ptr<Array> ListArray::offsets() const { return BoxOffsets(int32(), *data_); }

// ----------------------------------------------------------------------
// LargeListArray

LargeListArray::LargeListArray(const std::shared_ptr<ArrayData>& data) {
  LargeListArray::SetData(data);
}

LargeListArray::LargeListArray(const std::shared_ptr<DataType>& type, int64_t length,
                               const std::shared_ptr<Buffer>& value_offsets,
                               const std::shared_ptr<Array>& values,
                               const std::shared_ptr<Buffer>& null_bitmap,
                               int64_t null_count, int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::LARGE_LIST);
  auto internal_data =
      ArrayData::Make(type, length, {null_bitmap, value_offsets}, null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  LargeListArray::SetData(internal_data);
}

void LargeListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  internal::SetListData(this, data);
}

Result<std::shared_ptr<LargeListArray>> LargeListArray::FromArrays(
    const Array& offsets, const Array& values, MemoryPool* pool,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  return ListArrayFromArrays<LargeListType>(
      std::make_shared<LargeListType>(values.type()), offsets, values, pool, null_bitmap,
      null_count);
}

Result<std::shared_ptr<LargeListArray>> LargeListArray::FromListView(
    const LargeListViewArray& source, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      (ListFromListViewImpl<LargeListType, LargeListViewType>(source.data(), pool)));
  return std::make_shared<LargeListArray>(std::move(data));
}

Result<std::shared_ptr<LargeListArray>> LargeListArray::FromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& values,
    MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  if (type->id() != Type::LARGE_LIST) {
    return Status::TypeError("Expected large list type, got ", type->ToString());
  }
  const auto& list_type = checked_cast<const LargeListType&>(*type);
  if (!list_type.value_type()->Equals(values.type())) {
    return Status::TypeError("Mismatching list value type");
  }
  return ListArrayFromArrays<LargeListType>(std::move(type), offsets, values, pool,
                                            null_bitmap, null_count);
}

Result<std::shared_ptr<Array>> LargeListArray::Flatten(MemoryPool* memory_pool) const {
  return FlattenListArray(*this, memory_pool);
}

std::shared_ptr<Array> LargeListArray::offsets() const {
  return BoxOffsets(int64(), *data_);
}

// ----------------------------------------------------------------------
// ListViewArray

ListViewArray::ListViewArray(std::shared_ptr<ArrayData> data) {
  ListViewArray::SetData(std::move(data));
}

ListViewArray::ListViewArray(std::shared_ptr<DataType> type, int64_t length,
                             std::shared_ptr<Buffer> value_offsets,
                             std::shared_ptr<Buffer> value_sizes,
                             std::shared_ptr<Array> values,
                             std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                             int64_t offset) {
  ListViewArray::SetData(ArrayData::Make(
      std::move(type), length,
      {std::move(null_bitmap), std::move(value_offsets), std::move(value_sizes)},
      /*child_data=*/{values->data()}, null_count, offset));
}

void ListViewArray::SetData(const std::shared_ptr<ArrayData>& data) {
  internal::SetListData(this, data);
  raw_value_sizes_ = data->GetValuesSafe<ListViewType::offset_type>(2, /*offset=*/0);
}

Result<std::shared_ptr<ListViewArray>> ListViewArray::FromArrays(
    const Array& offsets, const Array& sizes, const Array& values, MemoryPool* pool,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  return ListViewArrayFromArrays<ListViewType>(
      std::make_shared<ListViewType>(values.type()), offsets, sizes, values, pool,
      null_bitmap, null_count);
}

Result<std::shared_ptr<ListViewArray>> ListViewArray::FromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& sizes,
    const Array& values, MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap,
    int64_t null_count) {
  if (type->id() != Type::LIST_VIEW) {
    return Status::TypeError("Expected list-view type, got ", type->ToString());
  }
  const auto& list_view_type = checked_cast<const ListViewType&>(*type);
  if (!list_view_type.value_type()->Equals(values.type())) {
    return Status::TypeError("Mismatching list-view value type");
  }
  return ListViewArrayFromArrays<ListViewType>(std::move(type), offsets, sizes, values,
                                               pool, null_bitmap, null_count);
}

Result<std::shared_ptr<ListViewArray>> ListViewArray::FromList(const ListArray& source,
                                                               MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data, (ListViewFromListImpl<ListViewType, ListType>(source.data(), pool)));
  return std::make_shared<ListViewArray>(std::move(data));
}

Result<std::shared_ptr<LargeListViewArray>> LargeListViewArray::FromList(
    const LargeListArray& source, MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      (ListViewFromListImpl<LargeListViewType, LargeListType>(source.data(), pool)));
  return std::make_shared<LargeListViewArray>(std::move(data));
}

Result<std::shared_ptr<Array>> ListViewArray::Flatten(MemoryPool* memory_pool) const {
  if (null_count() > 0) {
    return FlattenListViewArray<ListViewArray, true>(*this, memory_pool);
  }
  return FlattenListViewArray<ListViewArray, false>(*this, memory_pool);
}

std::shared_ptr<Array> ListViewArray::offsets() const {
  return BoxOffsets(int32(), *data_);
}

std::shared_ptr<Array> ListViewArray::sizes() const { return BoxSizes(int32(), *data_); }

// ----------------------------------------------------------------------
// LargeListViewArray

LargeListViewArray::LargeListViewArray(std::shared_ptr<ArrayData> data) {
  LargeListViewArray::SetData(std::move(data));
}

LargeListViewArray::LargeListViewArray(std::shared_ptr<DataType> type, int64_t length,
                                       std::shared_ptr<Buffer> value_offsets,
                                       std::shared_ptr<Buffer> value_sizes,
                                       std::shared_ptr<Array> values,
                                       std::shared_ptr<Buffer> null_bitmap,
                                       int64_t null_count, int64_t offset) {
  LargeListViewArray::SetData(ArrayData::Make(
      type, length,
      {std::move(null_bitmap), std::move(value_offsets), std::move(value_sizes)},
      /*child_data=*/{values->data()}, null_count, offset));
}

void LargeListViewArray::SetData(const std::shared_ptr<ArrayData>& data) {
  internal::SetListData(this, data);
  raw_value_sizes_ = data->GetValuesSafe<LargeListViewType::offset_type>(2, /*offset=*/0);
}

Result<std::shared_ptr<LargeListViewArray>> LargeListViewArray::FromArrays(
    const Array& offsets, const Array& sizes, const Array& values, MemoryPool* pool,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  return ListViewArrayFromArrays<LargeListViewType>(
      std::make_shared<LargeListViewType>(values.type()), offsets, sizes, values, pool,
      null_bitmap, null_count);
}

Result<std::shared_ptr<LargeListViewArray>> LargeListViewArray::FromArrays(
    std::shared_ptr<DataType> type, const Array& offsets, const Array& sizes,
    const Array& values, MemoryPool* pool, std::shared_ptr<Buffer> null_bitmap,
    int64_t null_count) {
  if (type->id() != Type::LARGE_LIST_VIEW) {
    return Status::TypeError("Expected large list-view type, got ", type->ToString());
  }
  const auto& large_list_view_type = checked_cast<const LargeListViewType&>(*type);
  if (!large_list_view_type.value_type()->Equals(values.type())) {
    return Status::TypeError("Mismatching large list-view value type");
  }
  return ListViewArrayFromArrays<LargeListViewType>(
      std::move(type), offsets, sizes, values, pool, null_bitmap, null_count);
}

Result<std::shared_ptr<Array>> LargeListViewArray::Flatten(
    MemoryPool* memory_pool) const {
  if (null_count() > 0) {
    return FlattenListViewArray<LargeListViewArray, true>(*this, memory_pool);
  }
  return FlattenListViewArray<LargeListViewArray, false>(*this, memory_pool);
}

std::shared_ptr<Array> LargeListViewArray::offsets() const {
  return BoxOffsets(int64(), *data_);
}

std::shared_ptr<Array> LargeListViewArray::sizes() const {
  return BoxSizes(int64(), *data_);
}

// ----------------------------------------------------------------------
// MapArray

MapArray::MapArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

MapArray::MapArray(const std::shared_ptr<DataType>& type, int64_t length,
                   const std::shared_ptr<Buffer>& offsets,
                   const std::shared_ptr<Array>& values,
                   const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                   int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, offsets}, {values->data()},
                          null_count, offset));
}

MapArray::MapArray(const std::shared_ptr<DataType>& type, int64_t length,
                   BufferVector buffers, const std::shared_ptr<Array>& keys,
                   const std::shared_ptr<Array>& items, int64_t null_count,
                   int64_t offset) {
  auto pair_data = ArrayData::Make(type->fields()[0]->type(), keys->data()->length,
                                   {nullptr}, {keys->data(), items->data()}, 0, offset);
  auto map_data =
      ArrayData::Make(type, length, std::move(buffers), {pair_data}, null_count, offset);
  SetData(map_data);
}

MapArray::MapArray(const std::shared_ptr<DataType>& type, int64_t length,
                   const std::shared_ptr<Buffer>& offsets,
                   const std::shared_ptr<Array>& keys,
                   const std::shared_ptr<Array>& items,
                   const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                   int64_t offset)
    : MapArray(type, length, {null_bitmap, offsets}, keys, items, null_count, offset) {}

Result<std::shared_ptr<Array>> MapArray::FromArraysInternal(
    std::shared_ptr<DataType> type, const std::shared_ptr<Array>& offsets,
    const std::shared_ptr<Array>& keys, const std::shared_ptr<Array>& items,
    MemoryPool* pool) {
  using offset_type = typename MapType::offset_type;
  using OffsetArrowType = typename CTypeTraits<offset_type>::ArrowType;

  if (offsets->length() == 0) {
    return Status::Invalid("Map offsets must have non-zero length");
  }

  if (offsets->type_id() != OffsetArrowType::type_id) {
    return Status::TypeError("Map offsets must be ", OffsetArrowType::type_name());
  }

  if (keys->null_count() != 0) {
    return Status::Invalid("Map cannot contain NULL valued keys");
  }

  if (keys->length() != items->length()) {
    return Status::Invalid("Map key and item arrays must be equal length");
  }

  if (offsets->null_count() > 0) {
    ARROW_ASSIGN_OR_RAISE(auto buffers,
                          CleanListOffsets<MapType>(NULLPTR, *offsets, pool));
    return std::make_shared<MapArray>(type, offsets->length() - 1, std::move(buffers),
                                      keys, items, offsets->null_count(), 0);
  }

  using OffsetArrayType = typename TypeTraits<OffsetArrowType>::ArrayType;
  const auto& typed_offsets = checked_cast<const OffsetArrayType&>(*offsets);
  auto buffers = BufferVector({nullptr, typed_offsets.values()});
  return std::make_shared<MapArray>(type, offsets->length() - 1, std::move(buffers), keys,
                                    items, /*null_count=*/0, offsets->offset());
}

Result<std::shared_ptr<Array>> MapArray::FromArrays(const std::shared_ptr<Array>& offsets,
                                                    const std::shared_ptr<Array>& keys,
                                                    const std::shared_ptr<Array>& items,
                                                    MemoryPool* pool) {
  return FromArraysInternal(std::make_shared<MapType>(keys->type(), items->type()),
                            offsets, keys, items, pool);
}

Result<std::shared_ptr<Array>> MapArray::FromArrays(std::shared_ptr<DataType> type,
                                                    const std::shared_ptr<Array>& offsets,
                                                    const std::shared_ptr<Array>& keys,
                                                    const std::shared_ptr<Array>& items,
                                                    MemoryPool* pool) {
  if (type->id() != Type::MAP) {
    return Status::TypeError("Expected map type, got ", type->ToString());
  }
  const auto& map_type = checked_cast<const MapType&>(*type);
  if (!map_type.key_type()->Equals(keys->type())) {
    return Status::TypeError("Mismatching map keys type");
  }
  if (!map_type.item_type()->Equals(items->type())) {
    return Status::TypeError("Mismatching map items type");
  }
  return FromArraysInternal(std::move(type), offsets, keys, items, pool);
}

Status MapArray::ValidateChildData(
    const std::vector<std::shared_ptr<ArrayData>>& child_data) {
  if (child_data.size() != 1) {
    return Status::Invalid("Expected one child array for map array");
  }
  const auto& pair_data = child_data[0];
  if (pair_data->type->id() != Type::STRUCT) {
    return Status::Invalid("Map array child array should have struct type");
  }
  if (pair_data->null_count != 0) {
    return Status::Invalid("Map array child array should have no nulls");
  }
  if (pair_data->child_data.size() != 2) {
    return Status::Invalid("Map array child array should have two fields");
  }
  if (pair_data->child_data[0]->null_count != 0) {
    return Status::Invalid("Map array keys array should have no nulls");
  }
  return Status::OK();
}

void MapArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_OK(ValidateChildData(data->child_data));

  internal::SetListData(this, data, Type::MAP);
  map_type_ = checked_cast<const MapType*>(data->type.get());
  const auto& pair_data = data->child_data[0];
  keys_ = MakeArray(pair_data->child_data[0]);
  items_ = MakeArray(pair_data->child_data[1]);
}

// ----------------------------------------------------------------------
// FixedSizeListArray

FixedSizeListArray::FixedSizeListArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

FixedSizeListArray::FixedSizeListArray(const std::shared_ptr<DataType>& type,
                                       int64_t length,
                                       const std::shared_ptr<Array>& values,
                                       const std::shared_ptr<Buffer>& null_bitmap,
                                       int64_t null_count, int64_t offset) {
  auto internal_data = ArrayData::Make(type, length, {null_bitmap}, null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  SetData(internal_data);
}

void FixedSizeListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::FIXED_SIZE_LIST);
  this->Array::SetData(data);

  ARROW_CHECK_EQ(list_type()->value_type()->id(), data->child_data[0]->type->id());
  DCHECK(list_type()->value_type()->Equals(data->child_data[0]->type));
  list_size_ = list_type()->list_size();

  ARROW_CHECK_EQ(data_->child_data.size(), 1);
  values_ = MakeArray(data_->child_data[0]);
}

const FixedSizeListType* FixedSizeListArray::list_type() const {
  return checked_cast<const FixedSizeListType*>(data_->type.get());
}

const std::shared_ptr<DataType>& FixedSizeListArray::value_type() const {
  return list_type()->value_type();
}

const std::shared_ptr<Array>& FixedSizeListArray::values() const { return values_; }

Result<std::shared_ptr<Array>> FixedSizeListArray::FromArrays(
    const std::shared_ptr<Array>& values, int32_t list_size,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  if (list_size <= 0) {
    return Status::Invalid("list_size needs to be a strict positive integer");
  }

  if ((values->length() % list_size) != 0) {
    return Status::Invalid(
        "The length of the values Array needs to be a multiple of the list_size");
  }
  int64_t length = values->length() / list_size;
  auto list_type = std::make_shared<FixedSizeListType>(values->type(), list_size);

  return std::make_shared<FixedSizeListArray>(list_type, length, values, null_bitmap,
                                              null_count);
}

Result<std::shared_ptr<Array>> FixedSizeListArray::FromArrays(
    const std::shared_ptr<Array>& values, std::shared_ptr<DataType> type,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count) {
  if (type->id() != Type::FIXED_SIZE_LIST) {
    return Status::TypeError("Expected fixed size list type, got ", type->ToString());
  }
  const auto& list_type = checked_cast<const FixedSizeListType&>(*type);

  if (!list_type.value_type()->Equals(values->type())) {
    return Status::TypeError("Mismatching list value type");
  }
  if ((values->length() % list_type.list_size()) != 0) {
    return Status::Invalid(
        "The length of the values Array needs to be a multiple of the list size");
  }
  int64_t length = values->length() / list_type.list_size();

  return std::make_shared<FixedSizeListArray>(type, length, values, null_bitmap,
                                              null_count);
}

Result<std::shared_ptr<Array>> FixedSizeListArray::Flatten(
    MemoryPool* memory_pool) const {
  return FlattenListArray(*this, memory_pool);
}

// ----------------------------------------------------------------------
// Struct

StructArray::StructArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRUCT);
  SetData(data);
  boxed_fields_.resize(data->child_data.size());
}

StructArray::StructArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::vector<std::shared_ptr<Array>>& children,
                         std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                         int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::STRUCT);
  SetData(ArrayData::Make(type, length, {null_bitmap}, null_count, offset));
  for (const auto& child : children) {
    data_->child_data.push_back(child->data());
  }
  boxed_fields_.resize(children.size());
}

Result<std::shared_ptr<StructArray>> StructArray::Make(
    const std::vector<std::shared_ptr<Array>>& children,
    const std::vector<std::shared_ptr<Field>>& fields,
    std::shared_ptr<Buffer> null_bitmap, int64_t null_count, int64_t offset) {
  if (children.size() != fields.size()) {
    return Status::Invalid("Mismatching number of fields and child arrays");
  }
  if (children.empty()) {
    return Status::Invalid("Can't infer struct array length with 0 child arrays");
  }
  const int64_t length = children.front()->length();
  for (const auto& child : children) {
    if (length != child->length()) {
      return Status::Invalid("Mismatching child array lengths");
    }
  }
  if (offset > length) {
    return Status::IndexError("Offset greater than length of child arrays");
  }
  if (null_bitmap == nullptr) {
    if (null_count > 0) {
      return Status::Invalid("null_count = ", null_count, " but no null bitmap given");
    }
    null_count = 0;
  }
  return std::make_shared<StructArray>(struct_(fields), length - offset, children,
                                       null_bitmap, null_count, offset);
}

Result<std::shared_ptr<StructArray>> StructArray::Make(
    const std::vector<std::shared_ptr<Array>>& children,
    const std::vector<std::string>& field_names, std::shared_ptr<Buffer> null_bitmap,
    int64_t null_count, int64_t offset) {
  if (children.size() != field_names.size()) {
    return Status::Invalid("Mismatching number of field names and child arrays");
  }
  std::vector<std::shared_ptr<Field>> fields(children.size());
  for (size_t i = 0; i < children.size(); ++i) {
    fields[i] = ::arrow::field(field_names[i], children[i]->type());
  }
  return Make(children, fields, std::move(null_bitmap), null_count, offset);
}

const StructType* StructArray::struct_type() const {
  return checked_cast<const StructType*>(data_->type.get());
}

const ArrayVector& StructArray::fields() const {
  for (int i = 0; i < num_fields(); ++i) {
    (void)field(i);
  }
  return boxed_fields_;
}

const std::shared_ptr<Array>& StructArray::field(int i) const {
  std::shared_ptr<Array> result = std::atomic_load(&boxed_fields_[i]);
  if (!result) {
    std::shared_ptr<ArrayData> field_data;
    if (data_->offset != 0 || data_->child_data[i]->length != data_->length) {
      field_data = data_->child_data[i]->Slice(data_->offset, data_->length);
    } else {
      field_data = data_->child_data[i];
    }
    std::shared_ptr<Array> result = MakeArray(field_data);
    std::atomic_store(&boxed_fields_[i], result);
    return boxed_fields_[i];
  }
  return boxed_fields_[i];
}

std::shared_ptr<Array> StructArray::GetFieldByName(const std::string& name) const {
  int i = struct_type()->GetFieldIndex(name);
  return i == -1 ? nullptr : field(i);
}

Status StructArray::CanReferenceFieldByName(const std::string& name) const {
  if (GetFieldByName(name) == nullptr) {
    return Status::Invalid("Field named '", name,
                           "' not found or not unique in the struct.");
  }
  return Status::OK();
}

Status StructArray::CanReferenceFieldsByNames(
    const std::vector<std::string>& names) const {
  for (const auto& name : names) {
    ARROW_RETURN_NOT_OK(CanReferenceFieldByName(name));
  }
  return Status::OK();
}

Result<ArrayVector> StructArray::Flatten(MemoryPool* pool) const {
  ArrayVector flattened;
  flattened.resize(data_->child_data.size());
  std::shared_ptr<Buffer> null_bitmap = data_->buffers[0];

  for (int i = 0; static_cast<size_t>(i) < data_->child_data.size(); i++) {
    ARROW_ASSIGN_OR_RAISE(flattened[i], GetFlattenedField(i, pool));
  }

  return flattened;
}

Result<std::shared_ptr<Array>> StructArray::GetFlattenedField(int index,
                                                              MemoryPool* pool) const {
  std::shared_ptr<Buffer> null_bitmap = data_->buffers[0];

  auto child_data = data_->child_data[index]->Copy();

  std::shared_ptr<Buffer> flattened_null_bitmap;
  int64_t flattened_null_count = kUnknownNullCount;

  // Need to adjust for parent offset
  if (data_->offset != 0 || data_->length != child_data->length) {
    child_data = child_data->Slice(data_->offset, data_->length);
  }
  std::shared_ptr<Buffer> child_null_bitmap = child_data->buffers[0];
  const int64_t child_offset = child_data->offset;

  // The validity of a flattened datum is the logical AND of the struct
  // element's validity and the individual field element's validity.
  if (null_bitmap && child_null_bitmap) {
    ARROW_ASSIGN_OR_RAISE(
        flattened_null_bitmap,
        BitmapAnd(pool, child_null_bitmap->data(), child_offset, null_bitmap_data_,
                  data_->offset, data_->length, child_offset));
  } else if (child_null_bitmap) {
    flattened_null_bitmap = child_null_bitmap;
    flattened_null_count = child_data->null_count;
  } else if (null_bitmap) {
    if (child_offset == data_->offset) {
      flattened_null_bitmap = null_bitmap;
    } else {
      // If the child has an offset, need to synthesize a validity
      // buffer with an offset too
      ARROW_ASSIGN_OR_RAISE(flattened_null_bitmap,
                            AllocateEmptyBitmap(child_offset + data_->length, pool));
      CopyBitmap(null_bitmap_data_, data_->offset, data_->length,
                 flattened_null_bitmap->mutable_data(), child_offset);
    }
    flattened_null_count = data_->null_count;
  } else {
    flattened_null_count = 0;
  }

  auto flattened_data = child_data->Copy();
  flattened_data->buffers[0] = flattened_null_bitmap;
  flattened_data->null_count = flattened_null_count;

  return MakeArray(flattened_data);
}

// ----------------------------------------------------------------------
// UnionArray

void UnionArray::SetData(std::shared_ptr<ArrayData> data) {
  this->Array::SetData(std::move(data));

  union_type_ = checked_cast<const UnionType*>(data_->type.get());

  ARROW_CHECK_GE(data_->buffers.size(), 2);
  raw_type_codes_ = data->GetValuesSafe<int8_t>(1, /*offset=*/0);
  boxed_fields_.resize(data_->child_data.size());
}

void SparseUnionArray::SetData(std::shared_ptr<ArrayData> data) {
  this->UnionArray::SetData(std::move(data));
  ARROW_CHECK_EQ(data_->type->id(), Type::SPARSE_UNION);
  ARROW_CHECK_EQ(data_->buffers.size(), 2);

  // No validity bitmap
  ARROW_CHECK_EQ(data_->buffers[0], nullptr);
}

void DenseUnionArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->UnionArray::SetData(std::move(data));

  ARROW_CHECK_EQ(data_->type->id(), Type::DENSE_UNION);
  ARROW_CHECK_EQ(data_->buffers.size(), 3);

  // No validity bitmap
  ARROW_CHECK_EQ(data_->buffers[0], nullptr);

  raw_value_offsets_ = data->GetValuesSafe<int32_t>(2, /*offset=*/0);
}

SparseUnionArray::SparseUnionArray(std::shared_ptr<ArrayData> data) {
  SetData(std::move(data));
}

SparseUnionArray::SparseUnionArray(std::shared_ptr<DataType> type, int64_t length,
                                   ArrayVector children,
                                   std::shared_ptr<Buffer> type_codes, int64_t offset) {
  auto internal_data = ArrayData::Make(std::move(type), length,
                                       BufferVector{nullptr, std::move(type_codes)},
                                       /*null_count=*/0, offset);
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  SetData(std::move(internal_data));
}

Result<std::shared_ptr<Array>> SparseUnionArray::GetFlattenedField(
    int index, MemoryPool* pool) const {
  if (index < 0 || index >= num_fields()) {
    return Status::Invalid("Index out of range: ", index);
  }
  auto child_data = data_->child_data[index]->Copy();
  // Adjust the result offset/length to be absolute.
  if (data_->offset != 0 || data_->length != child_data->length) {
    child_data = child_data->Slice(data_->offset, data_->length);
  }
  std::shared_ptr<Buffer> child_null_bitmap = child_data->buffers[0];
  const int64_t child_offset = child_data->offset;

  // Synthesize a null bitmap based on the union discriminant.
  // Make sure the bitmap has extra bits corresponding to the child offset.
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> flattened_null_bitmap,
                        AllocateEmptyBitmap(child_data->length + child_offset, pool));
  const int8_t type_code = union_type()->type_codes()[index];
  const int8_t* type_codes = raw_type_codes();
  int64_t offset = 0;
  internal::GenerateBitsUnrolled(flattened_null_bitmap->mutable_data(), child_offset,
                                 data_->length,
                                 [&] { return type_codes[offset++] == type_code; });

  // The validity of a flattened datum is the logical AND of the synthesized
  // null bitmap buffer and the individual field element's validity.
  if (child_null_bitmap) {
    BitmapAnd(flattened_null_bitmap->data(), child_offset, child_null_bitmap->data(),
              child_offset, child_data->length, child_offset,
              flattened_null_bitmap->mutable_data());
  }

  child_data->buffers[0] = std::move(flattened_null_bitmap);
  child_data->null_count = kUnknownNullCount;
  return MakeArray(child_data);
}

DenseUnionArray::DenseUnionArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

DenseUnionArray::DenseUnionArray(std::shared_ptr<DataType> type, int64_t length,
                                 ArrayVector children, std::shared_ptr<Buffer> type_ids,
                                 std::shared_ptr<Buffer> value_offsets, int64_t offset) {
  auto internal_data = ArrayData::Make(
      std::move(type), length,
      BufferVector{nullptr, std::move(type_ids), std::move(value_offsets)},
      /*null_count=*/0, offset);
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  SetData(internal_data);
}

Result<std::shared_ptr<Array>> DenseUnionArray::Make(
    const Array& type_ids, const Array& value_offsets, ArrayVector children,
    std::vector<std::string> field_names, std::vector<type_code_t> type_codes) {
  if (value_offsets.type_id() != Type::INT32) {
    return Status::TypeError("UnionArray offsets must be signed int32");
  }

  if (type_ids.type_id() != Type::INT8) {
    return Status::TypeError("UnionArray type_ids must be signed int8");
  }

  if (type_ids.null_count() != 0) {
    return Status::Invalid("Union type ids may not have nulls");
  }

  if (value_offsets.null_count() != 0) {
    return Status::Invalid("Make does not allow nulls in value_offsets");
  }

  if (field_names.size() > 0 && field_names.size() != children.size()) {
    return Status::Invalid("field_names must have the same length as children");
  }

  if (type_codes.size() > 0 && type_codes.size() != children.size()) {
    return Status::Invalid("type_codes must have the same length as children");
  }

  BufferVector buffers = {nullptr, checked_cast<const Int8Array&>(type_ids).values(),
                          checked_cast<const Int32Array&>(value_offsets).values()};

  auto union_type = dense_union(children, std::move(field_names), std::move(type_codes));
  auto internal_data =
      ArrayData::Make(std::move(union_type), type_ids.length(), std::move(buffers),
                      /*null_count=*/0, type_ids.offset());
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  return std::make_shared<DenseUnionArray>(std::move(internal_data));
}

Result<std::shared_ptr<Array>> SparseUnionArray::Make(
    const Array& type_ids, ArrayVector children, std::vector<std::string> field_names,
    std::vector<int8_t> type_codes) {
  if (type_ids.type_id() != Type::INT8) {
    return Status::TypeError("UnionArray type_ids must be signed int8");
  }

  if (type_ids.null_count() != 0) {
    return Status::Invalid("Union type ids may not have nulls");
  }

  if (field_names.size() > 0 && field_names.size() != children.size()) {
    return Status::Invalid("field_names must have the same length as children");
  }

  if (type_codes.size() > 0 && type_codes.size() != children.size()) {
    return Status::Invalid("type_codes must have the same length as children");
  }

  BufferVector buffers = {nullptr, checked_cast<const Int8Array&>(type_ids).values()};
  auto union_type = sparse_union(children, std::move(field_names), std::move(type_codes));
  auto internal_data =
      ArrayData::Make(std::move(union_type), type_ids.length(), std::move(buffers),
                      /*null_count=*/0, type_ids.offset());
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
    if (child->length() != type_ids.length()) {
      return Status::Invalid(
          "Sparse UnionArray must have len(child) == len(type_ids) for all children");
    }
  }
  return std::make_shared<SparseUnionArray>(std::move(internal_data));
}

std::shared_ptr<Array> UnionArray::field(int i) const {
  if (i < 0 ||
      static_cast<decltype(boxed_fields_)::size_type>(i) >= boxed_fields_.size()) {
    return nullptr;
  }
  std::shared_ptr<Array> result = std::atomic_load(&boxed_fields_[i]);
  if (!result) {
    std::shared_ptr<ArrayData> child_data = data_->child_data[i]->Copy();
    if (mode() == UnionMode::SPARSE) {
      // Sparse union: need to adjust child if union is sliced
      // (for dense unions, the need to lookup through the offsets
      //  makes this unnecessary)
      if (data_->offset != 0 || child_data->length > data_->length) {
        child_data = child_data->Slice(data_->offset, data_->length);
      }
    }
    result = MakeArray(child_data);
    std::atomic_store(&boxed_fields_[i], result);
  }
  return result;
}

}  // namespace arrow
