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

#include <cstdint>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow::list_util {

namespace internal {

namespace {

using arrow::internal::checked_cast;

/// \pre input.length() > 0 && input.null_count() != input.length()
/// \param input A LIST_VIEW or LARGE_LIST_VIEW array
template <typename offset_type>
int64_t MinViewOffset(const ArraySpan& input) {
  const uint8_t* validity = input.MayHaveNulls() ? input.buffers[0].data : nullptr;
  const auto* offsets = reinterpret_cast<const offset_type*>(input.buffers[1].data);
  const auto* sizes = reinterpret_cast<const offset_type*>(input.buffers[2].data);

  // It's very likely that the first non-null non-empty list-view starts at
  // offset 0 of the child array.
  int64_t i = 0;
  while (i < input.length && (input.IsNull(i) || sizes[input.offset + i] == 0)) {
    i += 1;
  }
  if (i >= input.length) {
    return 0;
  }
  auto min_offset = offsets[input.offset + i];
  if (ARROW_PREDICT_TRUE(min_offset == 0)) {
    // Early exit: offset 0 found already.
    return 0;
  }

  // Slow path: scan the buffers entirely.
  arrow::internal::VisitSetBitRunsVoid(
      validity, /*offset=*/input.offset + i + 1, /*length=*/input.length - i - 1,
      [&](int64_t i, int64_t run_length) {
        for (int64_t j = 0; j < run_length; j++) {
          const auto offset = offsets[input.offset + i + j];
          if (ARROW_PREDICT_FALSE(offset < min_offset)) {
            if (sizes[input.offset + i + j] > 0) {
              min_offset = offset;
            }
          }
        }
      });
  return min_offset;
}

/// \pre input.length() > 0 && input.null_count() != input.length()
/// \param input A LIST_VIEW or LARGE_LIST_VIEW array
template <typename offset_type>
int64_t MaxViewEnd(const ArraySpan& input) {
  const uint8_t* validity = input.MayHaveNulls() ? input.buffers[0].data : NULLPTR;
  const auto* offsets = reinterpret_cast<const offset_type*>(input.buffers[1].data);
  const auto* sizes = reinterpret_cast<const offset_type*>(input.buffers[2].data);
  const auto IsNull = [validity](int64_t i) -> bool {
    return validity && !arrow::bit_util::GetBit(validity, i);
  };

  int64_t i = input.length - 1;  // safe because input.length() > 0
  while (i != 0 && (IsNull(i) || sizes[input.offset + i] == 0)) {
    i -= 1;
  }
  const auto offset = static_cast<int64_t>(offsets[input.offset + i]);
  const auto size = sizes[input.offset + i];
  if (i == 0) {
    return (IsNull(i) || sizes[input.offset + i] == 0) ? 0 : offset + size;
  }
  constexpr auto kInt64Max = std::numeric_limits<int64_t>::max();
  if constexpr (sizeof(offset_type) == sizeof(int64_t)) {
    if (ARROW_PREDICT_FALSE(offset > kInt64Max - size)) {
      // Early-exit: 64-bit overflow detected. This is not possible on a
      // valid list-view, but we return the maximum possible value to
      // avoid undefined behavior.
      return kInt64Max;
    }
  }
  int64_t max_end =
      static_cast<int64_t>(offsets[input.offset + i]) + sizes[input.offset + i];
  if (max_end == input.child_data[0].length) {
    // Early-exit: maximum possible view-end found already.
    return max_end;
  }

  // Slow path: scan the buffers entirely.
  arrow::internal::VisitSetBitRunsVoid(
      validity, input.offset, /*length=*/i + 1, [&](int64_t i, int64_t run_length) {
        for (int64_t j = 0; j < run_length; ++j) {
          const auto offset = static_cast<int64_t>(offsets[input.offset + i + j]);
          const auto size = sizes[input.offset + i + j];
          if (size > 0) {
            if constexpr (sizeof(offset_type) == sizeof(int64_t)) {
              if (ARROW_PREDICT_FALSE(offset > kInt64Max - size)) {
                // 64-bit overflow detected. This is not possible on a valid list-view,
                // but we saturate max_end to the maximum possible value to avoid
                // undefined behavior.
                max_end = kInt64Max;
                return;
              }
            }
            max_end = std::max(max_end, offset + size);
          }
        }
      });
  return max_end;
}

template <typename offset_type>
std::pair<int64_t, int64_t> RangeOfValuesUsedByListView(const ArraySpan& input) {
  DCHECK(is_list_view(*input.type));
  if (input.length == 0 || input.GetNullCount() == input.length) {
    return {0, 0};
  }
  const int64_t min_offset = MinViewOffset<offset_type>(input);
  const int64_t max_end = MaxViewEnd<offset_type>(input);
  return {min_offset, max_end - min_offset};
}

template <typename offset_type>
std::pair<int64_t, int64_t> RangeOfValuesUsedByList(const ArraySpan& input) {
  DCHECK(is_var_length_list(*input.type));
  if (input.length == 0) {
    return {0, 0};
  }
  const auto* offsets = reinterpret_cast<const offset_type*>(input.buffers[1].data);
  const int64_t min_offset = offsets[input.offset];
  const int64_t max_end = offsets[input.offset + input.length];
  return {min_offset, max_end - min_offset};
}

template <typename offset_type>
int64_t SumOfListSizes(const ArraySpan& input) {
  DCHECK(is_var_length_list(*input.type));
  const uint8_t* validity = input.buffers[0].data;
  const auto* offsets = input.GetValues<offset_type>(1);
  int64_t sum = 0;
  arrow::internal::VisitSetBitRunsVoid(
      validity, input.offset, input.length,
      [&sum, offsets](int64_t run_start, int64_t run_length) {
        sum += offsets[run_start + run_length + 1] - offsets[run_start];
      });
  return sum;
}

template <typename offset_type>
int64_t SumOfListViewSizes(const ArraySpan& input) {
  DCHECK(is_list_view(*input.type));
  const uint8_t* validity = input.buffers[0].data;
  const auto* sizes = input.GetValues<offset_type>(2);
  int64_t sum = 0;
  arrow::internal::VisitSetBitRunsVoid(
      validity, input.offset, input.length,
      [&sum, sizes](int64_t run_start, int64_t run_length) {
        for (int64_t i = run_start; i < run_start + run_length; ++i) {
          sum += sizes[i];
        }
      });
  return sum;
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
  auto* sizes = reinterpret_cast<offset_type*>(sizes_buffer->mutable_data());
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

  auto sum_of_list_view_sizes = SumOfListViewSizes<offset_type>(*list_view_data);
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

Result<std::pair<int64_t, int64_t>> RangeOfValuesUsed(const ArraySpan& input) {
  switch (input.type->id()) {
    case Type::LIST:
      return RangeOfValuesUsedByList<ListType::offset_type>(input);
    case Type::MAP:
      return RangeOfValuesUsedByList<MapType::offset_type>(input);
    case Type::LARGE_LIST:
      return RangeOfValuesUsedByList<LargeListType::offset_type>(input);
    case Type::LIST_VIEW:
      return RangeOfValuesUsedByListView<ListViewType::offset_type>(input);
    case Type::LARGE_LIST_VIEW:
      return RangeOfValuesUsedByListView<LargeListViewType::offset_type>(input);
    default:
      break;
  }
  DCHECK(!is_var_length_list_like(*input.type));
  return Status::TypeError(
      "RangeOfValuesUsed: input is not a var-length list-like array");
}

Result<int64_t> SumOfLogicalListSizes(const ArraySpan& input) {
  switch (input.type->id()) {
    case Type::LIST:
      return SumOfListSizes<ListType::offset_type>(input);
    case Type::MAP:
      return SumOfListSizes<MapType::offset_type>(input);
    case Type::LARGE_LIST:
      return SumOfListSizes<LargeListType::offset_type>(input);
    case Type::LIST_VIEW:
      return SumOfListViewSizes<ListViewType::offset_type>(input);
    case Type::LARGE_LIST_VIEW:
      return SumOfListViewSizes<LargeListViewType::offset_type>(input);
    default:
      break;
  }
  DCHECK(!is_var_length_list_like(*input.type));
  return Status::TypeError(
      "SumOfLogicalListSizes: input is not a var-length list-like array");
}

Result<std::shared_ptr<ListViewArray>> ListViewFromList(const ListArray& source,
                                                        MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      (internal::ListViewFromListImpl<ListViewType, ListType>(source.data(), pool)));
  return std::make_shared<ListViewArray>(std::move(data));
}

Result<std::shared_ptr<LargeListViewArray>> ListViewFromList(const LargeListArray& source,
                                                             MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto data,
                        (internal::ListViewFromListImpl<LargeListViewType, LargeListType>(
                            source.data(), pool)));
  return std::make_shared<LargeListViewArray>(std::move(data));
}

Result<std::shared_ptr<ListArray>> ListFromListView(const ListViewArray& source,
                                                    MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(
      auto data,
      (internal::ListFromListViewImpl<ListType, ListViewType>(source.data(), pool)));
  return std::make_shared<ListArray>(std::move(data));
}

Result<std::shared_ptr<LargeListArray>> ListFromListView(const LargeListViewArray& source,
                                                         MemoryPool* pool) {
  ARROW_ASSIGN_OR_RAISE(auto data,
                        (internal::ListFromListViewImpl<LargeListType, LargeListViewType>(
                            source.data(), pool)));
  return std::make_shared<LargeListArray>(std::move(data));
}

}  // namespace internal

}  // namespace arrow::list_util
