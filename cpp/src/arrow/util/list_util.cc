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
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/extension_type.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/string.h"

namespace arrow::list_util {

namespace internal {

namespace {

using arrow::internal::checked_cast;
using arrow::internal::CopyBitmap;
using arrow::internal::ReverseSetBitRunReader;
using arrow::internal::SetBitRunReader;

/// \pre input.length() > 0 && input.null_count() != input.length()
/// \param input A LIST_VIEW or LARGE_LIST_VIEW array
template <typename offset_type>
std::optional<int64_t> MinViewOffset(const ArraySpan& input) {
  const uint8_t* validity = input.buffers[0].data;
  const auto* offsets = input.GetValues<offset_type>(1);
  const auto* sizes = input.GetValues<offset_type>(2);

  // Make an access to the sizes buffer only when strictly necessary.
#define MINIMIZE_MIN_VIEW_OFFSET(i)             \
  auto offset = offsets[i];                     \
  if (min_offset.has_value()) {                 \
    if (offset < *min_offset && sizes[i] > 0) { \
      if (offset == 0) {                        \
        return 0;                               \
      }                                         \
      min_offset = offset;                      \
    }                                           \
  } else {                                      \
    if (sizes[i] > 0) {                         \
      if (offset == 0) {                        \
        return 0;                               \
      }                                         \
      min_offset = offset;                      \
    }                                           \
  }

  std::optional<offset_type> min_offset;
  if (validity == nullptr) {
    for (int64_t i = 0; i < input.length; i++) {
      MINIMIZE_MIN_VIEW_OFFSET(i);
    }
  } else {
    SetBitRunReader reader(validity, input.offset, input.length);
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) {
        break;
      }
      for (int64_t i = run.position; i < run.position + run.length; ++i) {
        MINIMIZE_MIN_VIEW_OFFSET(i);
      }
    }
  }
  return min_offset;

#undef MINIMIZE_MIN_VIEW_OFFSET
}

/// \pre input.length() > 0 && input.null_count() != input.length()
/// \param input A LIST_VIEW or LARGE_LIST_VIEW array
template <typename offset_type>
int64_t MaxViewEnd(const ArraySpan& input) {
  const auto values_length = input.child_data[0].length;

  const uint8_t* validity = input.buffers[0].data;
  const auto* offsets = input.GetValues<offset_type>(1);
  const auto* sizes = input.GetValues<offset_type>(2);

#define MAXIMIZE_MAX_VIEW_END(i)                        \
  const auto offset = static_cast<int64_t>(offsets[i]); \
  const offset_type size = sizes[i];                    \
  if (size > 0) {                                       \
    const int64_t end = offset + size;                  \
    if (end > max_end) {                                \
      if (end == values_length) {                       \
        return values_length;                           \
      }                                                 \
      max_end = end;                                    \
    }                                                   \
  }

  int64_t max_end = 0;
  if (validity == nullptr) {
    for (int64_t i = input.length - 1; i >= 0; --i) {
      MAXIMIZE_MAX_VIEW_END(i);
    }
  } else {
    ReverseSetBitRunReader reader(validity, input.offset, input.length);
    while (true) {
      const auto run = reader.NextRun();
      if (run.length == 0) {
        break;
      }
      for (int64_t i = run.position + run.length - 1; i >= run.position; --i) {
        MAXIMIZE_MAX_VIEW_END(i);
      }
    }
  }
  return max_end;

#undef MAXIMIZE_MAX_VIEW_END
}

template <typename offset_type>
std::pair<int64_t, int64_t> RangeOfValuesUsedByListView(const ArraySpan& input) {
  DCHECK(is_list_view(*input.type));
  if (input.length == 0 || input.null_count == input.length) {
    return {0, 0};
  }
  const auto min_offset = MinViewOffset<offset_type>(input);
  // If all list-views are empty, min_offset will be std::nullopt.
  if (!min_offset.has_value()) {
    return {0, 0};
  }
  const int64_t max_end = MaxViewEnd<offset_type>(input);
  return {*min_offset, max_end - *min_offset};
}

template <typename offset_type>
std::pair<int64_t, int64_t> RangeOfValuesUsedByList(const ArraySpan& input) {
  DCHECK(is_var_length_list(*input.type));
  if (input.length == 0) {
    return {0, 0};
  }
  const auto* offsets = input.buffers[1].data_as<offset_type>();
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

std::shared_ptr<DataType> StorageType(const std::shared_ptr<DataType>& type);

std::shared_ptr<Field> StorageField(const std::shared_ptr<Field>& field) {
  return field->WithType(StorageType(field->type()));
}

FieldVector StorageFields(const FieldVector& fields) {
  FieldVector storage_fields;
  storage_fields.reserve(fields.size());
  for (const auto& field : fields) {
    storage_fields.push_back(StorageField(field));
  }
  return storage_fields;
}

/// \brief Replace every extension type by its storage type, recursively
///
/// `MakeBuilder` does not support extension types, so the values gathered from
/// a list-view are built using their storage types. The original types are then
/// restored by `RestoreType`.
std::shared_ptr<DataType> StorageType(const std::shared_ptr<DataType>& type) {
  switch (type->id()) {
    case Type::EXTENSION:
      return StorageType(checked_cast<const ExtensionType&>(*type).storage_type());
    case Type::LIST:
      return std::make_shared<ListType>(
          StorageField(checked_cast<const ListType&>(*type).value_field()));
    case Type::LARGE_LIST:
      return std::make_shared<LargeListType>(
          StorageField(checked_cast<const LargeListType&>(*type).value_field()));
    case Type::LIST_VIEW:
      return std::make_shared<ListViewType>(
          StorageField(checked_cast<const ListViewType&>(*type).value_field()));
    case Type::LARGE_LIST_VIEW:
      return std::make_shared<LargeListViewType>(
          StorageField(checked_cast<const LargeListViewType&>(*type).value_field()));
    case Type::MAP: {
      const auto& map_type = checked_cast<const MapType&>(*type);
      return std::make_shared<MapType>(StorageField(map_type.key_field()),
                                       StorageField(map_type.item_field()),
                                       map_type.keys_sorted());
    }
    case Type::FIXED_SIZE_LIST: {
      const auto& list_type = checked_cast<const FixedSizeListType&>(*type);
      return std::make_shared<FixedSizeListType>(StorageField(list_type.value_field()),
                                                 list_type.list_size());
    }
    case Type::STRUCT:
      return std::make_shared<StructType>(StorageFields(type->fields()));
    case Type::SPARSE_UNION: {
      const auto& union_type = checked_cast<const SparseUnionType&>(*type);
      return std::make_shared<SparseUnionType>(StorageFields(union_type.fields()),
                                               union_type.type_codes());
    }
    case Type::DENSE_UNION: {
      const auto& union_type = checked_cast<const DenseUnionType&>(*type);
      return std::make_shared<DenseUnionType>(StorageFields(union_type.fields()),
                                              union_type.type_codes());
    }
    case Type::RUN_END_ENCODED: {
      const auto& ree_type = checked_cast<const RunEndEncodedType&>(*type);
      return std::make_shared<RunEndEncodedType>(ree_type.run_end_type(),
                                                 StorageType(ree_type.value_type()));
    }
    case Type::DICTIONARY: {
      const auto& dict_type = checked_cast<const DictionaryType&>(*type);
      return std::make_shared<DictionaryType>(dict_type.index_type(),
                                              StorageType(dict_type.value_type()),
                                              dict_type.ordered());
    }
    default:
      return type;
  }
}

/// \brief Restore the original (possibly nested) types on gathered array data
void RestoreType(const std::shared_ptr<DataType>& type, ArrayData* data,
                 bool update_type = true) {
  if (update_type) {
    data->type = type;
  }
  switch (type->id()) {
    case Type::EXTENSION:
      RestoreType(checked_cast<const ExtensionType&>(*type).storage_type(), data,
                  /*update_type=*/false);
      break;
    case Type::LIST:
    case Type::LARGE_LIST:
    case Type::LIST_VIEW:
    case Type::LARGE_LIST_VIEW:
    case Type::MAP:
    case Type::FIXED_SIZE_LIST:
      RestoreType(type->field(0)->type(), data->child_data[0].get());
      break;
    case Type::STRUCT:
    case Type::SPARSE_UNION:
    case Type::DENSE_UNION:
    case Type::RUN_END_ENCODED:
      for (int i = 0; i < type->num_fields(); ++i) {
        RestoreType(type->field(i)->type(), data->child_data[i].get());
      }
      break;
    case Type::DICTIONARY:
      if (data->dictionary) {
        RestoreType(checked_cast<const DictionaryType&>(*type).value_type(),
                    data->dictionary.get());
      }
      break;
    default:
      break;
  }
}

/// \brief Rewrite the type of a span and of all its children to a storage type
///
/// The gathered values are appended to storage-typed builders (see
/// `StorageType`), so the appended slices have to be seen through those same
/// storage types: a builder may check or inspect the type of the array it is
/// given (e.g. `RunEndEncodedBuilder` or `DictionaryBuilder`).
///
/// \pre `span` has the layout of `storage_type`, i.e. the two only differ by
///      extension types
void ApplyStorageType(ArraySpan* span, const std::shared_ptr<DataType>& storage_type) {
  span->type = storage_type.get();
  if (storage_type->id() == Type::DICTIONARY) {
    // An ArraySpan holds the dictionary of a dictionary array in its first child
    const auto& dict_type = checked_cast<const DictionaryType&>(*storage_type);
    ApplyStorageType(&span->child_data[0], dict_type.value_type());
    return;
  }
  for (int i = 0; i < static_cast<int>(span->child_data.size()); ++i) {
    ApplyStorageType(&span->child_data[i], storage_type->field(i)->type());
  }
}

/// \brief The range of values a list-view references, when its views are contiguous
///
/// A list-view describes a list without moving any value when its valid views
/// are in order and contiguous, i.e. when every valid view starts where the
/// previous one ended. Such a view is a list in disguise: the values it
/// references are a slice of its child array, and the values that only null
/// views reference (before the first and after the last valid view) are not
/// part of that slice.
///
/// \return The range of the child array to keep, or `std::nullopt` when the
///         views are out of order, overlapping or have gaps in between
template <typename SrcListViewType>
std::optional<std::pair<int64_t, int64_t>> ContiguousValuesRange(
    const ArraySpan& list_view) {
  using offset_type = typename SrcListViewType::offset_type;
  const uint8_t* validity = list_view.buffers[0].data;
  const auto* offsets = list_view.GetValues<offset_type>(1);
  const auto* sizes = list_view.GetValues<offset_type>(2);
  const int64_t values_length = list_view.child_data[0].length;

  int64_t range_start = 0;
  int64_t range_end = 0;
  bool has_valid_view = false;
  for (int64_t i = 0; i < list_view.length; ++i) {
    // A null view contributes no values, whatever size it declares.
    if (validity != nullptr && !bit_util::GetBit(validity, list_view.offset + i)) {
      continue;
    }
    const int64_t offset = offsets[i];
    const int64_t size = sizes[i];
    if (offset < 0 || size < 0 || offset > values_length ||
        size > values_length - offset) {
      return std::nullopt;
    }
    if (!has_valid_view) {
      range_start = offset;
      has_valid_view = true;
    } else if (offset != range_end) {
      // Values are missing, shared or referenced out of order: they have to be
      // gathered instead of sliced.
      return std::nullopt;
    }
    range_end = offset + size;
  }
  if (!has_valid_view) {
    // Every view is null: no value is referenced at all.
    return std::make_pair<int64_t, int64_t>(0, 0);
  }
  return std::make_pair(range_start, range_end - range_start);
}

/// \brief Convert a list-view whose views are contiguous, keeping its values
///
/// \param list_view A LIST_VIEW or LARGE_LIST_VIEW array whose views are
/// contiguous, as checked by `ContiguousValuesRange`
/// \param values_range The range of the values to keep, as returned by
/// `ContiguousValuesRange`
template <typename DestListType, typename SrcListViewType>
Result<std::shared_ptr<ArrayData>> ContiguousViewToList(
    const ArraySpan& list_view, const std::pair<int64_t, int64_t>& values_range,
    MemoryPool* pool) {
  using src_offset_type = typename SrcListViewType::offset_type;
  using dest_offset_type = typename DestListType::offset_type;

  const auto& list_view_type = checked_cast<const SrcListViewType&>(*list_view.type);
  const auto list_type = std::make_shared<DestListType>(list_view_type.value_type());
  const int64_t range_length = values_range.second;
  if (range_length > std::numeric_limits<dest_offset_type>::max()) {
    return Status::Invalid("Array of type ", list_view.type->ToString(),
                           " too large to convert to ", list_type->ToString());
  }

  // The views' offsets cannot be reused as list offsets: they are relative to
  // the child array, not to the range of values that is kept, and the views'
  // declared sizes are dropped for the null ones.
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<Buffer> offsets_buffer,
      AllocateBuffer(sizeof(dest_offset_type) * (list_view.length + 1), pool));
  auto* offsets = offsets_buffer->mutable_data_as<dest_offset_type>();
  const uint8_t* validity = list_view.buffers[0].data;
  const auto* sizes = list_view.GetValues<src_offset_type>(2);
  dest_offset_type offset = 0;
  for (int64_t i = 0; i < list_view.length; ++i) {
    offsets[i] = offset;
    const bool is_valid = !validity || bit_util::GetBit(validity, list_view.offset + i);
    offset += static_cast<dest_offset_type>(is_valid ? sizes[i] : 0);
  }
  offsets[list_view.length] = offset;
  DCHECK_EQ(offset, range_length);

  // The bitmap is bit-packed, so it can only be shared when the views start at
  // the first bit; otherwise the range has to be copied out of the way.
  std::shared_ptr<Buffer> null_bitmap = list_view.GetBuffer(0);
  if (validity != nullptr && list_view.offset != 0) {
    ARROW_ASSIGN_OR_RAISE(null_bitmap,
                          CopyBitmap(pool, validity, list_view.offset, list_view.length));
  }

  // The values are kept as they are, only narrowed down to the values that the
  // valid views reference (GH-51612).
  std::shared_ptr<ArrayData> values = list_view.child_data[0].ToArrayData();
  if (values_range.first != 0 || range_length != values->length) {
    values = values->Slice(values_range.first, range_length);
  }

  return ArrayData::Make(list_type, list_view.length,
                         {std::move(null_bitmap), std::move(offsets_buffer)},
                         {std::move(values)}, list_view.GetNullCount());
}

/// \param list_view A LIST_VIEW or LARGE_LIST_VIEW array
template <typename DestListType, typename SrcListViewType>
Result<std::shared_ptr<ArrayData>> ListFromListViewImpl(const ArraySpan& list_view,
                                                        MemoryPool* pool) {
  using src_offset_type = typename SrcListViewType::offset_type;
  using ListBuilderType = typename TypeTraits<DestListType>::BuilderType;

  const auto& list_view_type = checked_cast<const SrcListViewType&>(*list_view.type);
  const auto& value_type = list_view_type.value_type();

  // The views may already describe a list, in which case its values are kept
  // instead of being gathered (GH-51612).
  if (auto values_range = ContiguousValuesRange<SrcListViewType>(list_view)) {
    return ContiguousViewToList<DestListType, SrcListViewType>(list_view, *values_range,
                                                               pool);
  }
  // MakeBuilder does not support extension types, so the values are gathered
  // through their storage types.
  const auto storage_value_type = StorageType(value_type);
  // The value type is carried over, so that callers may cast the child.
  auto list_type = std::make_shared<DestListType>(value_type);

  ARROW_ASSIGN_OR_RAISE(auto sum_of_sizes, SumOfLogicalListSizes(list_view));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayBuilder> value_builder,
                        MakeBuilder(storage_value_type, pool));
  RETURN_NOT_OK(value_builder->Reserve(sum_of_sizes));
  auto list_builder = std::make_shared<ListBuilderType>(pool, value_builder, list_type);
  RETURN_NOT_OK(list_builder->Reserve(list_view.length));

  // The bitmap is bit-packed, so it must be read with the element offset
  // applied as a bit offset, not as the byte offset GetValues() would apply.
  const uint8_t* validity = list_view.buffers[0].data;
  const auto* offsets = list_view.GetValues<src_offset_type>(1);
  const auto* sizes = list_view.GetValues<src_offset_type>(2);
  // The value builder is storage-typed, so the appended slices must be seen
  // through the storage types as well (GH-51612).
  ArraySpan values{list_view.child_data[0]};
  ApplyStorageType(&values, storage_value_type);
  for (int64_t i = 0; i < list_view.length; ++i) {
    const bool is_valid = !validity || bit_util::GetBit(validity, list_view.offset + i);
    // A null view contributes no values, whatever size it declares.
    const int64_t size = is_valid ? sizes[i] : 0;
    RETURN_NOT_OK(list_builder->Append(is_valid, size));
    RETURN_NOT_OK(value_builder->AppendArraySlice(values, offsets[i], size));
  }

  std::shared_ptr<ArrayData> list_data;
  RETURN_NOT_OK(list_builder->FinishInternal(&list_data));
  // The builders produced storage-typed data; restore the original types,
  // including extension types nested anywhere in the value type.
  list_data->type = list_type;
  RestoreType(value_type, list_data->child_data[0].get());
  return list_data;
}

}  // namespace

Result<std::shared_ptr<ArrayData>> ListFromListView(const ArraySpan& input,
                                                    Type::type dest_type_id,
                                                    MemoryPool* pool) {
  switch (input.type->id()) {
    case Type::LIST_VIEW:
      switch (dest_type_id) {
        case Type::LIST:
          return ListFromListViewImpl<ListType, ListViewType>(input, pool);
        case Type::LARGE_LIST:
          return ListFromListViewImpl<LargeListType, ListViewType>(input, pool);
        default:
          break;
      }
      break;
    case Type::LARGE_LIST_VIEW:
      switch (dest_type_id) {
        case Type::LIST:
          return ListFromListViewImpl<ListType, LargeListViewType>(input, pool);
        case Type::LARGE_LIST:
          return ListFromListViewImpl<LargeListType, LargeListViewType>(input, pool);
        default:
          break;
      }
      break;
    default:
      break;
  }
  return Status::TypeError("ListFromListView: expected a list-view array and a ",
                           arrow::internal::ToString(dest_type_id),
                           " destination type, got ", input.type->ToString());
}

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

}  // namespace internal

}  // namespace arrow::list_util
