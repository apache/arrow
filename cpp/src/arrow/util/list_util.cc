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

}  // namespace internal

}  // namespace arrow::list_util
