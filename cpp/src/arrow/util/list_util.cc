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
#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace list_util {

namespace internal {

namespace {

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

}  // namespace internal

}  // namespace list_util
}  // namespace arrow
