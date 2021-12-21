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
#include <cmath>
#include <cstdint>
#include <functional>

#include "arrow/array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/chunked_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {
namespace internal {

// Visit all physical types for which sorting is implemented.
#define VISIT_SORTABLE_PHYSICAL_TYPES(VISIT) \
  VISIT(BooleanType)                         \
  VISIT(Int8Type)                            \
  VISIT(Int16Type)                           \
  VISIT(Int32Type)                           \
  VISIT(Int64Type)                           \
  VISIT(UInt8Type)                           \
  VISIT(UInt16Type)                          \
  VISIT(UInt32Type)                          \
  VISIT(UInt64Type)                          \
  VISIT(FloatType)                           \
  VISIT(DoubleType)                          \
  VISIT(BinaryType)                          \
  VISIT(LargeBinaryType)                     \
  VISIT(FixedSizeBinaryType)                 \
  VISIT(Decimal128Type)                      \
  VISIT(Decimal256Type)

// NOTE: std::partition is usually faster than std::stable_partition.

struct NonStablePartitioner {
  template <typename Predicate>
  uint64_t* operator()(uint64_t* indices_begin, uint64_t* indices_end, Predicate&& pred) {
    return std::partition(indices_begin, indices_end, std::forward<Predicate>(pred));
  }
};

struct StablePartitioner {
  template <typename Predicate>
  uint64_t* operator()(uint64_t* indices_begin, uint64_t* indices_end, Predicate&& pred) {
    return std::stable_partition(indices_begin, indices_end,
                                 std::forward<Predicate>(pred));
  }
};

template <typename TypeClass, typename Enable = void>
struct NullTraits {
  using has_null_like_values = std::false_type;
};

template <typename TypeClass>
struct NullTraits<TypeClass, enable_if_physical_floating_point<TypeClass>> {
  using has_null_like_values = std::true_type;
};

template <typename TypeClass>
using has_null_like_values = typename NullTraits<TypeClass>::has_null_like_values;

// Compare two values, taking NaNs into account

template <typename Type, typename Enable = void>
struct ValueComparator;

template <typename Type>
struct ValueComparator<Type, enable_if_t<!has_null_like_values<Type>::value>> {
  template <typename Value>
  static int Compare(const Value& left, const Value& right, SortOrder order,
                     NullPlacement null_placement) {
    int compared;
    if (left == right) {
      compared = 0;
    } else if (left > right) {
      compared = 1;
    } else {
      compared = -1;
    }
    if (order == SortOrder::Descending) {
      compared = -compared;
    }
    return compared;
  }
};

template <typename Type>
struct ValueComparator<Type, enable_if_t<has_null_like_values<Type>::value>> {
  template <typename Value>
  static int Compare(const Value& left, const Value& right, SortOrder order,
                     NullPlacement null_placement) {
    const bool is_nan_left = std::isnan(left);
    const bool is_nan_right = std::isnan(right);
    if (is_nan_left && is_nan_right) {
      return 0;
    } else if (is_nan_left) {
      return null_placement == NullPlacement::AtStart ? -1 : 1;
    } else if (is_nan_right) {
      return null_placement == NullPlacement::AtStart ? 1 : -1;
    }
    int compared;
    if (left == right) {
      compared = 0;
    } else if (left > right) {
      compared = 1;
    } else {
      compared = -1;
    }
    if (order == SortOrder::Descending) {
      compared = -compared;
    }
    return compared;
  }
};

template <typename Type, typename Value>
int CompareTypeValues(const Value& left, const Value& right, SortOrder order,
                      NullPlacement null_placement) {
  return ValueComparator<Type>::Compare(left, right, order, null_placement);
}

struct NullPartitionResult {
  uint64_t* non_nulls_begin;
  uint64_t* non_nulls_end;
  uint64_t* nulls_begin;
  uint64_t* nulls_end;

  uint64_t* overall_begin() const { return std::min(nulls_begin, non_nulls_begin); }

  uint64_t* overall_end() const { return std::max(nulls_end, non_nulls_end); }

  int64_t non_null_count() const { return non_nulls_end - non_nulls_begin; }

  int64_t null_count() const { return nulls_end - nulls_begin; }

  static NullPartitionResult NoNulls(uint64_t* indices_begin, uint64_t* indices_end,
                                     NullPlacement null_placement) {
    if (null_placement == NullPlacement::AtStart) {
      return {indices_begin, indices_end, indices_begin, indices_begin};
    } else {
      return {indices_begin, indices_end, indices_end, indices_end};
    }
  }

  static NullPartitionResult NullsOnly(uint64_t* indices_begin, uint64_t* indices_end,
                                       NullPlacement null_placement) {
    if (null_placement == NullPlacement::AtStart) {
      return {indices_end, indices_end, indices_begin, indices_end};
    } else {
      return {indices_begin, indices_begin, indices_begin, indices_end};
    }
  }

  static NullPartitionResult NullsAtEnd(uint64_t* indices_begin, uint64_t* indices_end,
                                        uint64_t* midpoint) {
    DCHECK_GE(midpoint, indices_begin);
    DCHECK_LE(midpoint, indices_end);
    return {indices_begin, midpoint, midpoint, indices_end};
  }

  static NullPartitionResult NullsAtStart(uint64_t* indices_begin, uint64_t* indices_end,
                                          uint64_t* midpoint) {
    DCHECK_GE(midpoint, indices_begin);
    DCHECK_LE(midpoint, indices_end);
    return {midpoint, indices_end, indices_begin, midpoint};
  }
};

// Move nulls (not null-like values) to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename Partitioner>
NullPartitionResult PartitionNullsOnly(uint64_t* indices_begin, uint64_t* indices_end,
                                       const Array& values, int64_t offset,
                                       NullPlacement null_placement) {
  if (values.null_count() == 0) {
    return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
  }
  Partitioner partitioner;
  if (null_placement == NullPlacement::AtStart) {
    auto nulls_end = partitioner(
        indices_begin, indices_end,
        [&values, &offset](uint64_t ind) { return values.IsNull(ind - offset); });
    return NullPartitionResult::NullsAtStart(indices_begin, indices_end, nulls_end);
  } else {
    auto nulls_begin = partitioner(
        indices_begin, indices_end,
        [&values, &offset](uint64_t ind) { return !values.IsNull(ind - offset); });
    return NullPartitionResult::NullsAtEnd(indices_begin, indices_end, nulls_begin);
  }
}

// Move non-null null-like values to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
enable_if_t<!has_null_like_values<typename ArrayType::TypeClass>::value,
            NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ArrayType& values, int64_t offset,
                   NullPlacement null_placement) {
  return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
}

template <typename ArrayType, typename Partitioner>
enable_if_t<has_null_like_values<typename ArrayType::TypeClass>::value,
            NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ArrayType& values, int64_t offset,
                   NullPlacement null_placement) {
  Partitioner partitioner;
  if (null_placement == NullPlacement::AtStart) {
    auto null_likes_end =
        partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
          return std::isnan(values.GetView(ind - offset));
        });
    return NullPartitionResult::NullsAtStart(indices_begin, indices_end, null_likes_end);
  } else {
    auto null_likes_begin =
        partitioner(indices_begin, indices_end, [&values, &offset](uint64_t ind) {
          return !std::isnan(values.GetView(ind - offset));
        });
    return NullPartitionResult::NullsAtEnd(indices_begin, indices_end, null_likes_begin);
  }
}

// Move nulls to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
NullPartitionResult PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
                                   const ArrayType& values, int64_t offset,
                                   NullPlacement null_placement) {
  // Partition nulls at start (resp. end), and null-like values just before (resp. after)
  NullPartitionResult p = PartitionNullsOnly<Partitioner>(indices_begin, indices_end,
                                                          values, offset, null_placement);
  NullPartitionResult q = PartitionNullLikes<ArrayType, Partitioner>(
      p.non_nulls_begin, p.non_nulls_end, values, offset, null_placement);
  return NullPartitionResult{q.non_nulls_begin, q.non_nulls_end,
                             std::min(q.nulls_begin, p.nulls_begin),
                             std::max(q.nulls_end, p.nulls_end)};
}

//
// Null partitioning on chunked arrays
//

template <typename Partitioner>
NullPartitionResult PartitionNullsOnly(uint64_t* indices_begin, uint64_t* indices_end,
                                       const ChunkedArrayResolver& resolver,
                                       int64_t null_count, NullPlacement null_placement) {
  if (null_count == 0) {
    return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
  }
  Partitioner partitioner;
  if (null_placement == NullPlacement::AtStart) {
    auto nulls_end = partitioner(indices_begin, indices_end, [&](uint64_t ind) {
      const auto chunk = resolver.Resolve<Array>(ind);
      return chunk.IsNull();
    });
    return NullPartitionResult::NullsAtStart(indices_begin, indices_end, nulls_end);
  } else {
    auto nulls_begin = partitioner(indices_begin, indices_end, [&](uint64_t ind) {
      const auto chunk = resolver.Resolve<Array>(ind);
      return !chunk.IsNull();
    });
    return NullPartitionResult::NullsAtEnd(indices_begin, indices_end, nulls_begin);
  }
}

template <typename ArrayType, typename Partitioner>
enable_if_t<!has_null_like_values<typename ArrayType::TypeClass>::value,
            NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ChunkedArrayResolver& resolver, NullPlacement null_placement) {
  return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
}

template <typename ArrayType, typename Partitioner>
enable_if_t<has_null_like_values<typename ArrayType::TypeClass>::value,
            NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ChunkedArrayResolver& resolver, NullPlacement null_placement) {
  Partitioner partitioner;
  if (null_placement == NullPlacement::AtStart) {
    auto null_likes_end = partitioner(indices_begin, indices_end, [&](uint64_t ind) {
      const auto chunk = resolver.Resolve<ArrayType>(ind);
      return std::isnan(chunk.Value());
    });
    return NullPartitionResult::NullsAtStart(indices_begin, indices_end, null_likes_end);
  } else {
    auto null_likes_begin = partitioner(indices_begin, indices_end, [&](uint64_t ind) {
      const auto chunk = resolver.Resolve<ArrayType>(ind);
      return !std::isnan(chunk.Value());
    });
    return NullPartitionResult::NullsAtEnd(indices_begin, indices_end, null_likes_begin);
  }
}

template <typename ArrayType, typename Partitioner>
NullPartitionResult PartitionNulls(uint64_t* indices_begin, uint64_t* indices_end,
                                   const ChunkedArrayResolver& resolver,
                                   int64_t null_count, NullPlacement null_placement) {
  // Partition nulls at start (resp. end), and null-like values just before (resp. after)
  NullPartitionResult p = PartitionNullsOnly<Partitioner>(
      indices_begin, indices_end, resolver, null_count, null_placement);
  NullPartitionResult q = PartitionNullLikes<ArrayType, Partitioner>(
      p.non_nulls_begin, p.non_nulls_end, resolver, null_placement);
  return NullPartitionResult{q.non_nulls_begin, q.non_nulls_end,
                             std::min(q.nulls_begin, p.nulls_begin),
                             std::max(q.nulls_end, p.nulls_end)};
}

struct MergeImpl {
  using MergeNullsFunc = std::function<void(uint64_t* nulls_begin, uint64_t* nulls_middle,
                                            uint64_t* nulls_end, uint64_t* temp_indices,
                                            int64_t null_count)>;

  using MergeNonNullsFunc =
      std::function<void(uint64_t* range_begin, uint64_t* range_middle,
                         uint64_t* range_end, uint64_t* temp_indices)>;

  MergeImpl(NullPlacement null_placement, MergeNullsFunc&& merge_nulls,
            MergeNonNullsFunc&& merge_non_nulls)
      : null_placement_(null_placement),
        merge_nulls_(std::move(merge_nulls)),
        merge_non_nulls_(std::move(merge_non_nulls)) {}

  Status Init(ExecContext* ctx, int64_t temp_indices_length) {
    ARROW_ASSIGN_OR_RAISE(
        temp_buffer_,
        AllocateBuffer(sizeof(int64_t) * temp_indices_length, ctx->memory_pool()));
    temp_indices_ = reinterpret_cast<uint64_t*>(temp_buffer_->mutable_data());
    return Status::OK();
  }

  NullPartitionResult Merge(const NullPartitionResult& left,
                            const NullPartitionResult& right, int64_t null_count) const {
    if (null_placement_ == NullPlacement::AtStart) {
      return MergeNullsAtStart(left, right, null_count);
    } else {
      return MergeNullsAtEnd(left, right, null_count);
    }
  }

  NullPartitionResult MergeNullsAtStart(const NullPartitionResult& left,
                                        const NullPartitionResult& right,
                                        int64_t null_count) const {
    // Input layout:
    // [left nulls .... left non-nulls .... right nulls .... right non-nulls]
    DCHECK_EQ(left.nulls_end, left.non_nulls_begin);
    DCHECK_EQ(left.non_nulls_end, right.nulls_begin);
    DCHECK_EQ(right.nulls_end, right.non_nulls_begin);

    // Mutate the input, stably, to obtain the following layout:
    // [left nulls .... right nulls .... left non-nulls .... right non-nulls]
    std::rotate(left.non_nulls_begin, right.nulls_begin, right.nulls_end);

    const auto p = NullPartitionResult::NullsAtStart(
        left.nulls_begin, right.non_nulls_end,
        left.nulls_begin + left.null_count() + right.null_count());

    // If the type has null-like values (such as NaN), ensure those plus regular
    // nulls are partitioned in the right order.  Note this assumes that all
    // null-like values (e.g. NaN) are ordered equally.
    if (p.null_count()) {
      merge_nulls_(p.nulls_begin, p.nulls_begin + left.null_count(), p.nulls_end,
                   temp_indices_, null_count);
    }

    // Merge the non-null values into temp area
    DCHECK_EQ(right.non_nulls_begin - p.non_nulls_begin, left.non_null_count());
    DCHECK_EQ(p.non_nulls_end - right.non_nulls_begin, right.non_null_count());
    if (p.non_null_count()) {
      merge_non_nulls_(p.non_nulls_begin, right.non_nulls_begin, p.non_nulls_end,
                       temp_indices_);
    }
    return p;
  }

  NullPartitionResult MergeNullsAtEnd(const NullPartitionResult& left,
                                      const NullPartitionResult& right,
                                      int64_t null_count) const {
    // Input layout:
    // [left non-nulls .... left nulls .... right non-nulls .... right nulls]
    DCHECK_EQ(left.non_nulls_end, left.nulls_begin);
    DCHECK_EQ(left.nulls_end, right.non_nulls_begin);
    DCHECK_EQ(right.non_nulls_end, right.nulls_begin);

    // Mutate the input, stably, to obtain the following layout:
    // [left non-nulls .... right non-nulls .... left nulls .... right nulls]
    std::rotate(left.nulls_begin, right.non_nulls_begin, right.non_nulls_end);

    const auto p = NullPartitionResult::NullsAtEnd(
        left.non_nulls_begin, right.nulls_end,
        left.non_nulls_begin + left.non_null_count() + right.non_null_count());

    // If the type has null-like values (such as NaN), ensure those plus regular
    // nulls are partitioned in the right order.  Note this assumes that all
    // null-like values (e.g. NaN) are ordered equally.
    if (p.null_count()) {
      merge_nulls_(p.nulls_begin, p.nulls_begin + left.null_count(), p.nulls_end,
                   temp_indices_, null_count);
    }

    // Merge the non-null values into temp area
    DCHECK_EQ(left.non_nulls_end - p.non_nulls_begin, left.non_null_count());
    DCHECK_EQ(p.non_nulls_end - left.non_nulls_end, right.non_null_count());
    if (p.non_null_count()) {
      merge_non_nulls_(p.non_nulls_begin, left.non_nulls_end, p.non_nulls_end,
                       temp_indices_);
    }
    return p;
  }

 private:
  NullPlacement null_placement_;
  MergeNullsFunc merge_nulls_;
  MergeNonNullsFunc merge_non_nulls_;
  std::unique_ptr<Buffer> temp_buffer_;
  uint64_t* temp_indices_ = nullptr;
};

// TODO make this usable if indices are non trivial on input
// (see ConcreteRecordBatchColumnSorter)
// `offset` is used when this is called on a chunk of a chunked array
using ArraySortFunc = std::function<NullPartitionResult(
    uint64_t* indices_begin, uint64_t* indices_end, const Array& values, int64_t offset,
    const ArraySortOptions& options)>;

Result<ArraySortFunc> GetArraySorter(const DataType& type);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
