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

#include <algorithm>
#include <cmath>
#include <functional>
#include <iterator>
#include <limits>
#include <numeric>
#include <queue>
#include <type_traits>
#include <unordered_set>
#include <utility>

#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/table.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/optional.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

// Visit all physical types for which sorting is implemented.
#define VISIT_PHYSICAL_TYPES(VISIT) \
  VISIT(BooleanType)                \
  VISIT(Int8Type)                   \
  VISIT(Int16Type)                  \
  VISIT(Int32Type)                  \
  VISIT(Int64Type)                  \
  VISIT(UInt8Type)                  \
  VISIT(UInt16Type)                 \
  VISIT(UInt32Type)                 \
  VISIT(UInt64Type)                 \
  VISIT(FloatType)                  \
  VISIT(DoubleType)                 \
  VISIT(BinaryType)                 \
  VISIT(LargeBinaryType)            \
  VISIT(FixedSizeBinaryType)        \
  VISIT(Decimal128Type)             \
  VISIT(Decimal256Type)

namespace {

struct SortField {
  int field_index;
  SortOrder order;
};

// Return the field indices of the sort keys, deduplicating them along the way
Result<std::vector<SortField>> FindSortKeys(const Schema& schema,
                                            const std::vector<SortKey>& sort_keys) {
  std::vector<SortField> fields;
  std::unordered_set<int> seen;
  fields.reserve(sort_keys.size());
  seen.reserve(sort_keys.size());

  for (const auto& sort_key : sort_keys) {
    const auto r = schema.GetFieldIndex(sort_key.name);
    if (r < 0) {
      return Status::KeyError("Nonexistent sort key column: ", sort_key.name);
    }
    if (seen.insert(r).second) {
      fields.push_back({r, sort_key.order});
    }
  }
  return fields;
}

template <typename ResolvedSortKey, typename ResolvedSortKeyFactory>
Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
    const Schema& schema, const std::vector<SortKey>& sort_keys,
    ResolvedSortKeyFactory&& factory) {
  ARROW_ASSIGN_OR_RAISE(const auto fields, FindSortKeys(schema, sort_keys));
  std::vector<ResolvedSortKey> resolved;
  resolved.reserve(fields.size());
  std::transform(fields.begin(), fields.end(), std::back_inserter(resolved), factory);
  return resolved;
}

template <typename ResolvedSortKey, typename TableOrBatch>
Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
    const TableOrBatch& table_or_batch, const std::vector<SortKey>& sort_keys) {
  return ResolveSortKeys<ResolvedSortKey>(
      *table_or_batch.schema(), sort_keys, [&](const SortField& f) {
        return ResolvedSortKey{table_or_batch.column(f.field_index), f.order};
      });
}

// The target chunk in a chunked array.
template <typename ArrayType>
struct ResolvedChunk {
  using V = GetViewType<typename ArrayType::TypeClass>;
  using LogicalValueType = typename V::T;

  // The target array in chunked array.
  const ArrayType* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const ArrayType* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }

  LogicalValueType Value() const { return V::LogicalValue(array->GetView(index)); }
};

// ResolvedChunk specialization for untyped arrays when all is needed is null lookup
template <>
struct ResolvedChunk<Array> {
  // The target array in chunked array.
  const Array* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const Array* array, int64_t index) : array(array), index(index) {}

  bool IsNull() const { return array->IsNull(index); }
};

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index.
struct ChunkResolver {
  explicit ChunkResolver(std::vector<int64_t> lengths)
      : num_chunks_(static_cast<int64_t>(lengths.size())),
        offsets_(MakeEndOffsets(std::move(lengths))),
        cached_chunk_(0) {}

  ChunkLocation Resolve(int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk_, index - offsets_[cached_chunk_]};
    } else {
      return ResolveMissBisect(index);
    }
  }

  static ChunkResolver FromBatches(const RecordBatchVector& batches) {
    std::vector<int64_t> lengths(batches.size());
    std::transform(
        batches.begin(), batches.end(), lengths.begin(),
        [](const std::shared_ptr<RecordBatch>& batch) { return batch->num_rows(); });
    return ChunkResolver(std::move(lengths));
  }

 protected:
  ChunkLocation ResolveMissBisect(int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    const int64_t* raw_offsets = offsets_.data();
    // Search [lo, lo + n)
    int64_t lo = 0, n = num_chunks_;
    while (n > 1) {
      int64_t m = n >> 1;
      int64_t mid = lo + m;
      if (index >= raw_offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    cached_chunk_ = lo;
    return {lo, index - offsets_[lo]};
  }

  static std::vector<int64_t> MakeEndOffsets(std::vector<int64_t> lengths) {
    int64_t offset = 0;
    for (auto& v : lengths) {
      const auto this_length = v;
      v = offset;
      offset += this_length;
    }
    lengths.push_back(offset);
    return lengths;
  }

  int64_t num_chunks_;
  std::vector<int64_t> offsets_;

  mutable int64_t cached_chunk_;
};

struct ChunkedArrayResolver : protected ChunkResolver {
  explicit ChunkedArrayResolver(const std::vector<const Array*>& chunks)
      : ChunkResolver(MakeLengths(chunks)), chunks_(chunks) {}

  template <typename ArrayType>
  ResolvedChunk<ArrayType> Resolve(int64_t index) const {
    const auto loc = ChunkResolver::Resolve(index);
    return ResolvedChunk<ArrayType>(
        checked_cast<const ArrayType*>(chunks_[loc.chunk_index]), loc.index_in_chunk);
  }

 protected:
  static std::vector<int64_t> MakeLengths(const std::vector<const Array*>& chunks) {
    std::vector<int64_t> lengths(chunks.size());
    std::transform(chunks.begin(), chunks.end(), lengths.begin(),
                   [](const Array* arr) { return arr->length(); });
    return lengths;
  }

  const std::vector<const Array*> chunks_;
};

// We could try to reproduce the concrete Array classes' facilities
// (such as cached raw values pointer) in a separate hierarchy of
// physical accessors, but doing so ends up too cumbersome.
// Instead, we simply create the desired concrete Array objects.
std::shared_ptr<Array> GetPhysicalArray(const Array& array,
                                        const std::shared_ptr<DataType>& physical_type) {
  auto new_data = array.data()->Copy();
  new_data->type = physical_type;
  return MakeArray(std::move(new_data));
}

ArrayVector GetPhysicalChunks(const ArrayVector& chunks,
                              const std::shared_ptr<DataType>& physical_type) {
  ArrayVector physical(chunks.size());
  std::transform(chunks.begin(), chunks.end(), physical.begin(),
                 [&](const std::shared_ptr<Array>& array) {
                   return GetPhysicalArray(*array, physical_type);
                 });
  return physical;
}

ArrayVector GetPhysicalChunks(const ChunkedArray& chunked_array,
                              const std::shared_ptr<DataType>& physical_type) {
  return GetPhysicalChunks(chunked_array.chunks(), physical_type);
}

std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays) {
  std::vector<const Array*> pointers(arrays.size());
  std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                 [&](const std::shared_ptr<Array>& array) { return array.get(); });
  return pointers;
}

Result<RecordBatchVector> BatchesFromTable(const Table& table) {
  RecordBatchVector batches;
  TableBatchReader reader(table);
  RETURN_NOT_OK(reader.ReadAll(&batches));
  return batches;
}

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

// Compare two values, taking NaNs into account

template <typename Type, typename Enable = void>
struct ValueComparator;

template <typename Type>
struct ValueComparator<Type, enable_if_t<!is_floating_type<Type>::value>> {
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
struct ValueComparator<Type, enable_if_t<is_floating_type<Type>::value>> {
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

template <typename TypeClass, typename Enable = void>
struct NullTraits {
  static constexpr bool has_null_like_values = false;
};

template <typename TypeClass>
struct NullTraits<TypeClass, enable_if_floating_point<TypeClass>> {
  static constexpr bool has_null_like_values = true;
};

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

// For chunked array.
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

// Move non-null null-like values to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ArrayType& values, int64_t offset,
                   NullPlacement null_placement) {
  return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
}

template <typename ArrayType, typename Partitioner>
enable_if_t<!is_floating_type<typename ArrayType::TypeClass>::value, NullPartitionResult>
PartitionNullLikes(uint64_t* indices_begin, uint64_t* indices_end,
                   const ChunkedArrayResolver& resolver, NullPlacement null_placement) {
  return NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement);
}

template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, NullPartitionResult>
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

template <typename ArrayType, typename Partitioner>
enable_if_t<is_floating_type<typename ArrayType::TypeClass>::value, NullPartitionResult>
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

// For chunked array.
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

// ----------------------------------------------------------------------
// partition_nth_indices implementation

// We need to preserve the options
using PartitionNthToIndicesState = internal::OptionsWrapper<PartitionNthOptions>;

template <typename OutType, typename InType>
struct PartitionNthToIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using GetView = GetViewType<InType>;

    if (ctx->state() == nullptr) {
      return Status::Invalid("NthToIndices requires PartitionNthOptions");
    }
    const auto& options = PartitionNthToIndicesState::Get(ctx);

    ArrayType arr(batch[0].array());

    const int64_t pivot = options.pivot;
    if (pivot > arr.length()) {
      return Status::IndexError("NthToIndices index out of bound");
    }
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);
    if (pivot == arr.length()) {
      return Status::OK();
    }
    const auto p = PartitionNulls<ArrayType, NonStablePartitioner>(
        out_begin, out_end, arr, 0, options.null_placement);
    auto nth_begin = out_begin + pivot;
    if (nth_begin >= p.non_nulls_begin && nth_begin < p.non_nulls_end) {
      std::nth_element(p.non_nulls_begin, nth_begin, p.non_nulls_end,
                       [&arr](uint64_t left, uint64_t right) {
                         const auto lval = GetView::LogicalValue(arr.GetView(left));
                         const auto rval = GetView::LogicalValue(arr.GetView(right));
                         return lval < rval;
                       });
    }
    return Status::OK();
  }
};

template <typename OutType>
struct PartitionNthToIndices<OutType, NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (ctx->state() == nullptr) {
      return Status::Invalid("NthToIndices requires PartitionNthOptions");
    }
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + batch.length;
    std::iota(out_begin, out_end, 0);
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Array sorting implementations

template <typename ArrayType, typename VisitorNotNull, typename VisitorNull>
inline void VisitRawValuesInline(const ArrayType& values,
                                 VisitorNotNull&& visitor_not_null,
                                 VisitorNull&& visitor_null) {
  const auto data = values.raw_values();
  VisitBitBlocksVoid(
      values.null_bitmap(), values.offset(), values.length(),
      [&](int64_t i) { visitor_not_null(data[i]); }, [&]() { visitor_null(); });
}

template <typename VisitorNotNull, typename VisitorNull>
inline void VisitRawValuesInline(const BooleanArray& values,
                                 VisitorNotNull&& visitor_not_null,
                                 VisitorNull&& visitor_null) {
  if (values.null_count() != 0) {
    const uint8_t* data = values.data()->GetValues<uint8_t>(1, 0);
    VisitBitBlocksVoid(
        values.null_bitmap(), values.offset(), values.length(),
        [&](int64_t i) { visitor_not_null(BitUtil::GetBit(data, values.offset() + i)); },
        [&]() { visitor_null(); });
  } else {
    // Can avoid GetBit() overhead in the no-nulls case
    VisitBitBlocksVoid(
        values.data()->buffers[1], values.offset(), values.length(),
        [&](int64_t i) { visitor_not_null(true); }, [&]() { visitor_not_null(false); });
  }
}

template <typename ArrowType>
class ArrayCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using GetView = GetViewType<ArrowType>;

 public:
  // `offset` is used when this is called on a chunk of a chunked array
  NullPartitionResult Sort(uint64_t* indices_begin, uint64_t* indices_end,
                           const ArrayType& values, int64_t offset,
                           const ArraySortOptions& options) {
    const auto p = PartitionNulls<ArrayType, StablePartitioner>(
        indices_begin, indices_end, values, offset, options.null_placement);
    if (options.order == SortOrder::Ascending) {
      std::stable_sort(
          p.non_nulls_begin, p.non_nulls_end,
          [&values, &offset](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(values.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(values.GetView(right - offset));
            return lhs < rhs;
          });
    } else {
      std::stable_sort(
          p.non_nulls_begin, p.non_nulls_end,
          [&values, &offset](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(values.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(values.GetView(right - offset));
            // We don't use 'left > right' here to reduce required operator.
            // If we use 'right < left' here, '<' is only required.
            return rhs < lhs;
          });
    }
    return p;
  }
};

template <typename ArrowType>
class ArrayCountSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  ArrayCountSorter() = default;

  explicit ArrayCountSorter(c_type min, c_type max) { SetMinMax(min, max); }

  // Assume: max >= min && (max - min) < 4Gi
  void SetMinMax(c_type min, c_type max) {
    min_ = min;
    value_range_ = static_cast<uint32_t>(max - min) + 1;
  }

  NullPartitionResult Sort(uint64_t* indices_begin, uint64_t* indices_end,
                           const ArrayType& values, int64_t offset,
                           const ArraySortOptions& options) const {
    // 32bit counter performs much better than 64bit one
    if (values.length() < (1LL << 32)) {
      return SortInternal<uint32_t>(indices_begin, indices_end, values, offset, options);
    } else {
      return SortInternal<uint64_t>(indices_begin, indices_end, values, offset, options);
    }
  }

 private:
  c_type min_{0};
  uint32_t value_range_{0};

  // `offset` is used when this is called on a chunk of a chunked array
  template <typename CounterType>
  NullPartitionResult SortInternal(uint64_t* indices_begin, uint64_t* indices_end,
                                   const ArrayType& values, int64_t offset,
                                   const ArraySortOptions& options) const {
    const uint32_t value_range = value_range_;

    // first and last slot reserved for prefix sum (depending on sort order)
    std::vector<CounterType> counts(2 + value_range);
    NullPartitionResult p;

    if (options.order == SortOrder::Ascending) {
      // counts will be increasing, starting with 0 and ending with (length - null_count)
      CountValues(values, &counts[1]);
      for (uint32_t i = 1; i <= value_range; ++i) {
        counts[i] += counts[i - 1];
      }

      if (options.null_placement == NullPlacement::AtStart) {
        p = NullPartitionResult::NullsAtStart(indices_begin, indices_end,
                                              indices_end - counts[value_range]);
      } else {
        p = NullPartitionResult::NullsAtEnd(indices_begin, indices_end,
                                            indices_begin + counts[value_range]);
      }
      EmitIndices(p, values, offset, &counts[0]);
    } else {
      // counts will be decreasing, starting with (length - null_count) and ending with 0
      CountValues(values, &counts[0]);
      for (uint32_t i = value_range; i >= 1; --i) {
        counts[i - 1] += counts[i];
      }

      if (options.null_placement == NullPlacement::AtStart) {
        p = NullPartitionResult::NullsAtStart(indices_begin, indices_end,
                                              indices_end - counts[0]);
      } else {
        p = NullPartitionResult::NullsAtEnd(indices_begin, indices_end,
                                            indices_begin + counts[0]);
      }
      EmitIndices(p, values, offset, &counts[1]);
    }
    return p;
  }

  template <typename CounterType>
  void CountValues(const ArrayType& values, CounterType* counts) const {
    VisitRawValuesInline(
        values, [&](c_type v) { ++counts[v - min_]; }, []() {});
  }

  template <typename CounterType>
  void EmitIndices(const NullPartitionResult& p, const ArrayType& values, int64_t offset,
                   CounterType* counts) const {
    int64_t index = offset;
    CounterType count_nulls = 0;
    VisitRawValuesInline(
        values, [&](c_type v) { p.non_nulls_begin[counts[v - min_]++] = index++; },
        [&]() { p.nulls_begin[count_nulls++] = index++; });
  }
};

using ::arrow::internal::Bitmap;

template <>
class ArrayCountSorter<BooleanType> {
 public:
  ArrayCountSorter() = default;

  // `offset` is used when this is called on a chunk of a chunked array
  NullPartitionResult Sort(uint64_t* indices_begin, uint64_t* indices_end,
                           const BooleanArray& values, int64_t offset,
                           const ArraySortOptions& options) {
    std::array<int64_t, 3> counts{0, 0, 0};  // false, true, null

    const int64_t nulls = values.null_count();
    const int64_t ones = values.true_count();
    const int64_t zeros = values.length() - ones - nulls;

    NullPartitionResult p;
    if (options.null_placement == NullPlacement::AtStart) {
      p = NullPartitionResult::NullsAtStart(indices_begin, indices_end,
                                            indices_begin + nulls);
    } else {
      p = NullPartitionResult::NullsAtEnd(indices_begin, indices_end,
                                          indices_end - nulls);
    }

    if (options.order == SortOrder::Ascending) {
      // ones start after zeros
      counts[1] = zeros;
    } else {
      // zeros start after ones
      counts[0] = ones;
    }

    int64_t index = offset;
    VisitRawValuesInline(
        values, [&](bool v) { p.non_nulls_begin[counts[v]++] = index++; },
        [&]() { p.nulls_begin[counts[2]++] = index++; });
    return p;
  }
};

// Sort integers with counting sort or comparison based sorting algorithm
// - Use O(n) counting sort if values are in a small range
// - Use O(nlogn) std::stable_sort otherwise
template <typename ArrowType>
class ArrayCountOrCompareSorter {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using c_type = typename ArrowType::c_type;

 public:
  // `offset` is used when this is called on a chunk of a chunked array
  NullPartitionResult Sort(uint64_t* indices_begin, uint64_t* indices_end,
                           const ArrayType& values, int64_t offset,
                           const ArraySortOptions& options) {
    if (values.length() >= countsort_min_len_ && values.length() > values.null_count()) {
      c_type min, max;
      std::tie(min, max) = GetMinMax<c_type>(*values.data());

      // For signed int32/64, (max - min) may overflow and trigger UBSAN.
      // Cast to largest unsigned type(uint64_t) before subtraction.
      if (static_cast<uint64_t>(max) - static_cast<uint64_t>(min) <=
          countsort_max_range_) {
        count_sorter_.SetMinMax(min, max);
        return count_sorter_.Sort(indices_begin, indices_end, values, offset, options);
      }
    }

    return compare_sorter_.Sort(indices_begin, indices_end, values, offset, options);
  }

 private:
  ArrayCompareSorter<ArrowType> compare_sorter_;
  ArrayCountSorter<ArrowType> count_sorter_;

  // Cross point to prefer counting sort than stl::stable_sort(merge sort)
  // - array to be sorted is longer than "count_min_len_"
  // - value range (max-min) is within "count_max_range_"
  //
  // The optimal setting depends heavily on running CPU. Below setting is
  // conservative to adapt to various hardware and keep code simple.
  // It's possible to decrease array-len and/or increase value-range to cover
  // more cases, or setup a table for best array-len/value-range combinations.
  // See https://issues.apache.org/jira/browse/ARROW-1571 for detailed analysis.
  static const uint32_t countsort_min_len_ = 1024;
  static const uint32_t countsort_max_range_ = 4096;
};

template <typename Type, typename Enable = void>
struct ArraySorter;

template <>
struct ArraySorter<BooleanType> {
  ArrayCountSorter<BooleanType> impl;
};

template <>
struct ArraySorter<UInt8Type> {
  ArrayCountSorter<UInt8Type> impl;
  ArraySorter() : impl(0, 255) {}
};

template <>
struct ArraySorter<Int8Type> {
  ArrayCountSorter<Int8Type> impl;
  ArraySorter() : impl(-128, 127) {}
};

template <typename Type>
struct ArraySorter<Type, enable_if_t<(is_integer_type<Type>::value &&
                                      (sizeof(typename Type::c_type) > 1)) ||
                                     is_temporal_type<Type>::value>> {
  ArrayCountOrCompareSorter<Type> impl;
};

template <typename Type>
struct ArraySorter<
    Type, enable_if_t<is_floating_type<Type>::value || is_base_binary_type<Type>::value ||
                      is_fixed_size_binary_type<Type>::value>> {
  ArrayCompareSorter<Type> impl;
};

using ArraySortIndicesState = internal::OptionsWrapper<ArraySortOptions>;

template <typename OutType, typename InType>
struct ArraySortIndices {
  using ArrayType = typename TypeTraits<InType>::ArrayType;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = ArraySortIndicesState::Get(ctx);

    ArrayType arr(batch[0].array());
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + arr.length();
    std::iota(out_begin, out_end, 0);

    ArraySorter<InType> sorter;
    sorter.impl.Sort(out_begin, out_end, arr, 0, options);

    return Status::OK();
  }
};

template <typename OutType>
struct ArraySortIndices<OutType, NullType> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ArrayData* out_arr = out->mutable_array();
    uint64_t* out_begin = out_arr->GetMutableValues<uint64_t>(1);
    uint64_t* out_end = out_begin + batch.length;
    std::iota(out_begin, out_end, 0);
    return Status::OK();
  }
};

template <template <typename...> class ExecTemplate>
void AddSortingKernels(VectorKernel base, VectorFunction* func) {
  // null type
  base.signature = KernelSignature::Make({InputType::Array(null())}, uint64());
  base.exec = ExecTemplate<UInt64Type, NullType>::Exec;
  DCHECK_OK(func->AddKernel(base));

  // bool type
  base.signature = KernelSignature::Make({InputType::Array(boolean())}, uint64());
  base.exec = ExecTemplate<UInt64Type, BooleanType>::Exec;
  DCHECK_OK(func->AddKernel(base));

  // duration type
  base.signature = KernelSignature::Make({InputType::Array(Type::DURATION)}, uint64());
  base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*int64());
  DCHECK_OK(func->AddKernel(base));

  for (const auto& ty : NumericTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : TemporalTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty->id())}, uint64());
    base.exec = GenerateNumeric<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto id : {Type::DECIMAL128, Type::DECIMAL256}) {
    base.signature = KernelSignature::Make({InputType::Array(id)}, uint64());
    base.exec = GenerateDecimal<ExecTemplate, UInt64Type>(id);
    DCHECK_OK(func->AddKernel(base));
  }
  for (const auto& ty : BaseBinaryTypes()) {
    auto physical_type = GetPhysicalType(ty);
    base.signature = KernelSignature::Make({InputType::Array(ty)}, uint64());
    base.exec = GenerateVarBinaryBase<ExecTemplate, UInt64Type>(*physical_type);
    DCHECK_OK(func->AddKernel(base));
  }
  base.signature =
      KernelSignature::Make({InputType::Array(Type::FIXED_SIZE_BINARY)}, uint64());
  base.exec = ExecTemplate<UInt64Type, FixedSizeBinaryType>::Exec;
  DCHECK_OK(func->AddKernel(base));
}

// ----------------------------------------------------------------------
// ChunkedArray sorting implementation

// Sort a chunked array by sorting each array in the chunked array,
// then merging the sorted chunks recursively.
class ChunkedArraySorter : public TypeVisitor {
 public:
  ChunkedArraySorter(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
                     const ChunkedArray& chunked_array, const SortOrder order,
                     const NullPlacement null_placement)
      : TypeVisitor(),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        chunked_array_(chunked_array),
        physical_type_(GetPhysicalType(chunked_array.type())),
        physical_chunks_(GetPhysicalChunks(chunked_array_, physical_type_)),
        order_(order),
        null_placement_(null_placement),
        ctx_(ctx) {}

  Status Sort() { return physical_type_->Accept(this); }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

  Status Visit(const NullType&) override {
    std::iota(indices_begin_, indices_end_, 0);
    return Status::OK();
  }

 private:
  template <typename Type>
  Status SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    ArraySortOptions options(order_, null_placement_);
    const auto num_chunks = chunked_array_.num_chunks();
    if (num_chunks == 0) {
      return Status::OK();
    }
    const auto arrays = GetArrayPointers(physical_chunks_);

    // Sort each chunk independently and merge to sorted indices.
    // This is a serial implementation.
    ArraySorter<Type> sorter;
    std::vector<NullPartitionResult> sorted(num_chunks);

    // First sort all individual chunks
    int64_t begin_offset = 0;
    int64_t end_offset = 0;
    int64_t null_count = 0;
    for (int i = 0; i < num_chunks; ++i) {
      const auto array = checked_cast<const ArrayType*>(arrays[i]);
      end_offset += array->length();
      null_count += array->null_count();
      sorted[i] =
          sorter.impl.Sort(indices_begin_ + begin_offset, indices_begin_ + end_offset,
                           *array, begin_offset, options);
      begin_offset = end_offset;
    }
    DCHECK_EQ(end_offset, indices_end_ - indices_begin_);

    // Then merge them by pairs, recursively
    if (sorted.size() > 1) {
      auto merge_nulls = [&](uint64_t* nulls_begin, uint64_t* nulls_middle,
                             uint64_t* nulls_end, uint64_t* temp_indices,
                             int64_t null_count) {
        if (NullTraits<typename ArrayType::TypeClass>::has_null_like_values) {
          PartitionNullsOnly<StablePartitioner>(nulls_begin, nulls_end,
                                                ChunkedArrayResolver(arrays), null_count,
                                                null_placement_);
        }
      };
      auto merge_non_nulls = [&](uint64_t* range_begin, uint64_t* range_middle,
                                 uint64_t* range_end, uint64_t* temp_indices) {
        MergeNonNulls<ArrayType>(range_begin, range_middle, range_end, arrays,
                                 temp_indices);
      };

      MergeImpl merge_impl{null_placement_, std::move(merge_nulls),
                           std::move(merge_non_nulls)};
      // std::merge is only called on non-null values, so size temp indices accordingly
      RETURN_NOT_OK(merge_impl.Init(ctx_, indices_end_ - indices_begin_ - null_count));

      while (sorted.size() > 1) {
        auto out_it = sorted.begin();
        auto it = sorted.begin();
        while (it < sorted.end() - 1) {
          const auto& left = *it++;
          const auto& right = *it++;
          DCHECK_EQ(left.overall_end(), right.overall_begin());
          const auto merged = merge_impl.Merge(left, right, null_count);
          *out_it++ = merged;
        }
        if (it < sorted.end()) {
          *out_it++ = *it++;
        }
        sorted.erase(out_it, sorted.end());
      }
    }

    DCHECK_EQ(sorted.size(), 1);
    DCHECK_EQ(sorted[0].overall_begin(), indices_begin_);
    DCHECK_EQ(sorted[0].overall_end(), indices_end_);
    // Note that "nulls" can also include NaNs, hence the >= check
    DCHECK_GE(sorted[0].null_count(), null_count);

    return Status::OK();
  }

  template <typename ArrayType>
  void MergeNonNulls(uint64_t* range_begin, uint64_t* range_middle, uint64_t* range_end,
                     const std::vector<const Array*>& arrays, uint64_t* temp_indices) {
    const ChunkedArrayResolver left_resolver(arrays);
    const ChunkedArrayResolver right_resolver(arrays);

    if (order_ == SortOrder::Ascending) {
      std::merge(range_begin, range_middle, range_middle, range_end, temp_indices,
                 [&](uint64_t left, uint64_t right) {
                   const auto chunk_left = left_resolver.Resolve<ArrayType>(left);
                   const auto chunk_right = right_resolver.Resolve<ArrayType>(right);
                   return chunk_left.Value() < chunk_right.Value();
                 });
    } else {
      std::merge(range_begin, range_middle, range_middle, range_end, temp_indices,
                 [&](uint64_t left, uint64_t right) {
                   const auto chunk_left = left_resolver.Resolve<ArrayType>(left);
                   const auto chunk_right = right_resolver.Resolve<ArrayType>(right);
                   // We don't use 'left > right' here to reduce required
                   // operator. If we use 'right < left' here, '<' is only
                   // required.
                   return chunk_right.Value() < chunk_left.Value();
                 });
    }
    // Copy back temp area into main buffer
    std::copy(temp_indices, temp_indices + (range_end - range_begin), range_begin);
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const ChunkedArray& chunked_array_;
  const std::shared_ptr<DataType> physical_type_;
  const ArrayVector physical_chunks_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  ExecContext* ctx_;
};

// ----------------------------------------------------------------------
// Record batch sorting implementation(s)

// Visit contiguous ranges of equal values.  All entries are assumed
// to be non-null.
template <typename ArrayType, typename Visitor>
void VisitConstantRanges(const ArrayType& array, uint64_t* indices_begin,
                         uint64_t* indices_end, int64_t offset, Visitor&& visit) {
  using GetView = GetViewType<typename ArrayType::TypeClass>;

  if (indices_begin == indices_end) {
    return;
  }
  auto range_start = indices_begin;
  auto range_cur = range_start;
  auto last_value = GetView::LogicalValue(array.GetView(*range_cur - offset));
  while (++range_cur != indices_end) {
    auto v = GetView::LogicalValue(array.GetView(*range_cur - offset));
    if (v != last_value) {
      visit(range_start, range_cur);
      range_start = range_cur;
      last_value = v;
    }
  }
  if (range_start != range_cur) {
    visit(range_start, range_cur);
  }
}

// A sorter for a single column of a RecordBatch, deferring to the next column
// for ranges of equal values.
class RecordBatchColumnSorter {
 public:
  explicit RecordBatchColumnSorter(RecordBatchColumnSorter* next_column = nullptr)
      : next_column_(next_column) {}
  virtual ~RecordBatchColumnSorter() {}

  virtual NullPartitionResult SortRange(uint64_t* indices_begin, uint64_t* indices_end,
                                        int64_t offset) = 0;

 protected:
  RecordBatchColumnSorter* next_column_;
};

template <typename Type>
class ConcreteRecordBatchColumnSorter : public RecordBatchColumnSorter {
 public:
  using ArrayType = typename TypeTraits<Type>::ArrayType;

  ConcreteRecordBatchColumnSorter(std::shared_ptr<Array> array, SortOrder order,
                                  NullPlacement null_placement,
                                  RecordBatchColumnSorter* next_column = nullptr)
      : RecordBatchColumnSorter(next_column),
        owned_array_(std::move(array)),
        array_(checked_cast<const ArrayType&>(*owned_array_)),
        order_(order),
        null_placement_(null_placement),
        null_count_(array_.null_count()) {}

  NullPartitionResult SortRange(uint64_t* indices_begin, uint64_t* indices_end,
                                int64_t offset) override {
    using GetView = GetViewType<Type>;

    NullPartitionResult p;
    if (null_count_ == 0) {
      p = NullPartitionResult::NoNulls(indices_begin, indices_end, null_placement_);
    } else {
      // NOTE that null_count_ is merely an upper bound on the number of nulls
      // in this particular range.
      p = PartitionNullsOnly<StablePartitioner>(indices_begin, indices_end, array_,
                                                offset, null_placement_);
      DCHECK_LE(p.nulls_end - p.nulls_begin, null_count_);
    }
    const NullPartitionResult q = PartitionNullLikes<ArrayType, StablePartitioner>(
        p.non_nulls_begin, p.non_nulls_end, array_, offset, null_placement_);

    // TODO This is roughly the same as ArrayCompareSorter.
    // Also, we would like to use a counting sort if possible.  This requires
    // a counting sort compatible with indirect indexing.
    if (order_ == SortOrder::Ascending) {
      std::stable_sort(
          q.non_nulls_begin, q.non_nulls_end, [&](uint64_t left, uint64_t right) {
            const auto lhs = GetView::LogicalValue(array_.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(array_.GetView(right - offset));
            return lhs < rhs;
          });
    } else {
      std::stable_sort(
          q.non_nulls_begin, q.non_nulls_end, [&](uint64_t left, uint64_t right) {
            // We don't use 'left > right' here to reduce required operator.
            // If we use 'right < left' here, '<' is only required.
            const auto lhs = GetView::LogicalValue(array_.GetView(left - offset));
            const auto rhs = GetView::LogicalValue(array_.GetView(right - offset));
            return lhs > rhs;
          });
    }

    if (next_column_ != nullptr) {
      // Visit all ranges of equal values in this column and sort them on
      // the next column.
      SortNextColumn(q.nulls_begin, q.nulls_end, offset);
      SortNextColumn(p.nulls_begin, p.nulls_end, offset);
      VisitConstantRanges(array_, q.non_nulls_begin, q.non_nulls_end, offset,
                          [&](uint64_t* range_start, uint64_t* range_end) {
                            SortNextColumn(range_start, range_end, offset);
                          });
    }
    return NullPartitionResult{q.non_nulls_begin, q.non_nulls_end,
                               std::min(q.nulls_begin, p.nulls_begin),
                               std::max(q.nulls_end, p.nulls_end)};
  }

  void SortNextColumn(uint64_t* indices_begin, uint64_t* indices_end, int64_t offset) {
    // Avoid the cost of a virtual method call in trivial cases
    if (indices_end - indices_begin > 1) {
      next_column_->SortRange(indices_begin, indices_end, offset);
    }
  }

 protected:
  const std::shared_ptr<Array> owned_array_;
  const ArrayType& array_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  const int64_t null_count_;
};

template <>
class ConcreteRecordBatchColumnSorter<NullType> : public RecordBatchColumnSorter {
 public:
  ConcreteRecordBatchColumnSorter(std::shared_ptr<Array> array, SortOrder order,
                                  NullPlacement null_placement,
                                  RecordBatchColumnSorter* next_column = nullptr)
      : RecordBatchColumnSorter(next_column), null_placement_(null_placement) {}

  NullPartitionResult SortRange(uint64_t* indices_begin, uint64_t* indices_end,
                                int64_t offset) {
    if (next_column_ != nullptr) {
      next_column_->SortRange(indices_begin, indices_end, offset);
    }
    return NullPartitionResult::NullsOnly(indices_begin, indices_end, null_placement_);
  }

 protected:
  const NullPlacement null_placement_;
};

// Sort a batch using a single-pass left-to-right radix sort.
class RadixRecordBatchSorter {
 public:
  RadixRecordBatchSorter(uint64_t* indices_begin, uint64_t* indices_end,
                         const RecordBatch& batch, const SortOptions& options)
      : batch_(batch),
        options_(options),
        indices_begin_(indices_begin),
        indices_end_(indices_end) {}

  // Offset is for table sorting
  Result<NullPartitionResult> Sort(int64_t offset = 0) {
    ARROW_ASSIGN_OR_RAISE(const auto sort_keys,
                          ResolveSortKeys(batch_, options_.sort_keys));

    // Create column sorters from right to left
    std::vector<std::unique_ptr<RecordBatchColumnSorter>> column_sorts(sort_keys.size());
    RecordBatchColumnSorter* next_column = nullptr;
    for (int64_t i = static_cast<int64_t>(sort_keys.size() - 1); i >= 0; --i) {
      ColumnSortFactory factory(sort_keys[i], options_, next_column);
      ARROW_ASSIGN_OR_RAISE(column_sorts[i], factory.MakeColumnSort());
      next_column = column_sorts[i].get();
    }

    // Sort from left to right
    return column_sorts.front()->SortRange(indices_begin_, indices_end_, offset);
  }

 protected:
  struct ResolvedSortKey {
    std::shared_ptr<Array> array;
    SortOrder order;
  };

  struct ColumnSortFactory {
    ColumnSortFactory(const ResolvedSortKey& sort_key, const SortOptions& options,
                      RecordBatchColumnSorter* next_column)
        : physical_type(GetPhysicalType(sort_key.array->type())),
          array(GetPhysicalArray(*sort_key.array, physical_type)),
          order(sort_key.order),
          null_placement(options.null_placement),
          next_column(next_column) {}

    Result<std::unique_ptr<RecordBatchColumnSorter>> MakeColumnSort() {
      RETURN_NOT_OK(VisitTypeInline(*physical_type, this));
      DCHECK_NE(result, nullptr);
      return std::move(result);
    }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return VisitGeneric(type); }

    VISIT_PHYSICAL_TYPES(VISIT)
    VISIT(NullType)

#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for RecordBatch sorting: ",
                               type.ToString());
    }

    template <typename Type>
    Status VisitGeneric(const Type&) {
      result.reset(new ConcreteRecordBatchColumnSorter<Type>(array, order, null_placement,
                                                             next_column));
      return Status::OK();
    }

    std::shared_ptr<DataType> physical_type;
    std::shared_ptr<Array> array;
    SortOrder order;
    NullPlacement null_placement;
    RecordBatchColumnSorter* next_column;
    std::unique_ptr<RecordBatchColumnSorter> result;
  };

  static Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys) {
    return ::arrow::compute::internal::ResolveSortKeys<ResolvedSortKey>(batch, sort_keys);
  }

  const RecordBatch& batch_;
  const SortOptions& options_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
};

// Compare two records in a single column (either from a batch or table)
template <typename ResolvedSortKey>
struct ColumnComparator {
  using Location = typename ResolvedSortKey::LocationType;

  ColumnComparator(const ResolvedSortKey& sort_key, NullPlacement null_placement)
      : sort_key_(sort_key), null_placement_(null_placement) {}

  virtual ~ColumnComparator() = default;

  virtual int Compare(const Location& left, const Location& right) const = 0;

  ResolvedSortKey sort_key_;
  NullPlacement null_placement_;
};

template <typename ResolvedSortKey, typename Type>
struct ConcreteColumnComparator : public ColumnComparator<ResolvedSortKey> {
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using Location = typename ResolvedSortKey::LocationType;

  using ColumnComparator<ResolvedSortKey>::ColumnComparator;

  int Compare(const Location& left, const Location& right) const override {
    const auto& sort_key = this->sort_key_;

    const auto chunk_left = sort_key.template GetChunk<ArrayType>(left);
    const auto chunk_right = sort_key.template GetChunk<ArrayType>(right);
    if (sort_key.null_count > 0) {
      const bool is_null_left = chunk_left.IsNull();
      const bool is_null_right = chunk_right.IsNull();
      if (is_null_left && is_null_right) {
        return 0;
      } else if (is_null_left) {
        return this->null_placement_ == NullPlacement::AtStart ? -1 : 1;
      } else if (is_null_right) {
        return this->null_placement_ == NullPlacement::AtStart ? 1 : -1;
      }
    }
    return CompareTypeValues<Type>(chunk_left.Value(), chunk_right.Value(),
                                   sort_key.order, this->null_placement_);
  }
};

template <typename ResolvedSortKey>
struct ConcreteColumnComparator<ResolvedSortKey, NullType>
    : public ColumnComparator<ResolvedSortKey> {
  using Location = typename ResolvedSortKey::LocationType;

  using ColumnComparator<ResolvedSortKey>::ColumnComparator;

  int Compare(const Location& left, const Location& right) const override { return 0; }
};

// Compare two records in the same RecordBatch or Table
// (indexing is handled through ResolvedSortKey)
template <typename ResolvedSortKey>
class MultipleKeyComparator {
 public:
  using Location = typename ResolvedSortKey::LocationType;

  MultipleKeyComparator(const std::vector<ResolvedSortKey>& sort_keys,
                        NullPlacement null_placement)
      : sort_keys_(sort_keys), null_placement_(null_placement) {
    status_ &= MakeComparators();
  }

  Status status() const { return status_; }

  // Returns true if the left-th value should be ordered before the
  // right-th value, false otherwise. The start_sort_key_index-th
  // sort key and subsequent sort keys are used for comparison.
  bool Compare(const Location& left, const Location& right, size_t start_sort_key_index) {
    return CompareInternal(left, right, start_sort_key_index) < 0;
  }

  bool Equals(const Location& left, const Location& right, size_t start_sort_key_index) {
    return CompareInternal(left, right, start_sort_key_index) == 0;
  }

 private:
  struct ColumnComparatorFactory {
#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return VisitGeneric(type); }

    VISIT_PHYSICAL_TYPES(VISIT)
    VISIT(NullType)

#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for batch or table sorting: ",
                               type.ToString());
    }

    template <typename Type>
    Status VisitGeneric(const Type& type) {
      res.reset(
          new ConcreteColumnComparator<ResolvedSortKey, Type>{sort_key, null_placement});
      return Status::OK();
    }

    const ResolvedSortKey& sort_key;
    NullPlacement null_placement;
    std::unique_ptr<ColumnComparator<ResolvedSortKey>> res;
  };

  Status MakeComparators() {
    column_comparators_.reserve(sort_keys_.size());

    for (const auto& sort_key : sort_keys_) {
      ColumnComparatorFactory factory{sort_key, null_placement_, nullptr};
      RETURN_NOT_OK(VisitTypeInline(*sort_key.type, &factory));
      column_comparators_.push_back(std::move(factory.res));
    }
    return Status::OK();
  }

  // Compare two records in the same table and return -1, 0 or 1.
  //
  // -1: The left is less than the right.
  // 0: The left equals to the right.
  // 1: The left is greater than the right.
  //
  // This supports null and NaN. Null is processed in this and NaN
  // is processed in CompareTypeValue().
  int CompareInternal(const Location& left, const Location& right,
                      size_t start_sort_key_index) {
    const auto num_sort_keys = sort_keys_.size();
    for (size_t i = start_sort_key_index; i < num_sort_keys; ++i) {
      const int r = column_comparators_[i]->Compare(left, right);
      if (r != 0) {
        return r;
      }
    }
    return 0;
  }

  const std::vector<ResolvedSortKey>& sort_keys_;
  const NullPlacement null_placement_;
  std::vector<std::unique_ptr<ColumnComparator<ResolvedSortKey>>> column_comparators_;
  Status status_;
};

// Sort a batch using a single sort and multiple-key comparisons.
class MultipleKeyRecordBatchSorter : public TypeVisitor {
 public:
  // Preprocessed sort key.
  struct ResolvedSortKey {
    ResolvedSortKey(const std::shared_ptr<Array>& array, SortOrder order)
        : type(GetPhysicalType(array->type())),
          owned_array(GetPhysicalArray(*array, type)),
          array(*owned_array),
          order(order),
          null_count(array->null_count()) {}

    using LocationType = int64_t;

    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(int64_t index) const {
      return {&checked_cast<const ArrayType&>(array), index};
    }

    const std::shared_ptr<DataType> type;
    std::shared_ptr<Array> owned_array;
    const Array& array;
    SortOrder order;
    int64_t null_count;
  };

 private:
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  MultipleKeyRecordBatchSorter(uint64_t* indices_begin, uint64_t* indices_end,
                               const RecordBatch& batch, const SortOptions& options)
      : indices_begin_(indices_begin),
        indices_end_(indices_end),
        sort_keys_(ResolveSortKeys(batch, options.sort_keys, &status_)),
        null_placement_(options.null_placement),
        comparator_(sort_keys_, null_placement_) {}

  // This is optimized for the first sort key. The first sort key sort
  // is processed in this class. The second and following sort keys
  // are processed in Comparator.
  Status Sort() {
    RETURN_NOT_OK(status_);
    return sort_keys_[0].type->Accept(this);
  }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_PHYSICAL_TYPES(VISIT)
  VISIT(NullType)

#undef VISIT

 private:
  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys, Status* status) {
    const auto maybe_resolved =
        ::arrow::compute::internal::ResolveSortKeys<ResolvedSortKey>(batch, sort_keys);
    if (!maybe_resolved.ok()) {
      *status = maybe_resolved.status();
      return {};
    }
    return *std::move(maybe_resolved);
  }

  template <typename Type>
  enable_if_t<!is_null_type<Type>::value, Status> SortInternal() {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    using GetView = GetViewType<Type>;

    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];
    const ArrayType& array = checked_cast<const ArrayType&>(first_sort_key.array);
    const auto p = PartitionNullsInternal<Type>(first_sort_key);

    // Sort first-key non-nulls
    std::stable_sort(
        p.non_nulls_begin, p.non_nulls_end, [&](uint64_t left, uint64_t right) {
          // Both values are never null nor NaN
          // (otherwise they've been partitioned away above).
          const auto value_left = GetView::LogicalValue(array.GetView(left));
          const auto value_right = GetView::LogicalValue(array.GetView(right));
          if (value_left != value_right) {
            bool compared = value_left < value_right;
            if (first_sort_key.order == SortOrder::Ascending) {
              return compared;
            } else {
              return !compared;
            }
          }
          // If the left value equals to the right value,
          // we need to compare the second and following
          // sort keys.
          return comparator.Compare(left, right, 1);
        });
    return comparator_.status();
  }

  template <typename Type>
  enable_if_null<Type, Status> SortInternal() {
    std::stable_sort(indices_begin_, indices_end_, [&](uint64_t left, uint64_t right) {
      return comparator_.Compare(left, right, 1);
    });
    return comparator_.status();
  }

  // Behaves like PartitionNulls() but this supports multiple sort keys.
  template <typename Type>
  NullPartitionResult PartitionNullsInternal(const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;
    const ArrayType& array = checked_cast<const ArrayType&>(first_sort_key.array);

    const auto p = PartitionNullsOnly<StablePartitioner>(indices_begin_, indices_end_,
                                                         array, 0, null_placement_);
    const auto q = PartitionNullLikes<ArrayType, StablePartitioner>(
        p.non_nulls_begin, p.non_nulls_end, array, 0, null_placement_);

    auto& comparator = comparator_;
    if (q.nulls_begin != q.nulls_end) {
      // Sort all NaNs by the second and following sort keys.
      // TODO: could we instead run an independent sort from the second key on
      // this slice?
      std::stable_sort(q.nulls_begin, q.nulls_end,
                       [&comparator](uint64_t left, uint64_t right) {
                         return comparator.Compare(left, right, 1);
                       });
    }
    if (p.nulls_begin != p.nulls_end) {
      // Sort all nulls by the second and following sort keys.
      // TODO: could we instead run an independent sort from the second key on
      // this slice?
      std::stable_sort(p.nulls_begin, p.nulls_end,
                       [&comparator](uint64_t left, uint64_t right) {
                         return comparator.Compare(left, right, 1);
                       });
    }
    return q;
  }

  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Status status_;
  std::vector<ResolvedSortKey> sort_keys_;
  NullPlacement null_placement_;
  Comparator comparator_;
};

// ----------------------------------------------------------------------
// Table sorting implementation(s)

// Sort a table using an explicit merge sort.
// Each batch is first sorted individually (taking advantage of the fact
// that batch columns are contiguous and therefore have less indexing
// overhead), then sorted batches are merged recursively.
class TableSorter {
 public:
  // Preprocessed sort key.
  struct ResolvedSortKey {
    ResolvedSortKey(const std::shared_ptr<DataType>& type, ArrayVector chunks,
                    SortOrder order, int64_t null_count)
        : type(GetPhysicalType(type)),
          owned_chunks(std::move(chunks)),
          chunks(GetArrayPointers(owned_chunks)),
          order(order),
          null_count(null_count) {}

    using LocationType = ChunkLocation;

    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(ChunkLocation loc) const {
      return {checked_cast<const ArrayType*>(chunks[loc.chunk_index]),
              loc.index_in_chunk};
    }

    // Make a vector of ResolvedSortKeys for the sort keys and the given table.
    // `batches` must be a chunking of `table`.
    static Result<std::vector<ResolvedSortKey>> Make(
        const Table& table, const RecordBatchVector& batches,
        const std::vector<SortKey>& sort_keys) {
      auto factory = [&](const SortField& f) {
        const auto& type = table.schema()->field(f.field_index)->type();
        // We must expose a homogenous chunking for all ResolvedSortKey,
        // so we can't simply pass `table.column(f.field_index)`
        ArrayVector chunks(batches.size());
        std::transform(batches.begin(), batches.end(), chunks.begin(),
                       [&](const std::shared_ptr<RecordBatch>& batch) {
                         return batch->column(f.field_index);
                       });
        return ResolvedSortKey(type, std::move(chunks), f.order,
                               table.column(f.field_index)->null_count());
      };

      return ::arrow::compute::internal::ResolveSortKeys<ResolvedSortKey>(
          *table.schema(), sort_keys, factory);
    }

    std::shared_ptr<DataType> type;
    ArrayVector owned_chunks;
    std::vector<const Array*> chunks;
    SortOrder order;
    int64_t null_count;
  };

  // TODO make all methods const and defer initialization into a Init() method?

  TableSorter(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
              const Table& table, const SortOptions& options)
      : ctx_(ctx),
        table_(table),
        batches_(MakeBatches(table, &status_)),
        options_(options),
        null_placement_(options.null_placement),
        left_resolver_(ChunkResolver::FromBatches(batches_)),
        right_resolver_(ChunkResolver::FromBatches(batches_)),
        sort_keys_(ResolveSortKeys(table, batches_, options.sort_keys, &status_)),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        comparator_(sort_keys_, null_placement_) {}

  // This is optimized for null partitioning and merging along the first sort key.
  // Other sort keys are delegated to the Comparator class.
  Status Sort() {
    ARROW_RETURN_NOT_OK(status_);
    return SortInternal();
  }

 private:
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

  static RecordBatchVector MakeBatches(const Table& table, Status* status) {
    const auto maybe_batches = BatchesFromTable(table);
    if (!maybe_batches.ok()) {
      *status = maybe_batches.status();
      return {};
    }
    return *std::move(maybe_batches);
  }

  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const Table& table, const RecordBatchVector& batches,
      const std::vector<SortKey>& sort_keys, Status* status) {
    const auto maybe_resolved = ResolvedSortKey::Make(table, batches, sort_keys);
    if (!maybe_resolved.ok()) {
      *status = maybe_resolved.status();
      return {};
    }
    return *std::move(maybe_resolved);
  }

  Status SortInternal() {
    // Sort each batch independently and merge to sorted indices.
    RecordBatchVector batches;
    {
      TableBatchReader reader(table_);
      RETURN_NOT_OK(reader.ReadAll(&batches));
    }
    const int64_t num_batches = static_cast<int64_t>(batches.size());
    if (num_batches == 0) {
      return Status::OK();
    }
    std::vector<NullPartitionResult> sorted(num_batches);

    // First sort all individual batches
    int64_t begin_offset = 0;
    int64_t end_offset = 0;
    int64_t null_count = 0;
    for (int64_t i = 0; i < num_batches; ++i) {
      const auto& batch = *batches[i];
      end_offset += batch.num_rows();
      RadixRecordBatchSorter sorter(indices_begin_ + begin_offset,
                                    indices_begin_ + end_offset, batch, options_);
      ARROW_ASSIGN_OR_RAISE(sorted[i], sorter.Sort(begin_offset));
      DCHECK_EQ(sorted[i].overall_begin(), indices_begin_ + begin_offset);
      DCHECK_EQ(sorted[i].overall_end(), indices_begin_ + end_offset);
      DCHECK_EQ(sorted[i].non_null_count() + sorted[i].null_count(), batch.num_rows());
      begin_offset = end_offset;
      // XXX this is an upper bound on the true null count
      null_count += sorted[i].null_count();
    }
    DCHECK_EQ(end_offset, indices_end_ - indices_begin_);

    // Then merge them by pairs, recursively
    if (sorted.size() > 1) {
      struct Visitor {
        TableSorter* sorter;
        std::vector<NullPartitionResult>* sorted;
        int64_t null_count;

#define VISIT(TYPE)                                                     \
  Status Visit(const TYPE& type) {                                      \
    return sorter->MergeInternal<TYPE>(std::move(*sorted), null_count); \
  }

        VISIT_PHYSICAL_TYPES(VISIT)
        VISIT(NullType)
#undef VISIT

        Status Visit(const DataType& type) {
          return Status::NotImplemented("Unsupported type for sorting: ",
                                        type.ToString());
        }
      };
      Visitor visitor{this, &sorted, null_count};
      RETURN_NOT_OK(VisitTypeInline(*sort_keys_[0].type, &visitor));
    }
    return Status::OK();
  }

  // Recursive merge routine, typed on the first sort key
  template <typename Type>
  Status MergeInternal(std::vector<NullPartitionResult> sorted, int64_t null_count) {
    auto merge_nulls = [&](uint64_t* nulls_begin, uint64_t* nulls_middle,
                           uint64_t* nulls_end, uint64_t* temp_indices,
                           int64_t null_count) {
      MergeNulls<Type>(nulls_begin, nulls_middle, nulls_end, temp_indices, null_count);
    };
    auto merge_non_nulls = [&](uint64_t* range_begin, uint64_t* range_middle,
                               uint64_t* range_end, uint64_t* temp_indices) {
      MergeNonNulls<Type>(range_begin, range_middle, range_end, temp_indices);
    };

    MergeImpl merge_impl(options_.null_placement, std::move(merge_nulls),
                         std::move(merge_non_nulls));
    RETURN_NOT_OK(merge_impl.Init(ctx_, table_.num_rows()));

    while (sorted.size() > 1) {
      auto out_it = sorted.begin();
      auto it = sorted.begin();
      while (it < sorted.end() - 1) {
        const auto& left = *it++;
        const auto& right = *it++;
        DCHECK_EQ(left.overall_end(), right.overall_begin());
        *out_it++ = merge_impl.Merge(left, right, null_count);
      }
      if (it < sorted.end()) {
        *out_it++ = *it++;
      }
      sorted.erase(out_it, sorted.end());
    }
    DCHECK_EQ(sorted.size(), 1);
    DCHECK_EQ(sorted[0].overall_begin(), indices_begin_);
    DCHECK_EQ(sorted[0].overall_end(), indices_end_);
    return comparator_.status();
  }

  // Merge rows with a null or a null-like in the first sort key
  template <typename Type>
  enable_if_t<NullTraits<Type>::has_null_like_values> MergeNulls(uint64_t* nulls_begin,
                                                                 uint64_t* nulls_middle,
                                                                 uint64_t* nulls_end,
                                                                 uint64_t* temp_indices,
                                                                 int64_t null_count) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];

    std::merge(nulls_begin, nulls_middle, nulls_middle, nulls_end, temp_indices,
               [&](uint64_t left, uint64_t right) {
                 // First column is either null or nan
                 const auto left_loc = left_resolver_.Resolve(left);
                 const auto right_loc = right_resolver_.Resolve(right);
                 auto chunk_left = first_sort_key.GetChunk<ArrayType>(left_loc);
                 auto chunk_right = first_sort_key.GetChunk<ArrayType>(right_loc);
                 const auto left_is_null = chunk_left.IsNull();
                 const auto right_is_null = chunk_right.IsNull();
                 if (left_is_null == right_is_null) {
                   return comparator.Compare(left_loc, right_loc, 1);
                 } else if (options_.null_placement == NullPlacement::AtEnd) {
                   return right_is_null;
                 } else {
                   return left_is_null;
                 }
               });
    // Copy back temp area into main buffer
    std::copy(temp_indices, temp_indices + (nulls_end - nulls_begin), nulls_begin);
  }

  template <typename Type>
  enable_if_t<!NullTraits<Type>::has_null_like_values> MergeNulls(uint64_t* nulls_begin,
                                                                  uint64_t* nulls_middle,
                                                                  uint64_t* nulls_end,
                                                                  uint64_t* temp_indices,
                                                                  int64_t null_count) {
    MergeNullsOnly(nulls_begin, nulls_middle, nulls_end, temp_indices, null_count);
  }

  void MergeNullsOnly(uint64_t* nulls_begin, uint64_t* nulls_middle, uint64_t* nulls_end,
                      uint64_t* temp_indices, int64_t null_count) {
    // Untyped implementation
    auto& comparator = comparator_;

    std::merge(nulls_begin, nulls_middle, nulls_middle, nulls_end, temp_indices,
               [&](uint64_t left, uint64_t right) {
                 // First column is always null
                 const auto left_loc = left_resolver_.Resolve(left);
                 const auto right_loc = right_resolver_.Resolve(right);
                 return comparator.Compare(left_loc, right_loc, 1);
               });
    // Copy back temp area into main buffer
    std::copy(temp_indices, temp_indices + (nulls_end - nulls_begin), nulls_begin);
  }

  //
  // Merge rows with a non-null in the first sort key
  //
  template <typename Type>
  enable_if_t<!is_null_type<Type>::value> MergeNonNulls(uint64_t* range_begin,
                                                        uint64_t* range_middle,
                                                        uint64_t* range_end,
                                                        uint64_t* temp_indices) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];

    std::merge(range_begin, range_middle, range_middle, range_end, temp_indices,
               [&](uint64_t left, uint64_t right) {
                 // Both values are never null nor NaN.
                 const auto left_loc = left_resolver_.Resolve(left);
                 const auto right_loc = right_resolver_.Resolve(right);
                 auto chunk_left = first_sort_key.GetChunk<ArrayType>(left_loc);
                 auto chunk_right = first_sort_key.GetChunk<ArrayType>(right_loc);
                 DCHECK(!chunk_left.IsNull());
                 DCHECK(!chunk_right.IsNull());
                 auto value_left = chunk_left.Value();
                 auto value_right = chunk_right.Value();
                 if (value_left == value_right) {
                   // If the left value equals to the right value,
                   // we need to compare the second and following
                   // sort keys.
                   return comparator.Compare(left_loc, right_loc, 1);
                 } else {
                   auto compared = value_left < value_right;
                   if (first_sort_key.order == SortOrder::Ascending) {
                     return compared;
                   } else {
                     return !compared;
                   }
                 }
               });
    // Copy back temp area into main buffer
    std::copy(temp_indices, temp_indices + (range_end - range_begin), range_begin);
  }

  template <typename Type>
  enable_if_null<Type> MergeNonNulls(uint64_t* range_begin, uint64_t* range_middle,
                                     uint64_t* range_end, uint64_t* temp_indices) {
    const int64_t null_count = range_end - range_begin;
    MergeNullsOnly(range_begin, range_middle, range_end, temp_indices, null_count);
  }

  ExecContext* ctx_;
  const Table& table_;
  const RecordBatchVector batches_;
  const SortOptions& options_;
  const NullPlacement null_placement_;
  const ChunkResolver left_resolver_, right_resolver_;
  const std::vector<ResolvedSortKey> sort_keys_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Comparator comparator_;
  Status status_;
};

// ----------------------------------------------------------------------
// Top-level sort functions

const auto kDefaultSortOptions = SortOptions::Defaults();

const FunctionDoc sort_indices_doc(
    "Return the indices that would sort an array, record batch or table",
    ("This function computes an array of indices that define a stable sort\n"
     "of the input array, record batch or table.  By default, nNull values are\n"
     "considered greater than any other value and are therefore sorted at the\n"
     "end of the input. For floating-point types, NaNs are considered greater\n"
     "than any other non-null value, but smaller than null values.\n"
     "\n"
     "The handling of nulls and NaNs can be changed in SortOptions."),
    {"input"}, "SortOptions");

class SortIndicesMetaFunction : public MetaFunction {
 public:
  SortIndicesMetaFunction()
      : MetaFunction("sort_indices", Arity::Unary(), &sort_indices_doc,
                     &kDefaultSortOptions) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    const SortOptions& sort_options = static_cast<const SortOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        return SortIndices(*args[0].make_array(), sort_options, ctx);
        break;
      case Datum::CHUNKED_ARRAY:
        return SortIndices(*args[0].chunked_array(), sort_options, ctx);
        break;
      case Datum::RECORD_BATCH: {
        return SortIndices(*args[0].record_batch(), sort_options, ctx);
      } break;
      case Datum::TABLE:
        return SortIndices(*args[0].table(), sort_options, ctx);
        break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for sort_indices operation: "
        "values=",
        args[0].ToString());
  }

 private:
  Result<Datum> SortIndices(const Array& values, const SortOptions& options,
                            ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }
    ArraySortOptions array_options(order, options.null_placement);
    return CallFunction("array_sort_indices", {values}, &array_options, ctx);
  }

  Result<Datum> SortIndices(const ChunkedArray& chunked_array, const SortOptions& options,
                            ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }

    auto out_type = uint64();
    auto length = chunked_array.length();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    ChunkedArraySorter sorter(ctx, out_begin, out_end, chunked_array, order,
                              options.null_placement);
    ARROW_RETURN_NOT_OK(sorter.Sort());
    return Datum(out);
  }

  Result<Datum> SortIndices(const RecordBatch& batch, const SortOptions& options,
                            ExecContext* ctx) const {
    auto n_sort_keys = options.sort_keys.size();
    if (n_sort_keys == 0) {
      return Status::Invalid("Must specify one or more sort keys");
    }
    if (n_sort_keys == 1) {
      auto array = batch.GetColumnByName(options.sort_keys[0].name);
      if (!array) {
        return Status::Invalid("Nonexistent sort key column: ",
                               options.sort_keys[0].name);
      }
      return SortIndices(*array, options, ctx);
    }

    auto out_type = uint64();
    auto length = batch.num_rows();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    BufferVector buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    // Radix sorting is consistently faster except when there is a large number
    // of sort keys, in which case it can end up degrading catastrophically.
    // Cut off above 8 sort keys.
    if (n_sort_keys <= 8) {
      RadixRecordBatchSorter sorter(out_begin, out_end, batch, options);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    } else {
      MultipleKeyRecordBatchSorter sorter(out_begin, out_end, batch, options);
      ARROW_RETURN_NOT_OK(sorter.Sort());
    }
    return Datum(out);
  }

  Result<Datum> SortIndices(const Table& table, const SortOptions& options,
                            ExecContext* ctx) const {
    auto n_sort_keys = options.sort_keys.size();
    if (n_sort_keys == 0) {
      return Status::Invalid("Must specify one or more sort keys");
    }
    if (n_sort_keys == 1) {
      auto chunked_array = table.GetColumnByName(options.sort_keys[0].name);
      if (!chunked_array) {
        return Status::Invalid("Nonexistent sort key column: ",
                               options.sort_keys[0].name);
      }
      return SortIndices(*chunked_array, options, ctx);
    }

    auto out_type = uint64();
    auto length = table.num_rows();
    auto buffer_size = BitUtil::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    TableSorter sorter(ctx, out_begin, out_end, table, options);
    RETURN_NOT_OK(sorter.Sort());

    return Datum(out);
  }
};

// ----------------------------------------------------------------------
// TopK/BottomK implementations

const auto kDefaultSelectKOptions = SelectKOptions::Defaults();

const FunctionDoc select_k_unstable_doc(
    "Selects the indices of the first `k` ordered elements from the input",
    ("This function selects an array of indices of the first `k` ordered elements from\n"
     "the input array, record batch or table specified in the column keys\n"
     "(`options.sort_keys`). Output is not guaranteed to be stable.\n"
     "The columns that are not specified are returned as well, but not used for\n"
     "ordering. Null values are considered  greater than any other value and are\n"
     "therefore sorted at the end of the array. For floating-point types, ordering of\n"
     "values is such that: Null > NaN > Inf > number."),
    {"input"}, "SelectKOptions");

Result<std::shared_ptr<ArrayData>> MakeMutableUInt64Array(
    std::shared_ptr<DataType> out_type, int64_t length, MemoryPool* memory_pool) {
  auto buffer_size = length * sizeof(uint64_t);
  ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(buffer_size, memory_pool));
  return ArrayData::Make(uint64(), length, {nullptr, std::move(data)}, /*null_count=*/0);
}

template <SortOrder order>
class SelectKComparator {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval);
};

template <>
class SelectKComparator<SortOrder::Ascending> {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval) {
    return lval < rval;
  }
};

template <>
class SelectKComparator<SortOrder::Descending> {
 public:
  template <typename Type>
  bool operator()(const Type& lval, const Type& rval) {
    return rval < lval;
  }
};

class ArraySelecter : public TypeVisitor {
 public:
  ArraySelecter(ExecContext* ctx, const Array& array, const SelectKOptions& options,
                Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        array_(array),
        k_(options.k),
        order_(options.sort_keys[0].order),
        physical_type_(GetPhysicalType(array.type())),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

#define VISIT(TYPE)                                           \
  Status Visit(const TYPE& type) {                            \
    if (order_ == SortOrder::Ascending) {                     \
      return SelectKthInternal<TYPE, SortOrder::Ascending>(); \
    }                                                         \
    return SelectKthInternal<TYPE, SortOrder::Descending>();  \
  }

  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    ArrayType arr(array_.data());
    std::vector<uint64_t> indices(arr.length());

    uint64_t* indices_begin = indices.data();
    uint64_t* indices_end = indices_begin + indices.size();
    std::iota(indices_begin, indices_end, 0);
    if (k_ > arr.length()) {
      k_ = arr.length();
    }

    const auto p = PartitionNulls<ArrayType, NonStablePartitioner>(
        indices_begin, indices_end, arr, 0, NullPlacement::AtEnd);
    const auto end_iter = p.non_nulls_end;

    auto kth_begin = std::min(indices_begin + k_, end_iter);

    SelectKComparator<sort_order> comparator;
    auto cmp = [&arr, &comparator](uint64_t left, uint64_t right) {
      const auto lval = GetView::LogicalValue(arr.GetView(left));
      const auto rval = GetView::LogicalValue(arr.GetView(right));
      return comparator(lval, rval);
    };
    using HeapContainer =
        std::priority_queue<uint64_t, std::vector<uint64_t>, decltype(cmp)>;
    HeapContainer heap(indices_begin, kth_begin, cmp);
    for (auto iter = kth_begin; iter != end_iter && !heap.empty(); ++iter) {
      uint64_t x_index = *iter;
      if (cmp(x_index, heap.top())) {
        heap.pop();
        heap.push(x_index);
      }
    }
    int64_t out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices, MakeMutableUInt64Array(uint64(), out_size,
                                                                    ctx_->memory_pool()));

    auto* out_cbegin = take_indices->GetMutableValues<uint64_t>(1) + out_size - 1;
    while (heap.size() > 0) {
      *out_cbegin = heap.top();
      heap.pop();
      --out_cbegin;
    }
    *output_ = Datum(take_indices);
    return Status::OK();
  }

  ExecContext* ctx_;
  const Array& array_;
  int64_t k_;
  SortOrder order_;
  const std::shared_ptr<DataType> physical_type_;
  Datum* output_;
};

template <typename ArrayType>
struct TypedHeapItem {
  uint64_t index;
  uint64_t offset;
  ArrayType* array;
};

class ChunkedArraySelecter : public TypeVisitor {
 public:
  ChunkedArraySelecter(ExecContext* ctx, const ChunkedArray& chunked_array,
                       const SelectKOptions& options, Datum* output)
      : TypeVisitor(),
        chunked_array_(chunked_array),
        physical_type_(GetPhysicalType(chunked_array.type())),
        physical_chunks_(GetPhysicalChunks(chunked_array_, physical_type_)),
        k_(options.k),
        order_(options.sort_keys[0].order),
        ctx_(ctx),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

#define VISIT(TYPE)                                           \
  Status Visit(const TYPE& type) {                            \
    if (order_ == SortOrder::Ascending) {                     \
      return SelectKthInternal<TYPE, SortOrder::Ascending>(); \
    }                                                         \
    return SelectKthInternal<TYPE, SortOrder::Descending>();  \
  }

  VISIT_PHYSICAL_TYPES(VISIT)
#undef VISIT

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    using HeapItem = TypedHeapItem<ArrayType>;

    const auto num_chunks = chunked_array_.num_chunks();
    if (num_chunks == 0) {
      return Status::OK();
    }
    if (k_ > chunked_array_.length()) {
      k_ = chunked_array_.length();
    }
    std::function<bool(const HeapItem&, const HeapItem&)> cmp;
    SelectKComparator<sort_order> comparator;

    cmp = [&comparator](const HeapItem& left, const HeapItem& right) -> bool {
      const auto lval = GetView::LogicalValue(left.array->GetView(left.index));
      const auto rval = GetView::LogicalValue(right.array->GetView(right.index));
      return comparator(lval, rval);
    };
    using HeapContainer =
        std::priority_queue<HeapItem, std::vector<HeapItem>, decltype(cmp)>;

    HeapContainer heap(cmp);
    std::vector<std::shared_ptr<ArrayType>> chunks_holder;
    uint64_t offset = 0;
    for (const auto& chunk : physical_chunks_) {
      if (chunk->length() == 0) continue;
      chunks_holder.emplace_back(std::make_shared<ArrayType>(chunk->data()));
      ArrayType& arr = *chunks_holder[chunks_holder.size() - 1];

      std::vector<uint64_t> indices(arr.length());
      uint64_t* indices_begin = indices.data();
      uint64_t* indices_end = indices_begin + indices.size();
      std::iota(indices_begin, indices_end, 0);

      const auto p = PartitionNulls<ArrayType, NonStablePartitioner>(
          indices_begin, indices_end, arr, 0, NullPlacement::AtEnd);
      const auto end_iter = p.non_nulls_end;

      auto kth_begin = std::min(indices_begin + k_, end_iter);
      uint64_t* iter = indices_begin;
      for (; iter != kth_begin && heap.size() < static_cast<size_t>(k_); ++iter) {
        heap.push(HeapItem{*iter, offset, &arr});
      }
      for (; iter != end_iter && !heap.empty(); ++iter) {
        uint64_t x_index = *iter;
        const auto& xval = GetView::LogicalValue(arr.GetView(x_index));
        auto top_item = heap.top();
        const auto& top_value =
            GetView::LogicalValue(top_item.array->GetView(top_item.index));
        if (comparator(xval, top_value)) {
          heap.pop();
          heap.push(HeapItem{x_index, offset, &arr});
        }
      }
      offset += chunk->length();
    }

    int64_t out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices, MakeMutableUInt64Array(uint64(), out_size,
                                                                    ctx_->memory_pool()));
    auto* out_cbegin = take_indices->GetMutableValues<uint64_t>(1) + out_size - 1;
    while (heap.size() > 0) {
      auto top_item = heap.top();
      *out_cbegin = top_item.index + top_item.offset;
      heap.pop();
      --out_cbegin;
    }
    *output_ = Datum(take_indices);
    return Status::OK();
  }

  const ChunkedArray& chunked_array_;
  const std::shared_ptr<DataType> physical_type_;
  const ArrayVector physical_chunks_;
  int64_t k_;
  SortOrder order_;
  ExecContext* ctx_;
  Datum* output_;
};

class RecordBatchSelecter : public TypeVisitor {
 private:
  using ResolvedSortKey = MultipleKeyRecordBatchSorter::ResolvedSortKey;
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  RecordBatchSelecter(ExecContext* ctx, const RecordBatch& record_batch,
                      const SelectKOptions& options, Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        record_batch_(record_batch),
        k_(options.k),
        output_(output),
        sort_keys_(ResolveSortKeys(record_batch, options.sort_keys)),
        comparator_(sort_keys_, NullPlacement::AtEnd) {}

  Status Run() { return sort_keys_[0].type->Accept(this); }

 protected:
#define VISIT(TYPE)                                            \
  Status Visit(const TYPE& type) {                             \
    if (sort_keys_[0].order == SortOrder::Descending)          \
      return SelectKthInternal<TYPE, SortOrder::Descending>(); \
    return SelectKthInternal<TYPE, SortOrder::Ascending>();    \
  }
  VISIT_PHYSICAL_TYPES(VISIT)
#undef VISIT

  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto array = batch.GetColumnByName(key.name);
      resolved.emplace_back(array, key.order);
    }
    return resolved;
  }

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];
    const ArrayType& arr = checked_cast<const ArrayType&>(first_sort_key.array);

    const auto num_rows = record_batch_.num_rows();
    if (num_rows == 0) {
      return Status::OK();
    }
    if (k_ > record_batch_.num_rows()) {
      k_ = record_batch_.num_rows();
    }
    std::function<bool(const uint64_t&, const uint64_t&)> cmp;
    SelectKComparator<sort_order> select_k_comparator;
    cmp = [&](const uint64_t& left, const uint64_t& right) -> bool {
      const auto lval = GetView::LogicalValue(arr.GetView(left));
      const auto rval = GetView::LogicalValue(arr.GetView(right));
      if (lval == rval) {
        // If the left value equals to the right value,
        // we need to compare the second and following
        // sort keys.
        return comparator.Compare(left, right, 1);
      }
      return select_k_comparator(lval, rval);
    };
    using HeapContainer =
        std::priority_queue<uint64_t, std::vector<uint64_t>, decltype(cmp)>;

    std::vector<uint64_t> indices(arr.length());
    uint64_t* indices_begin = indices.data();
    uint64_t* indices_end = indices_begin + indices.size();
    std::iota(indices_begin, indices_end, 0);

    const auto p = PartitionNulls<ArrayType, NonStablePartitioner>(
        indices_begin, indices_end, arr, 0, NullPlacement::AtEnd);
    const auto end_iter = p.non_nulls_end;

    auto kth_begin = std::min(indices_begin + k_, end_iter);

    HeapContainer heap(indices_begin, kth_begin, cmp);
    for (auto iter = kth_begin; iter != end_iter && !heap.empty(); ++iter) {
      uint64_t x_index = *iter;
      auto top_item = heap.top();
      if (cmp(x_index, top_item)) {
        heap.pop();
        heap.push(x_index);
      }
    }
    int64_t out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices, MakeMutableUInt64Array(uint64(), out_size,
                                                                    ctx_->memory_pool()));
    auto* out_cbegin = take_indices->GetMutableValues<uint64_t>(1) + out_size - 1;
    while (heap.size() > 0) {
      *out_cbegin = heap.top();
      heap.pop();
      --out_cbegin;
    }
    *output_ = Datum(take_indices);
    return Status::OK();
  }

  ExecContext* ctx_;
  const RecordBatch& record_batch_;
  int64_t k_;
  Datum* output_;
  std::vector<ResolvedSortKey> sort_keys_;
  Comparator comparator_;
};

class TableSelecter : public TypeVisitor {
 private:
  struct ResolvedSortKey {
    ResolvedSortKey(const std::shared_ptr<ChunkedArray>& chunked_array,
                    const SortOrder order)
        : order(order),
          type(GetPhysicalType(chunked_array->type())),
          chunks(GetPhysicalChunks(*chunked_array, type)),
          chunk_pointers(GetArrayPointers(chunks)),
          null_count(chunked_array->null_count()),
          resolver(chunk_pointers) {}

    using LocationType = int64_t;

    // Find the target chunk and index in the target chunk from an
    // index in chunked array.
    template <typename ArrayType>
    ResolvedChunk<ArrayType> GetChunk(int64_t index) const {
      return resolver.Resolve<ArrayType>(index);
    }

    const SortOrder order;
    const std::shared_ptr<DataType> type;
    const ArrayVector chunks;
    const std::vector<const Array*> chunk_pointers;
    const int64_t null_count;
    const ChunkedArrayResolver resolver;
  };
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  TableSelecter(ExecContext* ctx, const Table& table, const SelectKOptions& options,
                Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        table_(table),
        k_(options.k),
        output_(output),
        sort_keys_(ResolveSortKeys(table, options.sort_keys)),
        comparator_(sort_keys_, NullPlacement::AtEnd) {}

  Status Run() { return sort_keys_[0].type->Accept(this); }

 protected:
#define VISIT(TYPE)                                            \
  Status Visit(const TYPE& type) {                             \
    if (sort_keys_[0].order == SortOrder::Descending)          \
      return SelectKthInternal<TYPE, SortOrder::Descending>(); \
    return SelectKthInternal<TYPE, SortOrder::Ascending>();    \
  }
  VISIT_PHYSICAL_TYPES(VISIT)

#undef VISIT

  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const Table& table, const std::vector<SortKey>& sort_keys) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto chunked_array = table.GetColumnByName(key.name);
      resolved.emplace_back(chunked_array, key.order);
    }
    return resolved;
  }

  // Behaves like PartitionNulls() but this supports multiple sort keys.
  template <typename Type>
  NullPartitionResult PartitionNullsInternal(uint64_t* indices_begin,
                                             uint64_t* indices_end,
                                             const ResolvedSortKey& first_sort_key) {
    using ArrayType = typename TypeTraits<Type>::ArrayType;

    const auto p = PartitionNullsOnly<StablePartitioner>(
        indices_begin, indices_end, first_sort_key.resolver, first_sort_key.null_count,
        NullPlacement::AtEnd);
    DCHECK_EQ(p.nulls_end - p.nulls_begin, first_sort_key.null_count);

    const auto q = PartitionNullLikes<ArrayType, StablePartitioner>(
        p.non_nulls_begin, p.non_nulls_end, first_sort_key.resolver,
        NullPlacement::AtEnd);

    auto& comparator = comparator_;
    // Sort all NaNs by the second and following sort keys.
    std::stable_sort(q.nulls_begin, q.nulls_end, [&](uint64_t left, uint64_t right) {
      return comparator.Compare(left, right, 1);
    });
    // Sort all nulls by the second and following sort keys.
    std::stable_sort(p.nulls_begin, p.nulls_end, [&](uint64_t left, uint64_t right) {
      return comparator.Compare(left, right, 1);
    });

    return q;
  }

  // XXX this implementation is rather inefficient as it computes chunk indices
  // at every comparison.  Instead we should iterate over individual batches
  // and remember ChunkLocation entries in the max-heap.

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];

    const auto num_rows = table_.num_rows();
    if (num_rows == 0) {
      return Status::OK();
    }
    if (k_ > table_.num_rows()) {
      k_ = table_.num_rows();
    }
    std::function<bool(const uint64_t&, const uint64_t&)> cmp;
    SelectKComparator<sort_order> select_k_comparator;
    cmp = [&](const uint64_t& left, const uint64_t& right) -> bool {
      auto chunk_left = first_sort_key.template GetChunk<ArrayType>(left);
      auto chunk_right = first_sort_key.template GetChunk<ArrayType>(right);
      auto value_left = chunk_left.Value();
      auto value_right = chunk_right.Value();
      if (value_left == value_right) {
        return comparator.Compare(left, right, 1);
      }
      return select_k_comparator(value_left, value_right);
    };
    using HeapContainer =
        std::priority_queue<uint64_t, std::vector<uint64_t>, decltype(cmp)>;

    std::vector<uint64_t> indices(num_rows);
    uint64_t* indices_begin = indices.data();
    uint64_t* indices_end = indices_begin + indices.size();
    std::iota(indices_begin, indices_end, 0);

    const auto p =
        this->PartitionNullsInternal<InType>(indices_begin, indices_end, first_sort_key);
    const auto end_iter = p.non_nulls_end;
    auto kth_begin = std::min(indices_begin + k_, end_iter);

    HeapContainer heap(indices_begin, kth_begin, cmp);
    for (auto iter = kth_begin; iter != end_iter && !heap.empty(); ++iter) {
      uint64_t x_index = *iter;
      uint64_t top_item = heap.top();
      if (cmp(x_index, top_item)) {
        heap.pop();
        heap.push(x_index);
      }
    }
    int64_t out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices, MakeMutableUInt64Array(uint64(), out_size,
                                                                    ctx_->memory_pool()));
    auto* out_cbegin = take_indices->GetMutableValues<uint64_t>(1) + out_size - 1;
    while (heap.size() > 0) {
      *out_cbegin = heap.top();
      heap.pop();
      --out_cbegin;
    }
    *output_ = Datum(take_indices);
    return Status::OK();
  }

  ExecContext* ctx_;
  const Table& table_;
  int64_t k_;
  Datum* output_;
  std::vector<ResolvedSortKey> sort_keys_;
  Comparator comparator_;
};

static Status CheckConsistency(const Schema& schema,
                               const std::vector<SortKey>& sort_keys) {
  for (const auto& key : sort_keys) {
    auto field = schema.GetFieldByName(key.name);
    if (!field) {
      return Status::Invalid("Nonexistent sort key column: ", key.name);
    }
  }
  return Status::OK();
}

class SelectKUnstableMetaFunction : public MetaFunction {
 public:
  SelectKUnstableMetaFunction()
      : MetaFunction("select_k_unstable", Arity::Unary(), &select_k_unstable_doc,
                     &kDefaultSelectKOptions) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options, ExecContext* ctx) const {
    const SelectKOptions& select_k_options = static_cast<const SelectKOptions&>(*options);
    if (select_k_options.k < 0) {
      return Status::Invalid("select_k_unstable requires a nonnegative `k`, got ",
                             select_k_options.k);
    }
    if (select_k_options.sort_keys.size() == 0) {
      return Status::Invalid("select_k_unstable requires a non-empty `sort_keys`");
    }
    switch (args[0].kind()) {
      case Datum::ARRAY: {
        return SelectKth(*args[0].make_array(), select_k_options, ctx);
      } break;
      case Datum::CHUNKED_ARRAY: {
        return SelectKth(*args[0].chunked_array(), select_k_options, ctx);
      } break;
      case Datum::RECORD_BATCH:
        return SelectKth(*args[0].record_batch(), select_k_options, ctx);
        break;
      case Datum::TABLE:
        return SelectKth(*args[0].table(), select_k_options, ctx);
        break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for select_k operation: "
        "values=",
        args[0].ToString());
  }

 private:
  Result<Datum> SelectKth(const Array& array, const SelectKOptions& options,
                          ExecContext* ctx) const {
    Datum output;
    ArraySelecter selecter(ctx, array, options, &output);
    ARROW_RETURN_NOT_OK(selecter.Run());
    return output;
  }

  Result<Datum> SelectKth(const ChunkedArray& chunked_array,
                          const SelectKOptions& options, ExecContext* ctx) const {
    Datum output;
    ChunkedArraySelecter selecter(ctx, chunked_array, options, &output);
    ARROW_RETURN_NOT_OK(selecter.Run());
    return output;
  }
  Result<Datum> SelectKth(const RecordBatch& record_batch, const SelectKOptions& options,
                          ExecContext* ctx) const {
    ARROW_RETURN_NOT_OK(CheckConsistency(*record_batch.schema(), options.sort_keys));
    Datum output;
    RecordBatchSelecter selecter(ctx, record_batch, options, &output);
    ARROW_RETURN_NOT_OK(selecter.Run());
    return output;
  }
  Result<Datum> SelectKth(const Table& table, const SelectKOptions& options,
                          ExecContext* ctx) const {
    ARROW_RETURN_NOT_OK(CheckConsistency(*table.schema(), options.sort_keys));
    Datum output;
    TableSelecter selecter(ctx, table, options, &output);
    ARROW_RETURN_NOT_OK(selecter.Run());
    return output;
  }
};

// array documentation
const auto kDefaultArraySortOptions = ArraySortOptions::Defaults();

const FunctionDoc array_sort_indices_doc(
    "Return the indices that would sort an array",
    ("This function computes an array of indices that define a stable sort\n"
     "of the input array.  By default, Null values are considered greater\n"
     "than any other value and are therefore sorted at the end of the array.\n"
     "For floating-point types, NaNs are considered greater than any\n"
     "other non-null value, but smaller than null values.\n"
     "\n"
     "The handling of nulls and NaNs can be changed in ArraySortOptions."),
    {"array"}, "ArraySortOptions");

const FunctionDoc partition_nth_indices_doc(
    "Return the indices that would partition an array around a pivot",
    ("This functions computes an array of indices that define a non-stable\n"
     "partial sort of the input array.\n"
     "\n"
     "The output is such that the `N`'th index points to the `N`'th element\n"
     "of the input in sorted order, and all indices before the `N`'th point\n"
     "to elements in the input less or equal to elements at or after the `N`'th.\n"
     "\n"
     "By default, null values are considered greater than any other value\n"
     "and are therefore partitioned towards the end of the array.\n"
     "For floating-point types, NaNs are considered greater than any\n"
     "other non-null value, but smaller than null values.\n"
     "\n"
     "The pivot index `N` must be given in PartitionNthOptions.\n"
     "The handling of nulls and NaNs can also be changed in PartitionNthOptions."),
    {"array"}, "PartitionNthOptions");

}  // namespace

void RegisterVectorSort(FunctionRegistry* registry) {
  // The kernel outputs into preallocated memory and is never null
  VectorKernel base;
  base.mem_allocation = MemAllocation::PREALLOCATE;
  base.null_handling = NullHandling::OUTPUT_NOT_NULL;

  auto array_sort_indices = std::make_shared<VectorFunction>(
      "array_sort_indices", Arity::Unary(), &array_sort_indices_doc,
      &kDefaultArraySortOptions);
  base.init = ArraySortIndicesState::Init;
  AddSortingKernels<ArraySortIndices>(base, array_sort_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(array_sort_indices)));

  DCHECK_OK(registry->AddFunction(std::make_shared<SortIndicesMetaFunction>()));

  // partition_nth_indices has a parameter so needs its init function
  auto part_indices = std::make_shared<VectorFunction>(
      "partition_nth_indices", Arity::Unary(), &partition_nth_indices_doc);
  base.init = PartitionNthToIndicesState::Init;
  AddSortingKernels<PartitionNthToIndices>(base, part_indices.get());
  DCHECK_OK(registry->AddFunction(std::move(part_indices)));

  // select_k_unstable
  DCHECK_OK(registry->AddFunction(std::make_shared<SelectKUnstableMetaFunction>()));
}

#undef VISIT_PHYSICAL_TYPES

}  // namespace internal
}  // namespace compute
}  // namespace arrow
