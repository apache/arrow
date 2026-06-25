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
#include <span>

#include "arrow/array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/chunked_internal.h"
#include "arrow/compute/ordering.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/logging_internal.h"

namespace arrow::compute::internal {

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
  auto operator()(std::span<uint64_t> indices, Predicate&& pred) {
    return std::ranges::partition(indices, std::forward<Predicate>(pred));
  }
};

struct StablePartitioner {
  template <typename Predicate>
  auto operator()(std::span<uint64_t> indices, Predicate&& pred) {
    return std::ranges::stable_partition(indices, std::forward<Predicate>(pred));
  }
};

template <typename TypeClass>
constexpr bool has_null_like_values() {
  return is_physical_floating(TypeClass::type_id);
}

// Compare two values, taking NaNs into account

template <typename Type, typename Value>
int CompareTypeValues(Value&& left, Value&& right, SortOrder order,
                      NullPlacement null_placement) {
  if constexpr (has_null_like_values<Type>()) {
    const bool is_nan_left = std::isnan(left);
    const bool is_nan_right = std::isnan(right);
    if (is_nan_left && is_nan_right) {
      return 0;
    } else if (is_nan_left) {
      return null_placement == NullPlacement::AtStart ? -1 : 1;
    } else if (is_nan_right) {
      return null_placement == NullPlacement::AtStart ? 1 : -1;
    }
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

template <typename IndexType>
struct GenericNullLikePartition {
  std::span<IndexType> non_null_like_range;
  std::span<IndexType> nan_range;
  std::span<IndexType> null_range;

  IndexType* overall_begin() const {
    return std::min(non_null_like_range.data(), null_range.data());
  }

  IndexType* overall_end() const {
    return std::max(non_null_like_range.data() + non_null_like_range.size(),
                    null_range.data() + null_range.size());
  }

  template <typename TargetIndexType>
  GenericNullLikePartition<TargetIndexType> TranslateTo(
      IndexType* indices_begin, TargetIndexType* target_indices_begin) const {
    return {.non_null_like_range = {(non_null_like_range.data() - indices_begin) +
                                        target_indices_begin,
                                    non_null_like_range.size()},
            .nan_range = {(nan_range.data() - indices_begin) + target_indices_begin,
                          nan_range.size()},
            .null_range = {(null_range.data() - indices_begin) + target_indices_begin,
                           null_range.size()}};
  }

  static GenericNullLikePartition fromCounts(std::span<IndexType> indices,
                                                         int64_t non_null_like_count,
                                                         int64_t nan_count,
                                                         int64_t null_count,
                                                         NullPlacement null_placement) {
    GenericNullLikePartition p;
    DCHECK_EQ(non_null_like_count + nan_count + null_count,
              static_cast<int64_t>(indices.size()));
    if (null_placement == NullPlacement::AtEnd) {
      p.non_null_like_range = indices.subspan(0, non_null_like_count);
      p.nan_range = indices.subspan(non_null_like_count, nan_count);
      p.null_range = indices.subspan(non_null_like_count + nan_count, null_count);
    } else {
      p.null_range = indices.subspan(0, null_count);
      p.nan_range = indices.subspan(null_count, nan_count);
      p.non_null_like_range =
          indices.subspan(null_count + nan_count, non_null_like_count);
    }
    return p;
  }
};

using NullLikePartition = GenericNullLikePartition<uint64_t>;
using ChunkedNullLikePartition =
    GenericNullLikePartition<CompressedChunkLocation>;

struct NullPartition {
  std::span<uint64_t> non_nulls;
  std::span<uint64_t> nulls;

  static NullPartition NoNulls(std::span<uint64_t> indices,
                               NullPlacement null_placement) {
    if (null_placement == NullPlacement::AtStart) {
      return {.non_nulls = indices, .nulls = indices.subspan(0, 0)};
    } else {
      return {.non_nulls = indices, .nulls = indices.subspan(indices.size(), 0)};
    }
  }

  static NullPartition NullsAtEnd(std::span<uint64_t> indices,
                                  std::span<uint64_t> null_tail) {
    ARROW_DCHECK_GE(null_tail.begin(), indices.begin());
    ARROW_DCHECK_LE(null_tail.begin(), indices.end());
    return {.non_nulls = {indices.begin(), null_tail.begin()}, .nulls = null_tail};
  }

  static NullPartition NullsAtStart(std::span<uint64_t> indices,
                                    std::span<uint64_t> non_null_tail) {
    ARROW_DCHECK_GE(non_null_tail.begin(), indices.begin());
    ARROW_DCHECK_LE(non_null_tail.begin(), indices.end());
    return {.non_nulls = non_null_tail,
            .nulls = {indices.begin(), non_null_tail.begin()}};
  }
};

// Move nulls (not null-like values) to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename Partitioner>
NullPartition PartitionNullsOnly(std::span<uint64_t> indices, const Array& values,
                                 int64_t offset, NullPlacement null_placement) {
  if (values.null_count() == 0) {
    return NullPartition::NoNulls(indices, null_placement);
  }
  Partitioner partitioner;
  if (null_placement == NullPlacement::AtStart) {
    auto non_null_tail = partitioner(indices, [&values, &offset](uint64_t ind) {
      return values.IsNull(ind - offset);
    });
    return NullPartition::NullsAtStart(indices, non_null_tail);
  } else {
    auto null_tail = partitioner(indices, [&values, &offset](uint64_t ind) {
      return !values.IsNull(ind - offset);
    });
    return NullPartition::NullsAtEnd(indices, null_tail);
  }
}

struct NanPartition {
  std::span<uint64_t> non_null_like_range;
  std::span<uint64_t> nan_range;
};

// Move non-null null-like values to end of array.
//
// `offset` is used when this is called on a chunk of a chunked array
template <typename ArrayType, typename Partitioner>
NanPartition PartitionNans(std::span<uint64_t> indices, const ArrayType& values,
                           int64_t offset, NullPlacement null_placement) {
  if constexpr (has_null_like_values<typename ArrayType::TypeClass>()) {
    Partitioner partitioner;
    if (null_placement == NullPlacement::AtStart) {
      auto non_null_like_tail = partitioner(indices, [&values, &offset](uint64_t ind) {
        return std::isnan(values.GetView(ind - offset));
      });
      return NanPartition{.non_null_like_range = non_null_like_tail,
                          .nan_range = {indices.data(), non_null_like_tail.data()}};
    } else {
      auto nan_tail = partitioner(indices, [&values, &offset](uint64_t ind) {
        return !std::isnan(values.GetView(ind - offset));
      });
      return NanPartition{.non_null_like_range = {indices.data(), nan_tail.data()},
                          .nan_range = nan_tail};
    }
  } else {
    if (null_placement == NullPlacement::AtStart) {
      return NanPartition{.non_null_like_range = indices,
                          .nan_range = {indices.data(), indices.data()}};
    } else {
      return NanPartition{.non_null_like_range = indices,
                          .nan_range = indices.subspan(indices.size())};
    }
  }
}

template <typename ArrayType, typename Partitioner>
NullLikePartition PartitionNullsAndNans(std::span<uint64_t> indices,
                                                    const ArrayType& values,
                                                    int64_t offset,
                                                    NullPlacement null_placement) {
  // Partition nulls at start (resp. end), and null-like values just before (resp. after)
  NullPartition p =
      PartitionNullsOnly<Partitioner>(indices, values, offset, null_placement);
  auto q =
      PartitionNans<ArrayType, Partitioner>(p.non_nulls, values, offset, null_placement);
  return NullLikePartition{.non_null_like_range = q.non_null_like_range,
                                       .nan_range = q.nan_range,
                                       .null_range = p.nulls};
}

template <typename ArrayType, typename Partitioner>
NullLikePartition PartitionNansOnly(std::span<uint64_t> indices,
                                                const ArrayType& values, int64_t offset,
                                                NullPlacement null_placement) {
  // Partition nulls at start (resp. end), and null-like values just before (resp. after)
  NullPartition p = NullPartition::NoNulls(indices, null_placement);
  auto q =
      PartitionNans<ArrayType, Partitioner>(p.non_nulls, values, offset, null_placement);
  return NullLikePartition{.non_null_like_range = q.non_null_like_range,
                                       .nan_range = q.nan_range,
                                       .null_range = p.nulls};
}

struct ChunkedMergeImpl {
  using MergeNonNullsFunc = std::function<void(
      CompressedChunkLocation* range_begin, CompressedChunkLocation* range_middle,
      CompressedChunkLocation* range_end, CompressedChunkLocation* temp_indices)>;

  ChunkedMergeImpl(NullPlacement null_placement, MergeNonNullsFunc&& merge_non_nulls)
      : null_placement_(null_placement), merge_non_nulls_(std::move(merge_non_nulls)) {}

  Status Init(ExecContext* ctx, int64_t temp_indices_length) {
    ARROW_ASSIGN_OR_RAISE(temp_buffer_, AllocateBuffer(sizeof(CompressedChunkLocation) *
                                                           temp_indices_length,
                                                       ctx->memory_pool()));
    temp_indices_ =
        reinterpret_cast<CompressedChunkLocation*>(temp_buffer_->mutable_data());
    return Status::OK();
  }

  ChunkedNullLikePartition Merge(
      const ChunkedNullLikePartition& left,
      const ChunkedNullLikePartition& right, int64_t null_count) const {
    if (null_placement_ == NullPlacement::AtStart) {
      return MergeNullsAtStart(left, right, null_count);
    } else {
      return MergeNullsAtEnd(left, right, null_count);
    }
  }

  ChunkedNullLikePartition MergeNullsAtStart(
      const ChunkedNullLikePartition& left,
      const ChunkedNullLikePartition& right, int64_t null_count) const {
    // Input layout:
    // [left nul .. left nan .. left non-nul .. right nul .. right nan .. right non-nul]
    ARROW_DCHECK_EQ(left.null_range.end(), left.nan_range.begin());
    ARROW_DCHECK_EQ(left.nan_range.end(), left.non_null_like_range.begin());
    ARROW_DCHECK_EQ(left.non_null_like_range.end(), right.null_range.begin());
    ARROW_DCHECK_EQ(right.null_range.end(), right.nan_range.begin());
    ARROW_DCHECK_EQ(right.nan_range.end(), right.non_null_like_range.begin());

    // Mutate the input, stably in two steps, to obtain the following layouts:
    // [left nul .. left nan .. right nul .. right nan .. left non-nul .. right non-nus]
    std::rotate(left.non_null_like_range.begin(), right.null_range.begin(),
                right.nan_range.end());

    // only use sizes of ranges that are at a different position now
    // [left nul .. right nul .. left nan .. right nan .. left non-nulls .. right
    // non-nulls] this is a no-op if no nan values are present
    std::rotate(left.nan_range.begin(), left.nan_range.begin() + left.nan_range.size(),
                left.nan_range.begin() + left.nan_range.size() + right.null_range.size());

    std::span<CompressedChunkLocation> full_span{left.overall_begin(),
                                                 right.overall_end()};
    const auto p = ChunkedNullLikePartition::fromCounts(
        full_span, left.non_null_like_range.size() + right.non_null_like_range.size(),
        left.nan_range.size() + right.nan_range.size(),
        left.null_range.size() + right.null_range.size(), NullPlacement::AtStart);

    if (!p.non_null_like_range.empty()) {
      merge_non_nulls_(p.non_null_like_range.data(),
                       p.non_null_like_range.data() + left.non_null_like_range.size(),
                       p.non_null_like_range.data() + p.non_null_like_range.size(),
                       temp_indices_);
    }
    return p;
  }

  ChunkedNullLikePartition MergeNullsAtEnd(
      const ChunkedNullLikePartition& left,
      const ChunkedNullLikePartition& right, int64_t null_count) const {
    // Input layout:
    // [left non-nul .. left nan .. left nul .. right non-nul .. right nan .. right nulls]
    ARROW_DCHECK_EQ(left.non_null_like_range.end(), left.nan_range.begin());
    ARROW_DCHECK_EQ(left.nan_range.end(), left.null_range.begin());
    ARROW_DCHECK_EQ(left.null_range.end(), right.non_null_like_range.begin());
    ARROW_DCHECK_EQ(right.non_null_like_range.end(), right.nan_range.begin());
    ARROW_DCHECK_EQ(right.nan_range.end(), right.null_range.begin());

    // Mutate the input, stably in two steps, to obtain the following layouts:
    // [left non-nul .. right non-nul .. left nan .. left nul .. right nan .. right nul]
    std::rotate(left.nan_range.begin(), right.non_null_like_range.begin(),
                right.non_null_like_range.end());

    // only use sizes of ranges that are at a different position now
    // [left non-nul .. right non-nul .. left nan .. left nul .. right nan .. right nul]
    // this is a no-op if no nan values are present
    auto new_left_null_range_begin =
        left.non_null_like_range.begin() + left.non_null_like_range.size() +
        right.non_null_like_range.size() + left.nan_range.size();
    std::rotate(
        new_left_null_range_begin, new_left_null_range_begin + left.null_range.size(),
        new_left_null_range_begin + left.null_range.size() + right.nan_range.size());

    std::span<CompressedChunkLocation> full_span{left.overall_begin(),
                                                 right.overall_end()};
    const auto p = ChunkedNullLikePartition::fromCounts(
        full_span, left.non_null_like_range.size() + right.non_null_like_range.size(),
        left.nan_range.size() + right.nan_range.size(),
        left.null_range.size() + right.null_range.size(), NullPlacement::AtEnd);

    // Merge the non-null values into temp area
    if (!p.non_null_like_range.empty()) {
      merge_non_nulls_(p.non_null_like_range.data(),
                       p.non_null_like_range.data() + left.non_null_like_range.size(),
                       p.non_null_like_range.data() + p.non_null_like_range.size(),
                       temp_indices_);
    }
    return p;
  }

 private:
  NullPlacement null_placement_;
  MergeNonNullsFunc merge_non_nulls_;
  std::unique_ptr<Buffer> temp_buffer_;
  CompressedChunkLocation* temp_indices_ = nullptr;
};

// TODO make this usable if indices are non trivial on input
// (see ConcreteRecordBatchColumnSorter)
// `offset` is used when this is called on a chunk of a chunked array
using ArraySortFunc = std::function<Result<NullLikePartition>(
    std::span<uint64_t> indices, const Array& values, int64_t offset,
    const ArraySortOptions& options, ExecContext* ctx)>;

Result<ArraySortFunc> GetArraySorter(const DataType& type);

Result<NullLikePartition> SortChunkedArray(ExecContext* ctx,
                                                       std::span<uint64_t> indices,
                                                       const ChunkedArray& chunked_array,
                                                       SortOrder sort_order,
                                                       NullPlacement null_placement);

Result<NullLikePartition> SortChunkedArray(
    ExecContext* ctx, std::span<uint64_t> indices,
    const std::shared_ptr<DataType>& physical_type, const ArrayVector& physical_chunks,
    SortOrder sort_order, NullPlacement null_placement);

Result<NullLikePartition> SortStructArray(ExecContext* ctx,
                                                      std::span<uint64_t> indices,
                                                      const StructArray& array,
                                                      SortOrder sort_order,
                                                      NullPlacement null_placement);

// ----------------------------------------------------------------------
// Helpers for Sort/SelectK/Rank implementations

struct SortField {
  SortField() = default;
  SortField(FieldPath path, SortOrder order, NullPlacement null_placement,
            const DataType* type)
      : path(std::move(path)), order(order), null_placement(null_placement), type(type) {}
  SortField(int index, SortOrder order, NullPlacement null_placement,
            const DataType* type)
      : SortField(FieldPath({index}), order, null_placement, type) {}

  bool is_nested() const { return path.indices().size() > 1; }

  FieldPath path;
  SortOrder order;
  NullPlacement null_placement;
  const DataType* type;
};

inline Status CheckNonNested(const FieldRef& ref) {
  if (ref.IsNested()) {
    return Status::KeyError("Nested keys not supported for SortKeys");
  }
  return Status::OK();
}

template <typename T>
Result<T> PrependInvalidColumn(Result<T> res) {
  if (res.ok()) return res;
  return res.status().WithMessage("Invalid sort key column: ", res.status().message());
}

// Return the field indices of the sort keys, deduplicating them along the way
Result<std::vector<SortField>> FindSortKeys(const Schema& schema,
                                            const std::vector<SortKey>& sort_keys);

template <typename ResolvedSortKey, typename ResolvedSortKeyFactory>
Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
    const Schema& schema, const std::vector<SortKey>& sort_keys,
    ResolvedSortKeyFactory&& factory) {
  ARROW_ASSIGN_OR_RAISE(const auto fields, FindSortKeys(schema, sort_keys));
  std::vector<ResolvedSortKey> resolved;
  resolved.reserve(fields.size());
  for (const auto& f : fields) {
    ARROW_ASSIGN_OR_RAISE(auto resolved_key, factory(f));
    resolved.push_back(std::move(resolved_key));
  }
  return resolved;
}

template <typename ResolvedSortKey, typename TableOrBatch>
Result<std::vector<ResolvedSortKey>> ResolveSortKeys(
    const TableOrBatch& table_or_batch, const std::vector<SortKey>& sort_keys) {
  return ResolveSortKeys<ResolvedSortKey>(
      *table_or_batch.schema(), sort_keys,
      [&](const SortField& f) -> Result<ResolvedSortKey> {
        if (f.is_nested()) {
          // TODO: Some room for improvement here, as we potentially duplicate some of the
          // null-flattening work for nested sort keys. For instance, given two keys with
          // paths [0,0,0,0] and [0,0,0,1], we shouldn't need to flatten the first three
          // components more than once.
          ARROW_ASSIGN_OR_RAISE(auto child, f.path.GetFlattened(table_or_batch));
          return ResolvedSortKey{std::move(child), f.order, f.null_placement};
        }
        return ResolvedSortKey{table_or_batch.column(f.path[0]), f.order,
                               f.null_placement};
      });
}

// // Returns an error status if no column matching `ref` is found, or if the FieldRef is
// // a nested reference.
inline Result<std::shared_ptr<ChunkedArray>> GetColumn(const Table& table,
                                                       const FieldRef& ref) {
  RETURN_NOT_OK(CheckNonNested(ref));
  ARROW_ASSIGN_OR_RAISE(auto path, ref.FindOne(*table.schema()));
  return table.column(path[0]);
}

inline Result<std::shared_ptr<Array>> GetColumn(const RecordBatch& batch,
                                                const FieldRef& ref) {
  RETURN_NOT_OK(CheckNonNested(ref));
  return ref.GetOne(batch);
}

// We could try to reproduce the concrete Array classes' facilities
// (such as cached raw values pointer) in a separate hierarchy of
// physical accessors, but doing so ends up too cumbersome.
// Instead, we simply create the desired concrete Array objects.
inline std::shared_ptr<Array> GetPhysicalArray(
    const Array& array, const std::shared_ptr<DataType>& physical_type) {
  auto new_data = array.data()->Copy();
  new_data->type = physical_type;
  return MakeArray(std::move(new_data));
}

inline ArrayVector GetPhysicalChunks(const ArrayVector& chunks,
                                     const std::shared_ptr<DataType>& physical_type) {
  ArrayVector physical(chunks.size());
  std::transform(chunks.begin(), chunks.end(), physical.begin(),
                 [&](const std::shared_ptr<Array>& array) {
                   return GetPhysicalArray(*array, physical_type);
                 });
  return physical;
}

inline ArrayVector GetPhysicalChunks(const ChunkedArray& chunked_array,
                                     const std::shared_ptr<DataType>& physical_type) {
  return GetPhysicalChunks(chunked_array.chunks(), physical_type);
}

// Compare two records in a single column (either from a batch or table)
template <typename ResolvedSortKey>
struct ColumnComparator {
  using Location = typename ResolvedSortKey::LocationType;

  explicit ColumnComparator(const ResolvedSortKey& sort_key) : sort_key_(sort_key) {}

  virtual ~ColumnComparator() = default;

  virtual int Compare(const Location& left, const Location& right) const = 0;

  ResolvedSortKey sort_key_;
};

template <typename ResolvedSortKey, typename Type>
struct ConcreteColumnComparator : public ColumnComparator<ResolvedSortKey> {
  using Location = typename ResolvedSortKey::LocationType;

  using ColumnComparator<ResolvedSortKey>::ColumnComparator;

  int Compare(const Location& left, const Location& right) const override {
    const auto& sort_key = this->sort_key_;

    const auto chunk_left = sort_key.GetChunk(left);
    const auto chunk_right = sort_key.GetChunk(right);
    if (sort_key.null_count > 0) {
      const bool is_null_left = chunk_left.IsNull();
      const bool is_null_right = chunk_right.IsNull();
      if (is_null_left && is_null_right) {
        return 0;
      } else if (is_null_left) {
        return sort_key.null_placement == NullPlacement::AtStart ? -1 : 1;
      } else if (is_null_right) {
        return sort_key.null_placement == NullPlacement::AtStart ? 1 : -1;
      }
    }
    return CompareTypeValues<Type>(chunk_left.template Value<Type>(),
                                   chunk_right.template Value<Type>(), sort_key.order,
                                   sort_key.null_placement);
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

  explicit MultipleKeyComparator(const std::vector<ResolvedSortKey>& sort_keys)
      : sort_keys_(sort_keys) {
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

    VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
    VISIT(NullType)

#undef VISIT

    Status Visit(const DataType& type) {
      return Status::TypeError("Unsupported type for batch or table sorting: ",
                               type.ToString());
    }

    template <typename Type>
    Status VisitGeneric(const Type& type) {
      res.reset(new ConcreteColumnComparator<ResolvedSortKey, Type>{sort_key});
      return Status::OK();
    }

    const ResolvedSortKey& sort_key;
    std::unique_ptr<ColumnComparator<ResolvedSortKey>> res;
  };

  Status MakeComparators() {
    column_comparators_.reserve(sort_keys_.size());

    for (const auto& sort_key : sort_keys_) {
      ColumnComparatorFactory factory{sort_key, nullptr};
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
  std::vector<std::unique_ptr<ColumnComparator<ResolvedSortKey>>> column_comparators_;
  Status status_;
};

struct ResolvedRecordBatchSortKey {
  ResolvedRecordBatchSortKey(const std::shared_ptr<Array>& array, SortOrder order,
                             NullPlacement null_placement)
      : type(GetPhysicalType(array->type())),
        owned_array(GetPhysicalArray(*array, type)),
        array(*owned_array),
        order(order),
        null_placement(null_placement),
        null_count(array->null_count()) {}

  using LocationType = int64_t;

  ResolvedChunk GetChunk(int64_t index) const { return {&array, index}; }

  const std::shared_ptr<DataType> type;
  std::shared_ptr<Array> owned_array;
  const Array& array;
  SortOrder order;
  NullPlacement null_placement;
  int64_t null_count;
};

struct ResolvedTableSortKey {
  ResolvedTableSortKey(const std::shared_ptr<DataType>& type, ArrayVector chunks,
                       SortOrder order, NullPlacement null_placement, int64_t null_count)
      : type(GetPhysicalType(type)),
        owned_chunks(std::move(chunks)),
        chunks(GetArrayPointers(owned_chunks)),
        order(order),
        null_placement(null_placement),
        null_count(null_count) {}

  using LocationType = ::arrow::ChunkLocation;

  ResolvedChunk GetChunk(::arrow::ChunkLocation loc) const {
    return {chunks[loc.chunk_index], loc.index_in_chunk};
  }

  // Make a vector of ResolvedSortKeys for the sort keys and the given table.
  // `batches` must be a chunking of `table`.
  static Result<std::vector<ResolvedTableSortKey>> Make(
      const Table& table, const RecordBatchVector& batches,
      const std::vector<SortKey>& sort_keys) {
    auto factory = [&](const SortField& f) -> Result<ResolvedTableSortKey> {
      // We must expose a homogenous chunking for all ResolvedSortKey,
      // so we can't simply access the column from the table directly.
      ArrayVector chunks;
      chunks.reserve(batches.size());
      int64_t null_count = 0;
      for (const auto& batch : batches) {
        ARROW_ASSIGN_OR_RAISE(auto child, f.path.GetFlattened(*batch));
        null_count += child->null_count();
        chunks.push_back(std::move(child));
      }

      return ResolvedTableSortKey(f.type->GetSharedPtr(), std::move(chunks), f.order,
                                  f.null_placement, null_count);
    };

    return ::arrow::compute::internal::ResolveSortKeys<ResolvedTableSortKey>(
        *table.schema(), sort_keys, factory);
  }

  std::shared_ptr<DataType> type;
  ArrayVector owned_chunks;
  std::vector<const Array*> chunks;
  SortOrder order;
  NullPlacement null_placement;
  int64_t null_count;
};

inline Result<std::shared_ptr<ArrayData>> MakeMutableUInt64Array(
    int64_t length, MemoryPool* memory_pool) {
  auto buffer_size = length * sizeof(uint64_t);
  ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(buffer_size, memory_pool));
  return ArrayData::Make(uint64(), length, {nullptr, std::move(data)}, /*null_count=*/0);
}

inline Result<std::shared_ptr<ArrayData>> MakeMutableFloat64Array(
    int64_t length, MemoryPool* memory_pool) {
  auto buffer_size = length * sizeof(double);
  ARROW_ASSIGN_OR_RAISE(auto data, AllocateBuffer(buffer_size, memory_pool));
  return ArrayData::Make(float64(), length, {nullptr, std::move(data)}, /*null_count=*/0);
}

}  // namespace arrow::compute::internal
