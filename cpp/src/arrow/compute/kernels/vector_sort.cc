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

#include <unordered_set>

#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

Result<RecordBatchVector> BatchesFromTable(const Table& table) {
  return TableBatchReader(table).ToRecordBatches();
}

// ----------------------------------------------------------------------
// ChunkedArray sorting implementation

// Sort a chunked array by sorting each array in the chunked array,
// then merging the sorted chunks recursively.
class ChunkedArraySorter : public TypeVisitor {
 public:
  ChunkedArraySorter(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
                     const std::shared_ptr<DataType>& physical_type,
                     const ArrayVector& physical_chunks, const SortOrder order,
                     const NullPlacement null_placement, NullPartitionResult* output)
      : TypeVisitor(),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        physical_type_(physical_type),
        physical_chunks_(physical_chunks),
        order_(order),
        null_placement_(null_placement),
        ctx_(ctx),
        output_(output) {}

  Status Sort() {
    ARROW_ASSIGN_OR_RAISE(array_sorter_, GetArraySorter(*physical_type_));
    return physical_type_->Accept(this);
  }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) override { return SortInternal<TYPE>(); }

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

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
    const auto num_chunks = static_cast<int>(physical_chunks_.size());
    if (num_chunks == 0) {
      *output_ = {indices_end_, indices_end_, indices_end_, indices_end_};
      return Status::OK();
    }
    const auto arrays = GetArrayPointers(physical_chunks_);

    // Sort each chunk independently and merge to sorted indices.
    // This is a serial implementation.
    std::vector<NullPartitionResult> sorted(num_chunks);

    // First sort all individual chunks
    int64_t begin_offset = 0;
    int64_t end_offset = 0;
    int64_t null_count = 0;
    for (int i = 0; i < num_chunks; ++i) {
      const auto array = checked_cast<const ArrayType*>(arrays[i]);
      end_offset += array->length();
      null_count += array->null_count();
      ARROW_ASSIGN_OR_RAISE(sorted[i], array_sorter_(indices_begin_ + begin_offset,
                                                     indices_begin_ + end_offset, *array,
                                                     begin_offset, options, ctx_));
      begin_offset = end_offset;
    }
    DCHECK_EQ(end_offset, indices_end_ - indices_begin_);

    // Then merge them by pairs, recursively
    if (sorted.size() > 1) {
      auto merge_nulls = [&](uint64_t* nulls_begin, uint64_t* nulls_middle,
                             uint64_t* nulls_end, uint64_t* temp_indices,
                             int64_t null_count) {
        if (has_null_like_values<typename ArrayType::TypeClass>::value) {
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

    *output_ = sorted[0];
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
  const std::shared_ptr<DataType>& physical_type_;
  const ArrayVector& physical_chunks_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  ArraySortFunc array_sorter_;
  ExecContext* ctx_;
  NullPartitionResult* output_;
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

    VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
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

// Sort a batch using a single sort and multiple-key comparisons.
class MultipleKeyRecordBatchSorter : public TypeVisitor {
 private:
  using ResolvedSortKey = ResolvedRecordBatchSortKey;
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

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
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
    const ArrayType& array =
        ::arrow::internal::checked_cast<const ArrayType&>(first_sort_key.array);
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
    const ArrayType& array =
        ::arrow::internal::checked_cast<const ArrayType&>(first_sort_key.array);

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
  // TODO make all methods const and defer initialization into a Init() method?
 private:
  using ResolvedSortKey = ResolvedTableSortKey;
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  TableSorter(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
              const Table& table, const SortOptions& options)
      : ctx_(ctx),
        table_(table),
        batches_(MakeBatches(table, &status_)),
        options_(options),
        null_placement_(options.null_placement),
        left_resolver_(batches_),
        right_resolver_(batches_),
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
    ARROW_ASSIGN_OR_RAISE(RecordBatchVector batches, BatchesFromTable(table_));
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

        VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
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
  enable_if_t<has_null_like_values<Type>::value> MergeNulls(uint64_t* nulls_begin,
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
  enable_if_t<!has_null_like_values<Type>::value> MergeNulls(uint64_t* nulls_begin,
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

  Status status_;
  ExecContext* ctx_;
  const Table& table_;
  const RecordBatchVector batches_;
  const SortOptions& options_;
  const NullPlacement null_placement_;
  const ::arrow::internal::ChunkResolver left_resolver_, right_resolver_;
  const std::vector<ResolvedSortKey> sort_keys_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  Comparator comparator_;
};

// ----------------------------------------------------------------------
// Top-level sort functions

const SortOptions* GetDefaultSortOptions() {
  static const auto kDefaultSortOptions = SortOptions::Defaults();
  return &kDefaultSortOptions;
}

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
      : MetaFunction("sort_indices", Arity::Unary(), sort_indices_doc,
                     GetDefaultSortOptions()) {}

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
    auto buffer_size = bit_util::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(out_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(out_type, length, buffers, 0);
    auto out_begin = out->GetMutableValues<uint64_t>(1);
    auto out_end = out_begin + length;
    std::iota(out_begin, out_end, 0);

    RETURN_NOT_OK(SortChunkedArray(ctx, out_begin, out_end, chunked_array, order,
                                   options.null_placement));
    return Datum(out);
  }

  Result<Datum> SortIndices(const RecordBatch& batch, const SortOptions& options,
                            ExecContext* ctx) const {
    auto n_sort_keys = options.sort_keys.size();
    if (n_sort_keys == 0) {
      return Status::Invalid("Must specify one or more sort keys");
    }
    if (n_sort_keys == 1) {
      ARROW_ASSIGN_OR_RAISE(auto array, PrependInvalidColumn(GetColumn(
                                            batch, options.sort_keys[0].target)));
      return SortIndices(*array, options, ctx);
    }

    auto out_type = uint64();
    auto length = batch.num_rows();
    auto buffer_size = bit_util::BytesForBits(
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
      ARROW_ASSIGN_OR_RAISE(auto chunked_array, PrependInvalidColumn(GetColumn(
                                                    table, options.sort_keys[0].target)));
      return SortIndices(*chunked_array, options, ctx);
    }

    auto out_type = uint64();
    auto length = table.num_rows();
    auto buffer_size = bit_util::BytesForBits(
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

}  // namespace

Result<std::vector<SortField>> FindSortKeys(const Schema& schema,
                                            const std::vector<SortKey>& sort_keys) {
  std::vector<SortField> fields;
  std::unordered_set<int> seen;
  fields.reserve(sort_keys.size());
  seen.reserve(sort_keys.size());

  for (const auto& sort_key : sort_keys) {
    RETURN_NOT_OK(CheckNonNested(sort_key.target));

    ARROW_ASSIGN_OR_RAISE(auto match,
                          PrependInvalidColumn(sort_key.target.FindOne(schema)));
    if (seen.insert(match[0]).second) {
      fields.push_back({match[0], sort_key.order});
    }
  }
  return fields;
}

Result<NullPartitionResult> SortChunkedArray(ExecContext* ctx, uint64_t* indices_begin,
                                             uint64_t* indices_end,
                                             const ChunkedArray& chunked_array,
                                             SortOrder sort_order,
                                             NullPlacement null_placement) {
  auto physical_type = GetPhysicalType(chunked_array.type());
  auto physical_chunks = GetPhysicalChunks(chunked_array, physical_type);
  return SortChunkedArray(ctx, indices_begin, indices_end, physical_type, physical_chunks,
                          sort_order, null_placement);
}

Result<NullPartitionResult> SortChunkedArray(
    ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
    const std::shared_ptr<DataType>& physical_type, const ArrayVector& physical_chunks,
    SortOrder sort_order, NullPlacement null_placement) {
  NullPartitionResult output;
  ChunkedArraySorter sorter(ctx, indices_begin, indices_end, physical_type,
                            physical_chunks, sort_order, null_placement, &output);
  RETURN_NOT_OK(sorter.Sort());
  return output;
}

void RegisterVectorSort(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<SortIndicesMetaFunction>()));
}

#undef VISIT_SORTABLE_PHYSICAL_TYPES

}  // namespace internal
}  // namespace compute
}  // namespace arrow
