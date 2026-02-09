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
#include <queue>
#include <span>

#include "arrow/compute/function.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/util/logging_internal.h"

namespace arrow {

using internal::checked_cast;

namespace compute::internal {

namespace {

// ----------------------------------------------------------------------
// TopK/BottomK implementations

const SelectKOptions* GetDefaultSelectKOptions() {
  static const auto kDefaultSelectKOptions = SelectKOptions::Defaults();
  return &kDefaultSelectKOptions;
}

const FunctionDoc select_k_unstable_doc(
    "Select the indices of the first `k` ordered elements from the input",
    ("This function selects an array of indices of the first `k` ordered elements\n"
     "from the `input` array, record batch or table specified in the column keys\n"
     "(`options.sort_keys`). Output is not guaranteed to be stable.\n"
     "Null values are considered greater than any other value and are\n"
     "therefore ordered at the end. For floating-point types, NaNs are considered\n"
     "greater than any other non-null value, but smaller than null values."),
    {"input"}, "SelectKOptions", /*options_required=*/true);

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

struct OutputRangesByNullLikeness {
  std::span<uint64_t> non_null_like_range;
  std::span<uint64_t> nan_range;
  std::span<uint64_t> null_range;
};

OutputRangesByNullLikeness CalculateOutputRangesByNullLikeness(
    int64_t non_null_like_count, int64_t nan_count, int64_t null_count,
    NullPlacement null_placement, std::span<uint64_t> output_indices) {
  auto k = static_cast<int64_t>(output_indices.size());
  int64_t non_null_like_to_take = 0;
  int64_t nan_to_take = 0;
  int64_t null_to_take = 0;
  if (null_placement == NullPlacement::AtEnd) {
    non_null_like_to_take = std::min(k, non_null_like_count);
    nan_to_take = std::min(k - non_null_like_to_take, nan_count);
    null_to_take = std::min(k - non_null_like_to_take - nan_to_take, null_count);
    return OutputRangesByNullLikeness{
        .non_null_like_range = output_indices.subspan(0, non_null_like_to_take),
        .nan_range = output_indices.subspan(non_null_like_to_take, nan_to_take),
        .null_range =
            output_indices.subspan(non_null_like_to_take + nan_to_take, null_to_take)};
  } else {
    null_to_take = std::min(k, null_count);
    nan_to_take = std::min(k - null_to_take, nan_count);
    non_null_like_to_take = std::min(k - null_to_take - nan_to_take, non_null_like_count);
    return OutputRangesByNullLikeness{
        .non_null_like_range =
            output_indices.subspan(null_to_take + nan_to_take, non_null_like_to_take),
        .nan_range = output_indices.subspan(null_to_take, nan_to_take),
        .null_range = output_indices.subspan(0, null_to_take)};
  }
}

template <typename Comparator>
void HeapSortNonNullsToOutput(std::span<uint64_t> non_null_input_range, Comparator cmp,
                              std::span<uint64_t> output_range) {
  std::span<uint64_t> heap = non_null_input_range.subspan(0, output_range.size());
  std::make_heap(heap.begin(), heap.end(), cmp);

  std::span<uint64_t> remaining_input = non_null_input_range.subspan(output_range.size());
  for (uint64_t x_index : remaining_input) {
    if (cmp(x_index, heap.front())) {
      std::pop_heap(heap.begin(), heap.end(), cmp);
      heap.back() = x_index;
      std::push_heap(heap.begin(), heap.end(), cmp);
    }
  }

  // fill output in reverse when destructing,
  // as the "worst" (next-to-would-have-been-replaced) element is at heap-top
  for (auto reverse_out_iter = output_range.rbegin();
       reverse_out_iter != output_range.rend(); reverse_out_iter++) {
    *reverse_out_iter = heap.front();  // heap-top has the next element
    std::ranges::pop_heap(heap, cmp);
    // Decrease heap-size by one
    heap = heap.first(heap.size() - 1);
  }
}

template <typename InType, SortOrder sort_order>
void HeapSortNonNullsToOutput(std::span<uint64_t> non_null_input_range,
                              const typename TypeTraits<InType>::ArrayType& arr,
                              std::span<uint64_t> output_range) {
  using GetView = GetViewType<InType>;
  SelectKComparator<sort_order> comparator;
  auto cmp = [&arr, &comparator](uint64_t left, uint64_t right) {
    const auto lval = GetView::LogicalValue(arr.GetView(left));
    const auto rval = GetView::LogicalValue(arr.GetView(right));
    return comparator(lval, rval);
  };
  HeapSortNonNullsToOutput(non_null_input_range, cmp, output_range);
}

template <typename InType>
void HeapSortNonNullsToOutput(std::span<uint64_t> non_null_input_range,
                              const typename TypeTraits<InType>::ArrayType& arr,
                              SortOrder order, std::span<uint64_t> output_range) {
  if (order == SortOrder::Ascending) {
    HeapSortNonNullsToOutput<InType, SortOrder::Ascending>(non_null_input_range, arr,
                                                           output_range);
  } else {
    HeapSortNonNullsToOutput<InType, SortOrder::Descending>(non_null_input_range, arr,
                                                            output_range);
  }
}

struct PartitionResultByNullLikeness {
  std::span<uint64_t> non_null_like_range;
  std::span<uint64_t> null_range;
  std::span<uint64_t> nan_range;
};

template <typename ArrayType, typename Partitioner>
PartitionResultByNullLikeness PartitionNullsAndNans(uint64_t* indices_begin,
                                                    uint64_t* indices_end,
                                                    const ArrayType& values,
                                                    int64_t offset,
                                                    NullPlacement null_placement) {
  // Partition nulls at start (resp. end), and null-like values just before (resp. after)
  NullPartitionResult p = PartitionNullsOnly<Partitioner>(indices_begin, indices_end,
                                                          values, offset, null_placement);
  NullPartitionResult q = PartitionNullLikes<ArrayType, Partitioner>(
      p.non_nulls_begin, p.non_nulls_end, values, offset, null_placement);
  return PartitionResultByNullLikeness{
      .non_null_like_range = {q.non_nulls_begin, q.non_nulls_end},
      .null_range = {p.nulls_begin, p.nulls_end},
      .nan_range = {q.nulls_begin, q.nulls_end}};
}

class ArraySelector : public TypeVisitor {
 public:
  ArraySelector(ExecContext* ctx, const Array& array, const SelectKOptions& options,
                Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        array_(array),
        k_(options.k),
        order_(options.GetSortKeys()[0].order),
        null_placement_(options.GetSortKeys()[0].null_placement),
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

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    ArrayType arr(array_.data());

    k_ = std::min(k_, arr.length());

    std::vector<uint64_t> indices(arr.length());

    uint64_t* indices_begin = indices.data();
    uint64_t* indices_end = indices_begin + indices.size();
    std::iota(indices_begin, indices_end, 0);

    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          MakeMutableUInt64Array(k_, ctx_->memory_pool()));
    auto* output_begin = take_indices->template GetMutableValues<uint64_t>(1);

    const auto p = PartitionNullsAndNans<ArrayType, NonStablePartitioner>(
        indices_begin, indices_end, arr, 0, null_placement_);

    // From k, calculate
    //   l = non_null_like elements to take from PartitionResult
    //   m = nan elements to take from PartitionResult
    //   n = null elements to take from PartitionResult
    // k = l + m + n because k was clipped to arr.length()
    // And directly compute the ranges in {output, output+k} where we will need to place
    // the selected elements from each group -> no longer need to track null_placement
    auto output = CalculateOutputRangesByNullLikeness(
        p.non_null_like_range.size(), p.nan_range.size(), p.null_range.size(),
        null_placement_, {output_begin, output_begin + k_});

    HeapSortNonNullsToOutput<InType, sort_order>(p.non_null_like_range, arr,
                                                 output.non_null_like_range);
    std::copy(p.nan_range.begin(), p.nan_range.begin() + output.nan_range.size(),
              output.nan_range.begin());
    std::copy(p.null_range.begin(), p.null_range.begin() + output.null_range.size(),
              output.null_range.begin());

    *output_ = Datum(take_indices);
    return Status::OK();
  }

  ExecContext* ctx_;
  const Array& array_;
  int64_t k_;
  SortOrder order_;
  NullPlacement null_placement_;
  const std::shared_ptr<DataType> physical_type_;
  Datum* output_;
};

template <typename ArrayType>
struct TypedHeapItem {
  uint64_t index;
  uint64_t offset;
  ArrayType* array;
};

class ChunkedArraySelector : public TypeVisitor {
  using ResolvedSortKey = ResolvedTableSortKey;
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  ChunkedArraySelector(ExecContext* ctx, const ChunkedArray& chunked_array,
                       const SelectKOptions& options, Datum* output)
      : TypeVisitor(),
        chunked_array_(chunked_array),
        physical_type_(GetPhysicalType(chunked_array.type())),
        physical_chunks_(GetPhysicalChunks(chunked_array_, physical_type_)),
        k_(options.k),
        order_(options.sort_keys[0].order),
        null_placement_(options.sort_keys[0].null_placement),
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

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
#undef VISIT

  template <typename InType>
  int64_t ComputeNanCount() {
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    if constexpr (has_null_like_values<typename ArrayType::TypeClass>()) {
      int64_t nan_count = 0;
      for (const auto& chunk : physical_chunks_) {
        auto values = std::make_shared<ArrayType>(chunk->data());
        int64_t length = values->length();
        for (int64_t index = 0; index < length; ++index) {
          if (std::isnan(values->GetView(index))) {
            nan_count++;
          }
        }
      }
      return nan_count;
    }
    return 0;
  }

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

    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          MakeMutableUInt64Array(k_, ctx_->memory_pool()));
    auto* output_begin = take_indices->GetMutableValues<uint64_t>(1);

    int64_t null_count = chunked_array_.null_count();
    int64_t nan_count = ComputeNanCount<InType>();
    int64_t non_null_like_count = chunked_array_.length() - null_count - nan_count;

    auto output = CalculateOutputRangesByNullLikeness(non_null_like_count, nan_count,
                                                      null_count, null_placement_,
                                                      {output_begin, output_begin + k_});

    // Now we can independently fill the output with non_null, nan and null items.
    // For non_null, we do a heap_sort, the others can just be copied until
    // nan_taken == output.nan_range.size() and
    // null_taken == output.null_range.size() respectively
    size_t nan_taken = 0;
    size_t null_taken = 0;

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

      const auto p = PartitionNullsAndNans<ArrayType, NonStablePartitioner>(
          indices_begin, indices_end, arr, 0, null_placement_);

      // First do nulls and nans
      auto iter = p.null_range.begin();
      for (; iter != p.null_range.end() && null_taken < output.null_range.size();
           ++iter) {
        output.null_range[null_taken] = offset + *iter;
        null_taken++;
      }
      iter = p.nan_range.begin();
      for (; iter != p.nan_range.end() && nan_taken < output.nan_range.size(); ++iter) {
        output.nan_range[nan_taken] = offset + *iter;
        nan_taken++;
      }

      iter = p.non_null_like_range.begin();
      for (; iter != p.non_null_like_range.end() &&
             heap.size() < output.non_null_like_range.size();
           ++iter) {
        heap.push(HeapItem{*iter, offset, &arr});
      }
      for (; iter != p.non_null_like_range.end() && !heap.empty(); ++iter) {
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

    // We sized output.non_null_like_range to hold exactly sufficient indices,
    // so the heap must have been completely filled
    assert(heap.size() == output.non_null_like_range.size());

    for (auto reverse_out_iter = output.non_null_like_range.rbegin();
         reverse_out_iter != output.non_null_like_range.rend(); reverse_out_iter++) {
      *reverse_out_iter =
          heap.top().index + heap.top().offset;  // heap-top has the next element
      heap.pop();
    }

    *output_ = Datum(take_indices);
    return Status::OK();
  }

  const ChunkedArray& chunked_array_;
  const std::shared_ptr<DataType> physical_type_;
  const ArrayVector physical_chunks_;
  int64_t k_;
  SortOrder order_;
  NullPlacement null_placement_;
  ExecContext* ctx_;
  Datum* output_;
};

class RecordBatchSelector {
 private:
  using ResolvedSortKey = ResolvedRecordBatchSortKey;
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  RecordBatchSelector(ExecContext* ctx, const RecordBatch& record_batch,
                      const SelectKOptions& options, Datum* output)
      : ctx_(ctx),
        record_batch_(record_batch),
        k_(options.k),
        output_(output),
        sort_keys_(ResolveSortKeys(record_batch, options.sort_keys, &status_)),
        comparator_(sort_keys_) {}

  Status Run() {
    RETURN_NOT_OK(status_);
    return SelectKthInternal();
  }

 protected:
  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto maybe_array = GetColumn(batch, key.target);
      if (!maybe_array.ok()) {
        *status = maybe_array.status();
        return {};
      }
      resolved.emplace_back(*std::move(maybe_array), key.order, key.null_placement);
    }
    return resolved;
  }

  class SelectKForKey : public TypeVisitor {
   public:
    SelectKForKey(RecordBatchSelector* selector, size_t start_sort_key_index,
                  std::span<uint64_t> input_indices, std::span<uint64_t> output_indices)
        : TypeVisitor(),
          selector_(selector),
          start_sort_key_index_(start_sort_key_index),
          input_indices_(input_indices),
          output_indices_(output_indices) {}

   private:
    template <typename InType>
    Status Do() {
      using ArrayType = typename TypeTraits<InType>::ArrayType;
      const auto& first_remaining_sort_key = selector_->sort_keys_[start_sort_key_index_];
      const auto& arr = checked_cast<const ArrayType&>(first_remaining_sort_key.array);

      uint64_t* input_indices_begin = input_indices_.data();
      uint64_t* input_indices_end = input_indices_.data() + input_indices_.size();

      const auto p = PartitionNullsAndNans<ArrayType, NonStablePartitioner>(
          input_indices_begin, input_indices_end, arr, 0,
          first_remaining_sort_key.null_placement);

      // From k = output_range.size(), calculate
      //   l = non_null_like elements to take from PartitionResult
      //   m = nan elements to take from PartitionResult
      //   n = null elements to take from PartitionResult
      // k = l + m + n because k was clipped to num_rows()
      // And directly compute the ranges in output_indices_ where we will need to place
      // the selected elements from each group -> no longer need to track null_placement
      auto output = CalculateOutputRangesByNullLikeness(
          static_cast<int64_t>(p.non_null_like_range.size()),
          static_cast<int64_t>(p.nan_range.size()),
          static_cast<int64_t>(p.null_range.size()),
          first_remaining_sort_key.null_placement, output_indices_);

      bool last_sort_key = start_sort_key_index_ + 1 == selector_->sort_keys_.size();

      if (last_sort_key) {
        if (!output.non_null_like_range.empty()) {
          HeapSortNonNullsToOutput<InType>(p.non_null_like_range, arr,
                                           first_remaining_sort_key.order,
                                           output.non_null_like_range);
        }
        if (output.nan_range.size() > 0) {
          // We have the last sort_key, can just copy over the null values
          std::copy(p.nan_range.begin(), p.nan_range.begin() + output.nan_range.size(),
                    output.nan_range.begin());
        }
        if (output.null_range.size() > 0) {
          // We have the last sort_key, can just copy over the null values
          std::copy(p.null_range.begin(), p.null_range.begin() + output.null_range.size(),
                    output.null_range.begin());
        }
      } else {
        if (!output.non_null_like_range.empty()) {
          auto cmp = [&](uint64_t left, uint64_t right) {
            return selector_->comparator_.Compare(left, right, start_sort_key_index_);
          };
          HeapSortNonNullsToOutput(p.non_null_like_range, cmp,
                                   output.non_null_like_range);
        }
        if (output.nan_range.size() > 0) {
          ARROW_RETURN_NOT_OK(selector_->DoSelectKForKey(start_sort_key_index_ + 1,
                                                         p.nan_range, output.nan_range));
        }
        if (output.null_range.size() > 0) {
          ARROW_RETURN_NOT_OK(selector_->DoSelectKForKey(
              start_sort_key_index_ + 1, p.null_range, output.null_range));
        }
      }

      return Status::OK();
    }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return Do<TYPE>(); }
    VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

    RecordBatchSelector* selector_;
    size_t start_sort_key_index_;
    std::span<uint64_t> input_indices_;
    std::span<uint64_t> output_indices_;
  };

  Status DoSelectKForKey(size_t start_sort_key_index, std::span<uint64_t> input_indices,
                         std::span<uint64_t> output_indices) {
    SelectKForKey tmp(this, start_sort_key_index, input_indices, output_indices);
    return sort_keys_.at(start_sort_key_index).type->Accept(&tmp);
  }

  Status SelectKthInternal() {
    if (k_ > record_batch_.num_rows()) {
      k_ = record_batch_.num_rows();
    }

    std::vector<uint64_t> input_indices(record_batch_.num_rows());
    std::iota(input_indices.begin(), input_indices.end(), 0);

    // We do not directly sort indices in output_indices, as it hold only k_ indices,
    // but e.g. need to partition all record_batch_.num_rows() of them
    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          MakeMutableUInt64Array(k_, ctx_->memory_pool()));
    auto* output_indices = take_indices->template GetMutableValues<uint64_t>(1);

    std::span<uint64_t> input_indices_span(input_indices);
    ARROW_RETURN_NOT_OK(
        DoSelectKForKey(0, input_indices_span, {output_indices, output_indices + k_}));
    *output_ = Datum(take_indices);
    return arrow::Status::OK();
  }

  Status status_;
  ExecContext* ctx_;
  const RecordBatch& record_batch_;
  int64_t k_;
  Datum* output_;
  std::vector<ResolvedSortKey> sort_keys_;
  Comparator comparator_;
};

class TableSelector : public TypeVisitor {
 private:
  struct ResolvedSortKey {
    ResolvedSortKey(const std::shared_ptr<ChunkedArray>& chunked_array,
                    const SortOrder order, const NullPlacement null_placement)
        : order(order),
          null_placement(null_placement),
          type(GetPhysicalType(chunked_array->type())),
          chunks(GetPhysicalChunks(*chunked_array, type)),
          null_count(chunked_array->null_count()),
          resolver(GetArrayPointers(chunks)) {}

    using LocationType = int64_t;

    // Find the target chunk and index in the target chunk from an
    // index in chunked array.
    ResolvedChunk GetChunk(int64_t index) const { return resolver.Resolve(index); }

    const SortOrder order;
    const NullPlacement null_placement;
    const std::shared_ptr<DataType> type;
    const ArrayVector chunks;
    const int64_t null_count;
    const ChunkedArrayResolver resolver;
  };
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  TableSelector(ExecContext* ctx, const Table& table, const SelectKOptions& options,
                Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        table_(table),
        k_(options.k),
        output_(output),
        sort_keys_(ResolveSortKeys(table, options.GetSortKeys(), &status_)),
        comparator_(sort_keys_) {}

  Status Run() {
    RETURN_NOT_OK(status_);
    return SelectKthInternal();
  }

 protected:
  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const Table& table, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto maybe_chunked_array = GetColumn(table, key.target);
      if (!maybe_chunked_array.ok()) {
        *status = maybe_chunked_array.status();
        return {};
      }
      resolved.emplace_back(*std::move(maybe_chunked_array), key.order,
                            key.null_placement);
    }
    return resolved;
  }

  // XXX this implementation is rather inefficient as it computes chunk indices
  // at every comparison.  Instead we should iterate over individual batches
  // and remember ChunkLocation entries in the max-heap.
  Status SelectKthInternal() {
    auto& comparator = comparator_;

    const auto num_rows = table_.num_rows();
    if (num_rows == 0) {
      return Status::OK();
    }
    if (k_ > table_.num_rows()) {
      k_ = table_.num_rows();
    }
    std::function<bool(const uint64_t&, const uint64_t&)> cmp =
        [&](const uint64_t& left, const uint64_t& right) -> bool {
      return comparator.Compare(left, right, 0);
    };
    using HeapContainer =
        std::priority_queue<uint64_t, std::vector<uint64_t>, decltype(cmp)>;

    std::vector<uint64_t> indices(num_rows);
    uint64_t* indices_begin = indices.data();
    uint64_t* indices_end = indices_begin + indices.size();
    std::iota(indices_begin, indices_end, 0);

    auto kth_begin = std::min(indices_begin + k_, indices_end);

    HeapContainer heap(indices_begin, kth_begin, cmp);
    for (auto iter = kth_begin; iter != indices_end && !heap.empty(); ++iter) {
      uint64_t x_index = *iter;
      uint64_t top_item = heap.top();
      if (cmp(x_index, top_item)) {
        heap.pop();
        heap.push(x_index);
      }
    }
    auto out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          MakeMutableUInt64Array(out_size, ctx_->memory_pool()));
    auto* out_cbegin = take_indices->GetMutableValues<uint64_t>(1) + out_size - 1;
    while (heap.size() > 0) {
      *out_cbegin = heap.top();
      heap.pop();
      --out_cbegin;
    }
    *output_ = Datum(take_indices);
    return Status::OK();
  }

  Status status_;
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
    RETURN_NOT_OK(CheckNonNested(key.target));
    RETURN_NOT_OK(PrependInvalidColumn(key.target.FindOne(schema)));
  }
  return Status::OK();
}

class SelectKUnstableMetaFunction : public MetaFunction {
 public:
  SelectKUnstableMetaFunction()
      : MetaFunction("select_k_unstable", Arity::Unary(), select_k_unstable_doc,
                     GetDefaultSelectKOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    const auto& select_k_options = static_cast<const SelectKOptions&>(*options);
    if (select_k_options.k < 0) {
      return Status::Invalid("select_k_unstable requires a nonnegative `k`, got ",
                             select_k_options.k);
    }
    if (select_k_options.sort_keys.size() == 0) {
      return Status::Invalid("select_k_unstable requires a non-empty `sort_keys`");
    }
    switch (args[0].kind()) {
      case Datum::ARRAY:
        return SelectKth(*args[0].make_array(), select_k_options, ctx);
      case Datum::CHUNKED_ARRAY:
        return SelectKth(*args[0].chunked_array(), select_k_options, ctx);
      case Datum::RECORD_BATCH:
        return SelectKth(*args[0].record_batch(), select_k_options, ctx);
      case Datum::TABLE:
        return SelectKth(*args[0].table(), select_k_options, ctx);
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
    ArraySelector selector(ctx, array, options, &output);
    ARROW_RETURN_NOT_OK(selector.Run());
    return output;
  }

  Result<Datum> SelectKth(const ChunkedArray& chunked_array,
                          const SelectKOptions& options, ExecContext* ctx) const {
    Datum output;
    ChunkedArraySelector selector(ctx, chunked_array, options, &output);
    ARROW_RETURN_NOT_OK(selector.Run());
    return output;
  }
  Result<Datum> SelectKth(const RecordBatch& record_batch, const SelectKOptions& options,
                          ExecContext* ctx) const {
    ARROW_RETURN_NOT_OK(CheckConsistency(*record_batch.schema(), options.GetSortKeys()));
    Datum output;
    RecordBatchSelector selector(ctx, record_batch, options, &output);
    ARROW_RETURN_NOT_OK(selector.Run());
    return output;
  }
  Result<Datum> SelectKth(const Table& table, const SelectKOptions& options,
                          ExecContext* ctx) const {
    ARROW_RETURN_NOT_OK(CheckConsistency(*table.schema(), options.GetSortKeys()));
    Datum output;
    TableSelector selector(ctx, table, options, &output);
    ARROW_RETURN_NOT_OK(selector.Run());
    return output;
  }
};

}  // namespace

void RegisterVectorSelectK(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<SelectKUnstableMetaFunction>()));
}

}  // namespace compute::internal
}  // namespace arrow
