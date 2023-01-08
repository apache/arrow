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

#include <queue>

#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"

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

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

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

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
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

    auto out_size = static_cast<int64_t>(heap.size());
    ARROW_ASSIGN_OR_RAISE(auto take_indices,
                          MakeMutableUInt64Array(out_size, ctx_->memory_pool()));
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
  using ResolvedSortKey = ResolvedRecordBatchSortKey;
  using Comparator = MultipleKeyComparator<ResolvedSortKey>;

 public:
  RecordBatchSelecter(ExecContext* ctx, const RecordBatch& record_batch,
                      const SelectKOptions& options, Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        record_batch_(record_batch),
        k_(options.k),
        output_(output),
        sort_keys_(ResolveSortKeys(record_batch, options.sort_keys, &status_)),
        comparator_(sort_keys_, NullPlacement::AtEnd) {}

  Status Run() {
    RETURN_NOT_OK(status_);
    return sort_keys_[0].type->Accept(this);
  }

 protected:
#define VISIT(TYPE)                                            \
  Status Visit(const TYPE& type) {                             \
    if (sort_keys_[0].order == SortOrder::Descending)          \
      return SelectKthInternal<TYPE, SortOrder::Descending>(); \
    return SelectKthInternal<TYPE, SortOrder::Ascending>();    \
  }
  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)
#undef VISIT

  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const RecordBatch& batch, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto maybe_array = GetColumn(batch, key.target);
      if (!maybe_array.ok()) {
        *status = maybe_array.status();
        return {};
      }
      resolved.emplace_back(*std::move(maybe_array), key.order);
    }
    return resolved;
  }

  template <typename InType, SortOrder sort_order>
  Status SelectKthInternal() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;
    auto& comparator = comparator_;
    const auto& first_sort_key = sort_keys_[0];
    const auto& arr = checked_cast<const ArrayType&>(first_sort_key.array);

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
          null_count(chunked_array->null_count()),
          resolver(GetArrayPointers(chunks)) {}

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
        sort_keys_(ResolveSortKeys(table, options.sort_keys, &status_)),
        comparator_(sort_keys_, NullPlacement::AtEnd) {}

  Status Run() {
    RETURN_NOT_OK(status_);
    return sort_keys_[0].type->Accept(this);
  }

 protected:
#define VISIT(TYPE)                                            \
  Status Visit(const TYPE& type) {                             \
    if (sort_keys_[0].order == SortOrder::Descending)          \
      return SelectKthInternal<TYPE, SortOrder::Descending>(); \
    return SelectKthInternal<TYPE, SortOrder::Ascending>();    \
  }
  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

  static std::vector<ResolvedSortKey> ResolveSortKeys(
      const Table& table, const std::vector<SortKey>& sort_keys, Status* status) {
    std::vector<ResolvedSortKey> resolved;
    for (const auto& key : sort_keys) {
      auto maybe_chunked_array = GetColumn(table, key.target);
      if (!maybe_chunked_array.ok()) {
        *status = maybe_chunked_array.status();
        return {};
      }
      resolved.emplace_back(*std::move(maybe_chunked_array), key.order);
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

}  // namespace

void RegisterVectorSelectK(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<SelectKUnstableMetaFunction>()));
}

}  // namespace compute::internal
}  // namespace arrow
