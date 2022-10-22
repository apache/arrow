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

#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"

namespace arrow::compute::internal {

namespace {

// ----------------------------------------------------------------------
// Rank implementation

template <typename T>
Result<Datum> CreateRankings(ExecContext* ctx, const NullPartitionResult& sorted,
                             const NullPlacement null_placement,
                             const RankOptions::Tiebreaker tiebreaker,
                             std::function<T(int64_t)> value_selector) {
  auto length = sorted.overall_end() - sorted.overall_begin();
  ARROW_ASSIGN_OR_RAISE(auto rankings,
                        MakeMutableUInt64Array(length, ctx->memory_pool()));
  auto out_begin = rankings->GetMutableValues<uint64_t>(1);
  uint64_t rank;

  switch (tiebreaker) {
    case RankOptions::Dense: {
      T curr_value, prev_value{};
      rank = 0;

      if (null_placement == NullPlacement::AtStart && sorted.null_count() > 0) {
        rank++;
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }

      for (auto it = sorted.non_nulls_begin; it < sorted.non_nulls_end; it++) {
        curr_value = value_selector(*it);
        if (it == sorted.non_nulls_begin || curr_value != prev_value) {
          rank++;
        }

        out_begin[*it] = rank;
        prev_value = curr_value;
      }

      if (null_placement == NullPlacement::AtEnd) {
        rank++;
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }
      break;
    }

    case RankOptions::First: {
      rank = 0;
      for (auto it = sorted.overall_begin(); it < sorted.overall_end(); it++) {
        out_begin[*it] = ++rank;
      }
      break;
    }

    case RankOptions::Min: {
      T curr_value, prev_value{};
      rank = 0;

      if (null_placement == NullPlacement::AtStart) {
        rank++;
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }

      for (auto it = sorted.non_nulls_begin; it < sorted.non_nulls_end; it++) {
        curr_value = value_selector(*it);
        if (it == sorted.non_nulls_begin || curr_value != prev_value) {
          rank = (it - sorted.overall_begin()) + 1;
        }
        out_begin[*it] = rank;
        prev_value = curr_value;
      }

      if (null_placement == NullPlacement::AtEnd) {
        rank = sorted.non_null_count() + 1;
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }
      break;
    }

    case RankOptions::Max: {
      // The algorithm for Max is just like Min, but in reverse order.
      T curr_value, prev_value{};
      rank = length;

      if (null_placement == NullPlacement::AtEnd) {
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }

      for (auto it = sorted.non_nulls_end - 1; it >= sorted.non_nulls_begin; it--) {
        curr_value = value_selector(*it);

        if (it == sorted.non_nulls_end - 1 || curr_value != prev_value) {
          rank = (it - sorted.overall_begin()) + 1;
        }
        out_begin[*it] = rank;
        prev_value = curr_value;
      }

      if (null_placement == NullPlacement::AtStart) {
        rank = sorted.null_count();
        for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
          out_begin[*it] = rank;
        }
      }

      break;
    }
  }

  return Datum(rankings);
}

const RankOptions* GetDefaultRankOptions() {
  static const auto kDefaultRankOptions = RankOptions::Defaults();
  return &kDefaultRankOptions;
}

class ArrayRanker : public TypeVisitor {
 public:
  ArrayRanker(ExecContext* ctx, const Array& array, const RankOptions& options,
              Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        array_(array),
        options_(options),
        null_placement_(options.null_placement),
        tiebreaker_(options.tiebreaker),
        physical_type_(GetPhysicalType(array.type())),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return RankInternal<TYPE>(); }

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

  template <typename InType>
  Status RankInternal() {
    using GetView = GetViewType<InType>;
    using T = typename GetViewType<InType>::T;
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    ArrayType arr(array_.data());

    SortOrder order = SortOrder::Ascending;
    if (!options_.sort_keys.empty()) {
      order = options_.sort_keys[0].order;
    }
    ArraySortOptions array_options(order, null_placement_);

    auto length = array_.length();
    ARROW_ASSIGN_OR_RAISE(auto sort_indices,
                          MakeMutableUInt64Array(length, ctx_->memory_pool()));
    auto sort_begin = sort_indices->GetMutableValues<uint64_t>(1);
    auto sort_end = sort_begin + length;
    std::iota(sort_begin, sort_end, 0);

    ARROW_ASSIGN_OR_RAISE(auto array_sorter, GetArraySorter(*physical_type_));

    NullPartitionResult sorted =
        array_sorter(sort_begin, sort_end, arr, 0, array_options);

    auto value_selector = [&arr](int64_t index) {
      return GetView::LogicalValue(arr.GetView(index));
    };
    auto rankings =
        CreateRankings<T>(ctx_, sorted, null_placement_, tiebreaker_, value_selector);
    ARROW_ASSIGN_OR_RAISE(auto output, rankings);
    *output_ = output;
    return Status::OK();
  }

  ExecContext* ctx_;
  const Array& array_;
  const RankOptions& options_;
  const NullPlacement null_placement_;
  const RankOptions::Tiebreaker tiebreaker_;
  const std::shared_ptr<DataType> physical_type_;
  Datum* output_;
};

class ChunkedArrayRanker : public TypeVisitor {
 public:
  // TODO: here we accept order / null_placement / tiebreaker as separate arguments
  // whereas the ArrayRanker accepts them as the RankOptions struct; this is consistent
  // with ArraySorter / ChunkedArraySorter, so likely should refactor ArrayRanker
  ChunkedArrayRanker(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
                     const ChunkedArray& chunked_array, const SortOrder order,
                     const NullPlacement null_placement,
                     const RankOptions::Tiebreaker tiebreaker, Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        chunked_array_(chunked_array),
        physical_type_(GetPhysicalType(chunked_array.type())),
        physical_chunks_(GetPhysicalChunks(chunked_array_, physical_type_)),
        order_(order),
        null_placement_(null_placement),
        tiebreaker_(tiebreaker),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

#define VISIT(TYPE) \
  Status Visit(const TYPE& type) { return RankInternal<TYPE>(); }

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

  template <typename InType>
  Status RankInternal() {
    using T = typename GetViewType<InType>::T;
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    const auto num_chunks = chunked_array_.num_chunks();
    if (num_chunks == 0) {
      return Status::OK();
    }
    const auto arrays = GetArrayPointers(physical_chunks_);

    ArraySortOptions array_options(order_, null_placement_);

    ARROW_ASSIGN_OR_RAISE(auto array_sorter, GetArraySorter(*physical_type_));

    // See related ChunkedArraySort method for comments
    std::vector<NullPartitionResult> sorted(num_chunks);
    int64_t begin_offset = 0;
    int64_t end_offset = 0;
    int64_t null_count = 0;
    for (int i = 0; i < num_chunks; ++i) {
      const auto array = checked_cast<const ArrayType*>(arrays[i]);
      end_offset += array->length();
      null_count += array->null_count();
      sorted[i] = array_sorter(indices_begin_ + begin_offset, indices_begin_ + end_offset,
                               *array, begin_offset, array_options);
      begin_offset = end_offset;
    }
    DCHECK_EQ(end_offset, indices_end_ - indices_begin_);

    auto resolver = ChunkedArrayResolver(arrays);
    if (sorted.size() > 1) {
      auto merge_nulls = [&](uint64_t* nulls_begin, uint64_t* nulls_middle,
                             uint64_t* nulls_end, uint64_t* temp_indices,
                             int64_t null_count) {
        if (has_null_like_values<typename ArrayType::TypeClass>::value) {
          PartitionNullsOnly<StablePartitioner>(nulls_begin, nulls_end, resolver,
                                                null_count, null_placement_);
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

    auto value_selector = [resolver](int64_t index) {
      return resolver.Resolve<ArrayType>(index).Value();
    };
    auto rankings =
        CreateRankings<T>(ctx_, sorted[0], null_placement_, tiebreaker_, value_selector);
    ARROW_ASSIGN_OR_RAISE(auto output, rankings);
    *output_ = output;
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

  ExecContext* ctx_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const ChunkedArray& chunked_array_;
  const std::shared_ptr<DataType> physical_type_;
  const ArrayVector physical_chunks_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  const RankOptions::Tiebreaker tiebreaker_;
  Datum* output_;
};

const FunctionDoc rank_doc(
    "Compute numerical ranks of an array (1-based)",
    ("This function computes a rank of the input array.\n"
     "By default, null values are considered greater than any other value and\n"
     "are therefore sorted at the end of the input. For floating-point types,\n"
     "NaNs are considered greater than any other non-null value, but smaller\n"
     "than null values. The default tiebreaker is to assign ranks in order of\n"
     "when ties appear in the input.\n"
     "\n"
     "The handling of nulls, NaNs and tiebreakers can be changed in RankOptions."),
    {"input"}, "RankOptions");

class RankMetaFunction : public MetaFunction {
 public:
  RankMetaFunction()
      : MetaFunction("rank", Arity::Unary(), rank_doc, GetDefaultRankOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    const auto& rank_options = checked_cast<const RankOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY: {
        return Rank(*args[0].make_array(), rank_options, ctx);
      } break;
      case Datum::CHUNKED_ARRAY: {
        return Rank(*args[0].chunked_array(), rank_options, ctx);
      } break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for rank operation: "
        "values=",
        args[0].ToString());
  }

 private:
  Result<Datum> Rank(const Array& array, const RankOptions& options,
                     ExecContext* ctx) const {
    Datum output;
    ArrayRanker ranker(ctx, array, options, &output);
    ARROW_RETURN_NOT_OK(ranker.Run());
    return output;
  }

  Result<Datum> Rank(const ChunkedArray& chunked_array, const RankOptions& options,
                     ExecContext* ctx) const {
    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }

    auto sort_type = uint64();
    auto length = chunked_array.length();
    auto buffer_size = bit_util::BytesForBits(
        length * std::static_pointer_cast<UInt64Type>(sort_type)->bit_width());
    std::vector<std::shared_ptr<Buffer>> buffers(2);
    ARROW_ASSIGN_OR_RAISE(buffers[1],
                          AllocateResizableBuffer(buffer_size, ctx->memory_pool()));
    auto out = std::make_shared<ArrayData>(sort_type, length, buffers, 0);
    auto sort_begin = out->GetMutableValues<uint64_t>(1);
    auto sort_end = sort_begin + length;
    std::iota(sort_begin, sort_end, 0);

    Datum output;
    ChunkedArrayRanker ranker(ctx, sort_begin, sort_end, chunked_array, order,
                              options.null_placement, options.tiebreaker, &output);
    ARROW_RETURN_NOT_OK(ranker.Run());
    return output;
  }
};

}  // namespace

void RegisterVectorRank(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<RankMetaFunction>()));
}

}  // namespace arrow::compute::internal
