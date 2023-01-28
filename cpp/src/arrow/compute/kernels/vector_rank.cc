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

template <typename ValueSelector,
          typename T = std::decay_t<std::invoke_result_t<ValueSelector, int64_t>>>
Result<Datum> CreateRankings(ExecContext* ctx, const NullPartitionResult& sorted,
                             const NullPlacement null_placement,
                             const RankOptions::Tiebreaker tiebreaker,
                             ValueSelector&& value_selector) {
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
    ARROW_ASSIGN_OR_RAISE(*output_, CreateRankings(ctx_, sorted, null_placement_,
                                                   tiebreaker_, value_selector));

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
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    if (chunked_array_.num_chunks() == 0) {
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(
        NullPartitionResult sorted,
        SortChunkedArray(ctx_, indices_begin_, indices_end_, physical_type_,
                         physical_chunks_, order_, null_placement_));

    const auto arrays = GetArrayPointers(physical_chunks_);
    auto value_selector = [resolver = ChunkedArrayResolver(arrays)](int64_t index) {
      return resolver.Resolve<ArrayType>(index).Value();
    };
    ARROW_ASSIGN_OR_RAISE(*output_, CreateRankings(ctx_, sorted, null_placement_,
                                                   tiebreaker_, value_selector));

    return Status::OK();
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
