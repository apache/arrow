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

#include <functional>
#include <memory>

#include "arrow/compute/function.h"
#include "arrow/compute/kernels/vector_sort_internal.h"
#include "arrow/compute/registry.h"

namespace arrow::compute::internal {

using ::arrow::util::span;

namespace {

// ----------------------------------------------------------------------
// Rank implementation

// A bit that is set in the sort indices when the value at the current sort index
// is the same as the value at the previous sort index.
constexpr uint64_t kDuplicateMask = 1ULL << 63;

template <typename ValueSelector>
void MarkDuplicates(const NullPartitionResult& sorted, ValueSelector&& value_selector) {
  using T = decltype(value_selector(int64_t{}));

  // Process non-nulls
  if (sorted.non_nulls_end != sorted.non_nulls_begin) {
    auto it = sorted.non_nulls_begin;
    T prev_value = value_selector(*it);
    while (++it < sorted.non_nulls_end) {
      T curr_value = value_selector(*it);
      if (curr_value == prev_value) {
        *it |= kDuplicateMask;
      }
      prev_value = curr_value;
    }
  }

  // Process nulls
  if (sorted.nulls_end != sorted.nulls_begin) {
    // TODO this should be able to distinguish between NaNs and real nulls (GH-45193)
    auto it = sorted.nulls_begin;
    while (++it < sorted.nulls_end) {
      *it |= kDuplicateMask;
    }
  }
}

const RankOptions* GetDefaultRankOptions() {
  static const auto kDefaultRankOptions = RankOptions::Defaults();
  return &kDefaultRankOptions;
}

const RankQuantileOptions* GetDefaultQuantileRankOptions() {
  static const auto kDefaultQuantileRankOptions = RankQuantileOptions::Defaults();
  return &kDefaultQuantileRankOptions;
}

template <typename ArrowType>
Result<NullPartitionResult> DoSortAndMarkDuplicate(
    ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end, const Array& input,
    const std::shared_ptr<DataType>& physical_type, const SortOrder order,
    const NullPlacement null_placement, bool needs_duplicates) {
  using GetView = GetViewType<ArrowType>;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  ARROW_ASSIGN_OR_RAISE(auto array_sorter, GetArraySorter(*physical_type));

  ArrayType array(input.data());
  ARROW_ASSIGN_OR_RAISE(auto sorted,
                        array_sorter(indices_begin, indices_end, array, 0,
                                     ArraySortOptions(order, null_placement), ctx));

  if (needs_duplicates) {
    auto value_selector = [&array](int64_t index) {
      return GetView::LogicalValue(array.GetView(index));
    };
    MarkDuplicates(sorted, value_selector);
  }
  return sorted;
}

template <typename ArrowType>
Result<NullPartitionResult> DoSortAndMarkDuplicate(
    ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
    const ChunkedArray& input, const std::shared_ptr<DataType>& physical_type,
    const SortOrder order, const NullPlacement null_placement, bool needs_duplicates) {
  auto physical_chunks = GetPhysicalChunks(input, physical_type);
  if (physical_chunks.empty()) {
    return NullPartitionResult{};
  }
  ARROW_ASSIGN_OR_RAISE(auto sorted,
                        SortChunkedArray(ctx, indices_begin, indices_end, physical_type,
                                         physical_chunks, order, null_placement));
  if (needs_duplicates) {
    const auto arrays = GetArrayPointers(physical_chunks);
    auto value_selector = [resolver = ChunkedArrayResolver(span(arrays))](int64_t index) {
      return resolver.Resolve(index).Value<ArrowType>();
    };
    MarkDuplicates(sorted, value_selector);
  }
  return sorted;
}

template <typename InputType>
class SortAndMarkDuplicate : public TypeVisitor {
 public:
  SortAndMarkDuplicate(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
                       const InputType& input, const SortOrder order,
                       const NullPlacement null_placement, const bool needs_duplicate)
      : TypeVisitor(),
        ctx_(ctx),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        input_(input),
        order_(order),
        null_placement_(null_placement),
        needs_duplicates_(needs_duplicate),
        physical_type_(GetPhysicalType(input.type())) {}

  Result<NullPartitionResult> Run() {
    RETURN_NOT_OK(physical_type_->Accept(this));
    return sorted_;
  }

#define VISIT(TYPE)                                                                 \
  Status Visit(const TYPE& type) {                                                  \
    ARROW_ASSIGN_OR_RAISE(                                                          \
        sorted_, DoSortAndMarkDuplicate<TYPE>(ctx_, indices_begin_, indices_end_,   \
                                              input_, physical_type_, order_,       \
                                              null_placement_, needs_duplicates_)); \
    return Status::OK();                                                            \
  }

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

 private:
  ExecContext* ctx_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const InputType& input_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  const bool needs_duplicates_;
  const std::shared_ptr<DataType> physical_type_;
  NullPartitionResult sorted_{};
};

// A helper class that emits rankings for the "rank_quantile" function
struct QuantileRanker {
  Result<Datum> CreateRankings(ExecContext* ctx, const NullPartitionResult& sorted) {
    const int64_t length = sorted.overall_end() - sorted.overall_begin();
    ARROW_ASSIGN_OR_RAISE(auto rankings,
                          MakeMutableFloat64Array(length, ctx->memory_pool()));
    auto out_begin = rankings->GetMutableValues<double>(1);

    auto is_duplicate = [](uint64_t index) { return (index & kDuplicateMask) != 0; };
    auto original_index = [](uint64_t index) { return index & ~kDuplicateMask; };

    // The count of values strictly less than the value being considered
    int64_t cum_freq = 0;
    auto it = sorted.overall_begin();

    while (it < sorted.overall_end()) {
      // Look for a run of duplicate values
      DCHECK(!is_duplicate(*it));
      auto run_end = it;
      while (++run_end < sorted.overall_end() && is_duplicate(*run_end)) {
      }
      // The run length, i.e. the frequency of the current value
      int64_t freq = run_end - it;
      double quantile = (cum_freq + 0.5 * freq) / static_cast<double>(length);
      // Output quantile rank values
      for (; it < run_end; ++it) {
        out_begin[original_index(*it)] = quantile;
      }
      cum_freq += freq;
    }
    DCHECK_EQ(cum_freq, length);
    return Datum(rankings);
  }
};

// A helper class that emits rankings for the "rank" function
struct OrdinalRanker {
  explicit OrdinalRanker(RankOptions::Tiebreaker tiebreaker) : tiebreaker_(tiebreaker) {}

  Result<Datum> CreateRankings(ExecContext* ctx, const NullPartitionResult& sorted) {
    const int64_t length = sorted.overall_end() - sorted.overall_begin();
    ARROW_ASSIGN_OR_RAISE(auto rankings,
                          MakeMutableUInt64Array(length, ctx->memory_pool()));
    auto out_begin = rankings->GetMutableValues<uint64_t>(1);
    uint64_t rank;

    auto is_duplicate = [](uint64_t index) { return (index & kDuplicateMask) != 0; };
    auto original_index = [](uint64_t index) { return index & ~kDuplicateMask; };

    switch (tiebreaker_) {
      case RankOptions::Dense: {
        rank = 0;
        for (auto it = sorted.overall_begin(); it < sorted.overall_end(); ++it) {
          if (!is_duplicate(*it)) {
            ++rank;
          }
          out_begin[original_index(*it)] = rank;
        }
        break;
      }

      case RankOptions::First: {
        rank = 0;
        for (auto it = sorted.overall_begin(); it < sorted.overall_end(); it++) {
          // No duplicate marks expected for RankOptions::First
          DCHECK(!is_duplicate(*it));
          out_begin[*it] = ++rank;
        }
        break;
      }

      case RankOptions::Min: {
        rank = 0;
        for (auto it = sorted.overall_begin(); it < sorted.overall_end(); ++it) {
          if (!is_duplicate(*it)) {
            rank = (it - sorted.overall_begin()) + 1;
          }
          out_begin[original_index(*it)] = rank;
        }
        break;
      }

      case RankOptions::Max: {
        rank = length;
        for (auto it = sorted.overall_end() - 1; it >= sorted.overall_begin(); --it) {
          out_begin[original_index(*it)] = rank;
          // If the current index isn't marked as duplicate, then it's the last
          // tie in a row (since we iterate in reverse order), so update rank
          // for the next row of ties.
          if (!is_duplicate(*it)) {
            rank = it - sorted.overall_begin();
          }
        }
        break;
      }
    }

    return Datum(rankings);
  }

 private:
  const RankOptions::Tiebreaker tiebreaker_;
};

const FunctionDoc rank_doc(
    "Compute ordinal ranks of an array (1-based)",
    ("This function computes a rank of the input array.\n"
     "By default, null values are considered greater than any other value and\n"
     "are therefore sorted at the end of the input. For floating-point types,\n"
     "NaNs are considered greater than any other non-null value, but smaller\n"
     "than null values. The default tiebreaker is to assign ranks in order of\n"
     "when ties appear in the input.\n"
     "\n"
     "The handling of nulls, NaNs and tiebreakers can be changed in RankOptions."),
    {"input"}, "RankOptions");

const FunctionDoc rank_quantile_doc(
    "Compute quantile ranks of an array",
    ("This function computes a quantile rank of the input array.\n"
     "By default, null values are considered greater than any other value and\n"
     "are therefore sorted at the end of the input. For floating-point types,\n"
     "NaNs are considered greater than any other non-null value, but smaller\n"
     "than null values.\n"
     "The results are real values strictly between 0 and 1. They are\n"
     "computed as in https://en.wikipedia.org/wiki/Quantile_rank\n"
     "but without multiplying by 100.\n"
     "\n"
     "The handling of nulls and NaNs can be changed in RankQuantileOptions."),
    {"input"}, "RankQuantileOptions");

template <typename Derived>
class RankMetaFunctionBase : public MetaFunction {
 public:
  using MetaFunction::MetaFunction;

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    switch (args[0].kind()) {
      case Datum::ARRAY: {
        return Rank(*args[0].make_array(), *options, ctx);
      } break;
      case Datum::CHUNKED_ARRAY: {
        return Rank(*args[0].chunked_array(), *options, ctx);
      } break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for rank operation: "
        "values=",
        args[0].ToString());
  }

 protected:
  template <typename T>
  Result<Datum> Rank(const T& input, const FunctionOptions& function_options,
                     ExecContext* ctx) const {
    const auto& options =
        checked_cast<const typename Derived::FunctionOptionsType&>(function_options);

    SortOrder order = SortOrder::Ascending;
    if (!options.sort_keys.empty()) {
      order = options.sort_keys[0].order;
    }

    int64_t length = input.length();
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          MakeMutableUInt64Array(length, ctx->memory_pool()));
    auto* indices_begin = indices->GetMutableValues<uint64_t>(1);
    auto* indices_end = indices_begin + length;
    std::iota(indices_begin, indices_end, 0);
    auto needs_duplicates = Derived::NeedsDuplicates(options);
    ARROW_ASSIGN_OR_RAISE(
        auto sorted, SortAndMarkDuplicate(ctx, indices_begin, indices_end, input, order,
                                          options.null_placement, needs_duplicates)
                         .Run());

    auto ranker = Derived::GetRanker(options);
    return ranker.CreateRankings(ctx, sorted);
  }
};

class RankMetaFunction : public RankMetaFunctionBase<RankMetaFunction> {
 public:
  using FunctionOptionsType = RankOptions;
  using RankerType = OrdinalRanker;

  static bool NeedsDuplicates(const RankOptions& options) {
    return options.tiebreaker != RankOptions::First;
  }

  static RankerType GetRanker(const RankOptions& options) {
    return RankerType(options.tiebreaker);
  }

  RankMetaFunction()
      : RankMetaFunctionBase("rank", Arity::Unary(), rank_doc, GetDefaultRankOptions()) {}
};

class RankQuantileMetaFunction : public RankMetaFunctionBase<RankQuantileMetaFunction> {
 public:
  using FunctionOptionsType = RankQuantileOptions;
  using RankerType = QuantileRanker;

  static bool NeedsDuplicates(const RankQuantileOptions&) { return true; }

  static RankerType GetRanker(const RankQuantileOptions& options) { return RankerType(); }

  RankQuantileMetaFunction()
      : RankMetaFunctionBase("rank_quantile", Arity::Unary(), rank_quantile_doc,
                             GetDefaultQuantileRankOptions()) {}
};

}  // namespace

void RegisterVectorRank(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<RankMetaFunction>()));
  DCHECK_OK(registry->AddFunction(std::make_shared<RankQuantileMetaFunction>()));
}

}  // namespace arrow::compute::internal
