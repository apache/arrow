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

constexpr bool NeedsDuplicates(RankOptions::Tiebreaker tiebreaker) {
  return tiebreaker != RankOptions::First;
}

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

Result<Datum> CreateRankings(ExecContext* ctx, const NullPartitionResult& sorted,
                             const NullPlacement null_placement,
                             const RankOptions::Tiebreaker tiebreaker) {
  auto length = sorted.overall_end() - sorted.overall_begin();
  ARROW_ASSIGN_OR_RAISE(auto rankings,
                        MakeMutableUInt64Array(length, ctx->memory_pool()));
  auto out_begin = rankings->GetMutableValues<uint64_t>(1);
  uint64_t rank;

  auto is_duplicate = [](uint64_t index) { return (index & kDuplicateMask) != 0; };
  auto original_index = [](uint64_t index) { return index & ~kDuplicateMask; };

  switch (tiebreaker) {
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

const RankOptions* GetDefaultRankOptions() {
  static const auto kDefaultRankOptions = RankOptions::Defaults();
  return &kDefaultRankOptions;
}

template <typename InputType, typename RankerType>
class RankerMixin : public TypeVisitor {
 public:
  RankerMixin(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
              const InputType& input, const SortOrder order,
              const NullPlacement null_placement,
              const RankOptions::Tiebreaker tiebreaker, Datum* output)
      : TypeVisitor(),
        ctx_(ctx),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        input_(input),
        order_(order),
        null_placement_(null_placement),
        tiebreaker_(tiebreaker),
        physical_type_(GetPhysicalType(input.type())),
        output_(output) {}

  Status Run() { return physical_type_->Accept(this); }

#define VISIT(TYPE)                                                       \
  Status Visit(const TYPE& type) {                                        \
    return static_cast<RankerType*>(this)->template RankInternal<TYPE>(); \
  }

  VISIT_SORTABLE_PHYSICAL_TYPES(VISIT)

#undef VISIT

 protected:
  ExecContext* ctx_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
  const InputType& input_;
  const SortOrder order_;
  const NullPlacement null_placement_;
  const RankOptions::Tiebreaker tiebreaker_;
  const std::shared_ptr<DataType> physical_type_;
  Datum* output_;
};

template <typename T>
class Ranker;

template <>
class Ranker<Array> : public RankerMixin<Array, Ranker<Array>> {
 public:
  using RankerMixin::RankerMixin;

  template <typename InType>
  Status RankInternal() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    ARROW_ASSIGN_OR_RAISE(auto array_sorter, GetArraySorter(*physical_type_));

    ArrayType array(input_.data());
    ARROW_ASSIGN_OR_RAISE(NullPartitionResult sorted,
                          array_sorter(indices_begin_, indices_end_, array, 0,
                                       ArraySortOptions(order_, null_placement_), ctx_));

    if (NeedsDuplicates(tiebreaker_)) {
      auto value_selector = [&array](int64_t index) {
        return GetView::LogicalValue(array.GetView(index));
      };
      MarkDuplicates(sorted, value_selector);
    }
    ARROW_ASSIGN_OR_RAISE(*output_,
                          CreateRankings(ctx_, sorted, null_placement_, tiebreaker_));

    return Status::OK();
  }
};

template <>
class Ranker<ChunkedArray> : public RankerMixin<ChunkedArray, Ranker<ChunkedArray>> {
 public:
  template <typename... Args>
  explicit Ranker(Args&&... args)
      : RankerMixin(std::forward<Args>(args)...),
        physical_chunks_(GetPhysicalChunks(input_, physical_type_)) {}

  template <typename InType>
  Status RankInternal() {
    if (physical_chunks_.empty()) {
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(
        NullPartitionResult sorted,
        SortChunkedArray(ctx_, indices_begin_, indices_end_, physical_type_,
                         physical_chunks_, order_, null_placement_));

    if (NeedsDuplicates(tiebreaker_)) {
      const auto arrays = GetArrayPointers(physical_chunks_);
      auto value_selector = [resolver =
                                 ChunkedArrayResolver(span(arrays))](int64_t index) {
        return resolver.Resolve(index).Value<InType>();
      };
      MarkDuplicates(sorted, value_selector);
    }
    ARROW_ASSIGN_OR_RAISE(*output_,
                          CreateRankings(ctx_, sorted, null_placement_, tiebreaker_));
    return Status::OK();
  }

 private:
  const ArrayVector physical_chunks_;
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
  template <typename T>
  static Result<Datum> Rank(const T& input, const RankOptions& options,
                            ExecContext* ctx) {
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

    Datum output;
    Ranker<T> ranker(ctx, indices_begin, indices_end, input, order,
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
