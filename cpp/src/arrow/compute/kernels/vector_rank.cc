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
#include "arrow/util/logging.h"

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

struct RankingsEmitter {
  virtual ~RankingsEmitter() = default;
  virtual bool NeedsDuplicates() = 0;
  virtual Result<Datum> CreateRankings(ExecContext* ctx,
                                       const NullPartitionResult& sorted) = 0;
};

// A helper class that emits rankings for the "rank_percentile" function
struct PercentileRankingsEmitter : public RankingsEmitter {
  explicit PercentileRankingsEmitter(double factor) : factor_(factor) {}

  bool NeedsDuplicates() override { return true; }

  Result<Datum> CreateRankings(ExecContext* ctx,
                               const NullPartitionResult& sorted) override {
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
      double percentile = (cum_freq + 0.5 * freq) * factor_ / static_cast<double>(length);
      // Output percentile rank values
      for (; it < run_end; ++it) {
        out_begin[original_index(*it)] = percentile;
      }
      cum_freq += freq;
    }
    DCHECK_EQ(cum_freq, length);
    return Datum(rankings);
  }

 private:
  const double factor_;
};

// A helper class that emits rankings for the "rank" function
struct OrdinalRankingsEmitter : public RankingsEmitter {
  explicit OrdinalRankingsEmitter(RankOptions::Tiebreaker tiebreaker)
      : tiebreaker_(tiebreaker) {}

  bool NeedsDuplicates() override { return tiebreaker_ != RankOptions::First; }

  Result<Datum> CreateRankings(ExecContext* ctx,
                               const NullPartitionResult& sorted) override {
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

const RankOptions* GetDefaultRankOptions() {
  static const auto kDefaultRankOptions = RankOptions::Defaults();
  return &kDefaultRankOptions;
}

const RankPercentileOptions* GetDefaultPercentileRankOptions() {
  static const auto kDefaultPercentileRankOptions = RankPercentileOptions::Defaults();
  return &kDefaultPercentileRankOptions;
}

template <typename InputType, typename RankerType>
class RankerMixin : public TypeVisitor {
 public:
  RankerMixin(ExecContext* ctx, uint64_t* indices_begin, uint64_t* indices_end,
              const InputType& input, const SortOrder order,
              const NullPlacement null_placement, RankingsEmitter* emitter)
      : TypeVisitor(),
        ctx_(ctx),
        indices_begin_(indices_begin),
        indices_end_(indices_end),
        input_(input),
        order_(order),
        null_placement_(null_placement),
        physical_type_(GetPhysicalType(input.type())),
        emitter_(emitter) {}

  Result<Datum> Run() {
    RETURN_NOT_OK(physical_type_->Accept(this));
    return emitter_->CreateRankings(ctx_, sorted_);
  }

#define VISIT(TYPE)                                                                \
  Status Visit(const TYPE& type) {                                                 \
    return static_cast<RankerType*>(this)->template SortAndMarkDuplicates<TYPE>(); \
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
  const std::shared_ptr<DataType> physical_type_;
  RankingsEmitter* emitter_;
  NullPartitionResult sorted_{};
};

template <typename T>
class Ranker;

template <>
class Ranker<Array> : public RankerMixin<Array, Ranker<Array>> {
 public:
  using RankerMixin::RankerMixin;

  template <typename InType>
  Status SortAndMarkDuplicates() {
    using GetView = GetViewType<InType>;
    using ArrayType = typename TypeTraits<InType>::ArrayType;

    ARROW_ASSIGN_OR_RAISE(auto array_sorter, GetArraySorter(*physical_type_));

    ArrayType array(input_.data());
    ARROW_ASSIGN_OR_RAISE(sorted_,
                          array_sorter(indices_begin_, indices_end_, array, 0,
                                       ArraySortOptions(order_, null_placement_), ctx_));

    if (emitter_->NeedsDuplicates()) {
      auto value_selector = [&array](int64_t index) {
        return GetView::LogicalValue(array.GetView(index));
      };
      MarkDuplicates(sorted_, value_selector);
    }
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
  Status SortAndMarkDuplicates() {
    if (physical_chunks_.empty()) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(
        sorted_, SortChunkedArray(ctx_, indices_begin_, indices_end_, physical_type_,
                                  physical_chunks_, order_, null_placement_));
    if (emitter_->NeedsDuplicates()) {
      const auto arrays = GetArrayPointers(physical_chunks_);
      auto value_selector = [resolver =
                                 ChunkedArrayResolver(span(arrays))](int64_t index) {
        return resolver.Resolve(index).Value<InType>();
      };
      MarkDuplicates(sorted_, value_selector);
    }
    return Status::OK();
  }

 private:
  const ArrayVector physical_chunks_;
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

const FunctionDoc rank_percentile_doc(
    "Compute percentile ranks of an array",
    ("This function computes a percentile rank of the input array.\n"
     "By default, null values are considered greater than any other value and\n"
     "are therefore sorted at the end of the input. For floating-point types,\n"
     "NaNs are considered greater than any other non-null value, but smaller\n"
     "than null values.\n"
     "Results are computed as in https://en.wikipedia.org/wiki/Percentile_rank\n"
     "\n"
     "The handling of nulls and NaNs, and the constant factor can be changed\n"
     "in RankPercentileOptions."),
    {"input"}, "RankPercentileOptions");

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
  struct UnpackedOptions {
    SortOrder order{SortOrder::Ascending};
    NullPlacement null_placement;
    std::unique_ptr<RankingsEmitter> emitter;
  };

  virtual UnpackedOptions UnpackOptions(const FunctionOptions&) const = 0;

  template <typename T>
  Result<Datum> Rank(const T& input, const FunctionOptions& function_options,
                     ExecContext* ctx) const {
    auto options = UnpackOptions(function_options);

    int64_t length = input.length();
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          MakeMutableUInt64Array(length, ctx->memory_pool()));
    auto* indices_begin = indices->GetMutableValues<uint64_t>(1);
    auto* indices_end = indices_begin + length;
    std::iota(indices_begin, indices_end, 0);

    Ranker<T> ranker(ctx, indices_begin, indices_end, input, options.order,
                     options.null_placement, options.emitter.get());
    return ranker.Run();
  }
};

class RankMetaFunction : public RankMetaFunctionBase {
 public:
  RankMetaFunction()
      : RankMetaFunctionBase("rank", Arity::Unary(), rank_doc, GetDefaultRankOptions()) {}

 protected:
  UnpackedOptions UnpackOptions(const FunctionOptions& function_options) const override {
    const auto& options = checked_cast<const RankOptions&>(function_options);
    UnpackedOptions unpacked{
        SortOrder::Ascending, options.null_placement,
        std::make_unique<OrdinalRankingsEmitter>(options.tiebreaker)};
    if (!options.sort_keys.empty()) {
      unpacked.order = options.sort_keys[0].order;
    }
    return unpacked;
  }
};

class RankPercentileMetaFunction : public RankMetaFunctionBase {
 public:
  RankPercentileMetaFunction()
      : RankMetaFunctionBase("rank_percentile", Arity::Unary(), rank_percentile_doc,
                             GetDefaultPercentileRankOptions()) {}

 protected:
  UnpackedOptions UnpackOptions(const FunctionOptions& function_options) const override {
    const auto& options = checked_cast<const RankPercentileOptions&>(function_options);
    UnpackedOptions unpacked{SortOrder::Ascending, options.null_placement,
                             std::make_unique<PercentileRankingsEmitter>(options.factor)};
    if (!options.sort_keys.empty()) {
      unpacked.order = options.sort_keys[0].order;
    }
    return unpacked;
  }
};

}  // namespace

void RegisterVectorRank(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<RankMetaFunction>()));
  DCHECK_OK(registry->AddFunction(std::make_shared<RankPercentileMetaFunction>()));
}

}  // namespace arrow::compute::internal
