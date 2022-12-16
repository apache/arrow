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
    uint64_t rank;

    ARROW_ASSIGN_OR_RAISE(auto rankings,
                          MakeMutableUInt64Array(length, ctx_->memory_pool()));
    auto out_begin = rankings->GetMutableValues<uint64_t>(1);

    switch (tiebreaker_) {
      case RankOptions::Dense: {
        T curr_value, prev_value{};
        rank = 0;

        if (null_placement_ == NullPlacement::AtStart && sorted.null_count() > 0) {
          rank++;
          for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
            out_begin[*it] = rank;
          }
        }

        for (auto it = sorted.non_nulls_begin; it < sorted.non_nulls_end; it++) {
          curr_value = GetView::LogicalValue(arr.GetView(*it));
          if (it == sorted.non_nulls_begin || curr_value != prev_value) {
            rank++;
          }

          out_begin[*it] = rank;
          prev_value = curr_value;
        }

        if (null_placement_ == NullPlacement::AtEnd) {
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

        if (null_placement_ == NullPlacement::AtStart) {
          rank++;
          for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
            out_begin[*it] = rank;
          }
        }

        for (auto it = sorted.non_nulls_begin; it < sorted.non_nulls_end; it++) {
          curr_value = GetView::LogicalValue(arr.GetView(*it));
          if (it == sorted.non_nulls_begin || curr_value != prev_value) {
            rank = (it - sorted.overall_begin()) + 1;
          }
          out_begin[*it] = rank;
          prev_value = curr_value;
        }

        if (null_placement_ == NullPlacement::AtEnd) {
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

        if (null_placement_ == NullPlacement::AtEnd) {
          for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
            out_begin[*it] = rank;
          }
        }

        for (auto it = sorted.non_nulls_end - 1; it >= sorted.non_nulls_begin; it--) {
          curr_value = GetView::LogicalValue(arr.GetView(*it));
          if (it == sorted.non_nulls_end - 1 || curr_value != prev_value) {
            rank = (it - sorted.overall_begin()) + 1;
          }
          out_begin[*it] = rank;
          prev_value = curr_value;
        }

        if (null_placement_ == NullPlacement::AtStart) {
          rank = sorted.null_count();
          for (auto it = sorted.nulls_begin; it < sorted.nulls_end; it++) {
            out_begin[*it] = rank;
          }
        }

        break;
      }
    }

    *output_ = Datum(rankings);
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
};

}  // namespace

void RegisterVectorRank(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<RankMetaFunction>()));
}

}  // namespace arrow::compute::internal
