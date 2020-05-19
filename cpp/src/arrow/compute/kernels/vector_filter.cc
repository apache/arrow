// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// returnGegarding copyright ownership.  The ASF licenses this file
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
#include <limits>

#include "arrow/array/concatenate.h"
#include "arrow/builder.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/vector_selection.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

// IndexSequence which yields the indices of positions in a BooleanArray
// which are either null or true
template <FilterOptions::NullSelectionBehavior NullSelectionBehavior>
class FilterIndexSequence {
 public:
  // constexpr so we'll never instantiate bounds checking
  constexpr bool never_out_of_bounds() const { return true; }
  void set_never_out_of_bounds() {}

  constexpr FilterIndexSequence() = default;

  FilterIndexSequence(const BooleanArray& filter, int64_t out_length)
      : filter_(&filter), out_length_(out_length) {}

  std::pair<int64_t, bool> Next() {
    if (NullSelectionBehavior == FilterOptions::DROP) {
      // skip until an index is found at which the filter is true
      while (filter_->IsNull(index_) || !filter_->Value(index_)) {
        ++index_;
      }
      return std::make_pair(index_++, true);
    }

    // skip until an index is found at which the filter is either null or true
    while (filter_->IsValid(index_) && !filter_->Value(index_)) {
      ++index_;
    }
    bool is_valid = filter_->IsValid(index_);
    return std::make_pair(index_++, is_valid);
  }

  int64_t length() const { return out_length_; }

  int64_t null_count() const {
    if (NullSelectionBehavior == FilterOptions::DROP) {
      return 0;
    }
    return filter_->null_count();
  }

 private:
  const BooleanArray* filter_ = nullptr;
  int64_t index_ = 0, out_length_ = -1;
};

static int64_t OutputSize(FilterOptions::NullSelectionBehavior null_selection,
                          const BooleanArray& filter) {
  // TODO(bkietz) this can be optimized. Use Bitmap::VisitWords
  int64_t size = 0;
  if (null_selection == FilterOptions::EMIT_NULL) {
    for (auto i = 0; i < filter.length(); ++i) {
      if (filter.IsNull(i) || filter.Value(i)) {
        ++size;
      }
    }
  } else {
    for (auto i = 0; i < filter.length(); ++i) {
      if (filter.IsValid(i) && filter.Value(i)) {
        ++size;
      }
    }
  }
  return size;
}

struct FilterState : public KernelState {
  FilterState(const FilterOptions& options) : options(options) {}
  FilterOptions options;
};

std::unique_ptr<KernelState> InitFilter(KernelContext*, const Kernel&,
                                      const FunctionOptions* options) {
  auto filter_options = static_cast<const FilterOptions*>(options);
  return std::unique_ptr<KernelState>(new FilterState{*filter_options});
}

template <typename ValueType>
struct FilterFunctor {
  using ArrayType = typename TypeTraits<ValueType>::ArrayType;

  template <FilterOptions::NullSelectionBehavior NullSelection>
  static void ExecImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using IS = FilterIndexSequence<NullSelection>;
    ArrayType values(batch[0].array());
    BooleanArray filter(batch[1].array());
    const int64_t output_size = OutputSize(NullSelection, filter);
    std::shared_ptr<Array> result;
    CTX_RETURN_IF_ERROR(ctx, Select(ctx, values, IS(filter, output_size), &result));
    out->value = result->data();
  }

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& state = checked_cast<const FilterState&>(*ctx->state());
    if (state.options.null_selection_behavior == FilterOptions::EMIT_NULL) {
      ExecImpl<FilterOptions::EMIT_NULL>(ctx, batch, out);
    } else {
      ExecImpl<FilterOptions::DROP>(ctx, batch, out);
    }
  }
};

struct FilterKernelVisitor {

  template <typename Type>
  Status Visit(const Type&) {
    this->result = FilterFunctor<Type>::Exec;
    return Status::OK();
  }

  Status Create(const DataType& type) { return VisitTypeInline(type, this); }
  ArrayKernelExec result;
};

Status GetFilterKernel(const DataType& type, ArrayKernelExec* exec) {
  FilterKernelVisitor visitor;
  RETURN_NOT_OK(visitor.Create(type));
  *exec = visitor.result;
  return Status::OK();
}

void RegisterVectorFilter(FunctionRegistry* registry) {
  VectorKernel base;
  base.init = InitFilter;
  base.mem_allocation = MemAllocation::NO_PREALLOCATE;
  base.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;

  auto filter = std::make_shared<VectorFunction>("filter", /*arity=*/2);
  OutputType out_ty(FirstType);
  InputType arg1_ty = InputType::Array(boolean());
  for (const auto& value_ty : PrimitiveTypes()) {
    InputType arg0_ty = InputType::Array(value_ty);
    base.signature = KernelSignature::Make({arg0_ty, arg1_ty}, out_ty);
    DCHECK_OK(GetFilterKernel(*value_ty, &base.exec));
    DCHECK_OK(filter->AddKernel(base));
  }

  for (const auto& value_ty : g_dummy_parametric_types) {
    InputType arg0_ty = InputType::Array(value_ty->id());
    base.signature = KernelSignature::Make({arg0_ty, arg1_ty}, out_ty);
    DCHECK_OK(GetFilterKernel(*value_ty, &base.exec));
    DCHECK_OK(filter->AddKernel(base));
  }
  DCHECK_OK(registry->AddFunction(std::move(filter)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
