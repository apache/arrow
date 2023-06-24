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

// Vector kernels for pairwise computation

#include <iostream>
#include <memory>
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/util.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

// We reuse the kernel exec function of a scalar binary function to compute pairwise
// results. For example, for pairwise_diff, we reuse subtract's kernel exec.
struct PairwiseState : KernelState {
  PairwiseState(const PairwiseOptions& options, const ArrayKernelExec& scalar_exec)
      : periods(options.periods), scalar_exec(scalar_exec) {}

  int64_t periods;
  const ArrayKernelExec& scalar_exec;
};

KernelInit GeneratePairwiseInit(const ArrayKernelExec& scalar_exec) {
  return [&scalar_exec](KernelContext* ctx, const KernelInitArgs& args) {
    return std::make_unique<PairwiseState>(
        checked_cast<const PairwiseOptions&>(*args.options), scalar_exec);
  };
}

/// A generic pairwise implementation that can be reused by different ops.
Status PairwiseExecImpl(KernelContext* ctx, const ArraySpan& input,
                        const ArrayKernelExec& scalar_exec, int64_t periods,
                        ArrayData* result) {
  auto offset = abs(periods);
  offset = std::min(offset, input.length);
  auto exec_length = input.length - offset;
  // prepare bitmap
  auto null_start = periods > 0 ? 0 : exec_length;
  auto non_null_start = periods > 0 ? offset : 0;
  bit_util::ClearBitmap(result->buffers[0]->mutable_data(), null_start, offset);
  for (int64_t i = non_null_start; i < non_null_start + exec_length; i++) {
    if (input.IsValid(i) && input.IsValid(i - periods)) {
      bit_util::SetBit(result->buffers[0]->mutable_data(), i);
    } else {
      bit_util::ClearBit(result->buffers[0]->mutable_data(), i);
    }
  }
  // prepare input span
  ArraySpan left(input);
  left.SetSlice(periods > 0 ? offset : 0, exec_length);
  ArraySpan right(input);
  right.SetSlice(periods > 0 ? 0 : offset, exec_length);
  // prepare output span
  ArraySpan output_span;
  output_span.SetMembers(*result);
  output_span.offset = periods > 0 ? offset : 0;
  output_span.length = exec_length;
  ExecResult output{output_span};
  // execute scalar function
  RETURN_NOT_OK(scalar_exec(ctx, ExecSpan({left, right}, exec_length), &output));

  return Status::OK();
}

Status PairwiseExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const auto& state = checked_cast<const PairwiseState&>(*ctx->state());
  auto input = batch[0].array;
  RETURN_NOT_OK(PairwiseExecImpl(ctx, batch[0].array, state.scalar_exec, state.periods,
                                 out->array_data_mutable()));
  return Status::OK();
}

const FunctionDoc pairwise_diff_doc(
    "Compute first order difference of an array",
    ("Computes the first order difference of an array, It internally calls \n"
     "the scalar function \"subtract\" to compute \n differences, so its \n"
     "behavior and supported types are the same as \n"
     "\"subtract\". The period can be specified in :struct:`PairwiseOptions`.\n"
     "\n"
     "Results will wrap around on integer overflow. Use function \n"
     "\"pairwise_diff_checked\" if you want overflow to return an error."),
    {"input"}, "PairwiseOptions");

const FunctionDoc pairwise_diff_checked_doc(
    "Compute first order difference of an array",
    ("Computes the first order difference of an array, It internally calls \n"
     "the scalar function \"subtract_checked\" (or the checked variant) to compute \n"
     "differences, so its behavior and supported types are the same as \n"
     "\"subtract_checked\". The period can be specified in :struct:`PairwiseOptions`.\n"
     "\n"
     "This function returns an error on overflow. For a variant that doesn't \n"
     "fail on overflow, use function \"pairwise_diff\"."),
    {"input"}, "PairwiseOptions");

const PairwiseOptions* GetDefaultPairwiseOptions() {
  static const auto kDefaultPairwiseOptions = PairwiseOptions::Defaults();
  return &kDefaultPairwiseOptions;
}

struct PairwiseKernelData {
  InputType input;
  OutputType output;
  ArrayKernelExec exec;
};

void RegisterPairwiseDiffKernels(std::string_view func_name,
                                 std::string_view base_func_name, const FunctionDoc& doc,
                                 FunctionRegistry* registry) {
  VectorKernel base_kernel;
  base_kernel.can_execute_chunkwise = false;
  base_kernel.null_handling = NullHandling::COMPUTED_PREALLOCATE;
  base_kernel.mem_allocation = MemAllocation::PREALLOCATE;
  base_kernel.init = OptionsWrapper<PairwiseOptions>::Init;
  auto func = std::make_shared<VectorFunction>(std::string(func_name), Arity::Unary(),
                                               doc, GetDefaultPairwiseOptions());

  auto base_func_result = registry->GetFunction(std::string(base_func_name));
  DCHECK_OK(base_func_result.status());
  const auto& base_func = checked_cast<const ScalarFunction&>(**base_func_result);
  DCHECK(base_func.arity().num_args == 2);

  for (const auto& base_func_kernel : base_func.kernels()) {
    const auto& base_func_kernel_sig = base_func_kernel->signature;
    if (base_func_kernel_sig->in_types()[0].Equals(base_func_kernel_sig->in_types()[1])) {
      OutputType out_type(base_func_kernel_sig->out_type());
      // Need to wrap base output resolver
      if (out_type.kind() == OutputType::COMPUTED) {
        const auto& base_resolver = base_func_kernel_sig->out_type().resolver();
        auto resolver = [&base_resolver](KernelContext* ctx,
                                         const std::vector<TypeHolder>& input_types) {
          return base_resolver(ctx, {input_types[0], input_types[0]});
        };
        out_type = OutputType(resolver);
      }

      base_kernel.signature =
          KernelSignature::Make({base_func_kernel_sig->in_types()[0]}, out_type);
      base_kernel.exec = PairwiseExec;
      base_kernel.init = GeneratePairwiseInit(base_func_kernel->exec);
      DCHECK_OK(func->AddKernel(base_kernel));
    }
  }

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterVectorPairwise(FunctionRegistry* registry) {
  RegisterPairwiseDiffKernels("pairwise_diff", "subtract", pairwise_diff_doc, registry);
  RegisterPairwiseDiffKernels("pairwise_diff_checked", "subtract_checked",
                              pairwise_diff_checked_doc, registry);
}

}  // namespace arrow::compute::internal
