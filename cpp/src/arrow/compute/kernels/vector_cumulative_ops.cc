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

#include <type_traits>
#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/result.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/visit_type_inline.h"

namespace arrow::compute::internal {

namespace {

template <typename OptionsType>
struct CumulativeOptionsWrapper : public OptionsWrapper<OptionsType> {
  using State = CumulativeOptionsWrapper<OptionsType>;

  explicit CumulativeOptionsWrapper(OptionsType options)
      : OptionsWrapper<OptionsType>(std::move(options)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    auto options = checked_cast<const OptionsType*>(args.options);
    if (!options) {
      return Status::Invalid(
          "Attempted to initialize KernelState from null FunctionOptions");
    }

    const auto& start = options->start;

    // Ensure `start` option, if given, matches input type
    if (start.has_value() && !start.value()->type->Equals(*args.inputs[0])) {
      ARROW_ASSIGN_OR_RAISE(auto casted_start,
                            Cast(Datum(start.value()), args.inputs[0],
                                 CastOptions::Safe(), ctx->exec_context()));
      auto new_options = OptionsType(casted_start.scalar(), options->skip_nulls);
      return std::make_unique<State>(new_options);
    }
    return std::make_unique<State>(*options);
  }
};

// The cumulative value is computed based on a simple arithmetic binary op
// such as Add, Mul, Min, Max, etc.
template <typename Op, typename ArgType>
struct CumulativeBinaryOp {
  using OutType = ArgType;
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  OutValue current_value;

  CumulativeBinaryOp() { current_value = Identity<Op>::template value<OutValue>; }

  explicit CumulativeBinaryOp(const std::shared_ptr<Scalar> start) {
    current_value = UnboxScalar<OutType>::Unbox(*start);
  }

  OutValue Call(KernelContext* ctx, ArgValue arg, Status* st) {
    current_value =
        Op::template Call<OutValue, ArgValue, ArgValue>(ctx, arg, current_value, st);
    return current_value;
  }
};

template <typename ArgType>
struct CumulativeMean {
  using OutType = DoubleType;
  using ArgValue = typename GetViewType<ArgType>::T;
  int64_t count = 0;
  double sum = 0;

  CumulativeMean() = default;

  // start value is ignored for CumulativeMean
  explicit CumulativeMean(const std::shared_ptr<Scalar> start) {}

  double Call(KernelContext* ctx, ArgValue arg, Status* st) {
    sum += static_cast<double>(arg);
    ++count;
    return sum / count;
  }
};

// The driver kernel for all cumulative compute functions.
// ArgType and OutType are the input and output types, which will
// normally be the same (e.g. the cumulative sum of an array of Int64Type will result in
// an array of Int64Type) with the exception of CumulativeMean, which will always return
// a double.
template <typename ArgType, typename CumulativeState>
struct Accumulator {
  using OutType = typename CumulativeState::OutType;
  using ArgValue = typename GetViewType<ArgType>::T;

  KernelContext* ctx;
  CumulativeState current_state;
  bool skip_nulls;
  bool encountered_null = false;
  NumericBuilder<OutType> builder;

  explicit Accumulator(KernelContext* ctx) : ctx(ctx), builder(ctx->memory_pool()) {}

  Status Accumulate(const ArraySpan& input) {
    Status st = Status::OK();

    if (skip_nulls || (input.GetNullCount() == 0 && !encountered_null)) {
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) { builder.UnsafeAppend(current_state.Call(ctx, v, &st)); },
          [&]() { builder.UnsafeAppendNull(); });
    } else {
      int64_t nulls_start_idx = 0;
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) {
            if (!encountered_null) {
              builder.UnsafeAppend(current_state.Call(ctx, v, &st));
              ++nulls_start_idx;
            }
          },
          [&]() { encountered_null = true; });

      RETURN_NOT_OK(builder.AppendNulls(input.length - nulls_start_idx));
    }

    return st;
  }
};

template <typename ArgType, typename CumulativeState, typename OptionsType>
struct CumulativeKernel {
  using OutType = typename CumulativeState::OutType;
  using OutValue = typename GetOutputType<OutType>::T;
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = CumulativeOptionsWrapper<OptionsType>::Get(ctx);
    Accumulator<ArgType, CumulativeState> accumulator(ctx);
    if (options.start.has_value()) {
      accumulator.current_state = CumulativeState(options.start.value());
    } else {
      accumulator.current_state = CumulativeState();
    }
    accumulator.skip_nulls = options.skip_nulls;

    RETURN_NOT_OK(accumulator.builder.Reserve(batch.length));
    RETURN_NOT_OK(accumulator.Accumulate(batch[0].array));

    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(accumulator.builder.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }
};

template <typename ArgType, typename CumulativeState, typename OptionsType>
struct CumulativeKernelChunked {
  using OutType = typename CumulativeState::OutType;
  using OutValue = typename GetOutputType<OutType>::T;
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = CumulativeOptionsWrapper<OptionsType>::Get(ctx);
    Accumulator<ArgType, CumulativeState> accumulator(ctx);
    if (options.start.has_value()) {
      accumulator.current_state = CumulativeState(options.start.value());
    } else {
      accumulator.current_state = CumulativeState();
    }
    accumulator.skip_nulls = options.skip_nulls;

    const ChunkedArray& chunked_input = *batch[0].chunked_array();
    RETURN_NOT_OK(accumulator.builder.Reserve(chunked_input.length()));
    std::vector<std::shared_ptr<Array>> out_chunks;
    for (const auto& chunk : chunked_input.chunks()) {
      RETURN_NOT_OK(accumulator.Accumulate(*chunk->data()));
    }
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(accumulator.builder.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }
};

const FunctionDoc cumulative_sum_doc{
    "Compute the cumulative sum over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative sum computed over `values`. Results will wrap around on\n"
     "integer overflow. Use function \"cumulative_sum_checked\" if you want\n"
     "overflow to return an error. The default start is 0."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_sum_checked_doc{
    "Compute the cumulative sum over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative sum computed over `values`. This function returns an error\n"
     "on overflow. For a variant that doesn't fail on overflow, use\n"
     "function \"cumulative_sum\". The default start is 0."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_prod_doc{
    "Compute the cumulative product over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative product computed over `values`. Results will wrap around on\n"
     "integer overflow. Use function \"cumulative_prod_checked\" if you want\n"
     "overflow to return an error. The default start is 1."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_prod_checked_doc{
    "Compute the cumulative product over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative product computed over `values`. This function returns an error\n"
     "on overflow. For a variant that doesn't fail on overflow, use\n"
     "function \"cumulative_prod\". The default start is 1."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_max_doc{
    "Compute the cumulative max over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative max computed over `values`. The default start is the minimum\n"
     "value of input type (so that any other value will replace the\n"
     "start as the new maximum)."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_min_doc{
    "Compute the cumulative min over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative min computed over `values`. The default start is the maximum\n"
     "value of input type (so that any other value will replace the\n"
     "start as the new minimum)."),
    {"values"},
    "CumulativeOptions"};

const FunctionDoc cumulative_mean_doc{
    "Compute the cumulative mean over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative mean computed over `values`. CumulativeOptions::start_value is \n"
     "ignored."),
    {"values"},
    "CumulativeOptions"};

// Kernel factory for complex stateful computations.
template <template <typename ArgType> typename State, typename OptionsType>
struct CumulativeStatefulKernelFactory {
  VectorKernel kernel;

  CumulativeStatefulKernelFactory() {
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    kernel.init = CumulativeOptionsWrapper<OptionsType>::Init;
  }

  template <typename Type>
  enable_if_number<Type, Status> Visit(const Type& type) {
    kernel.signature = KernelSignature::Make(
        {type.GetSharedPtr()},
        OutputType(TypeTraits<typename State<Type>::OutType>::type_singleton()));
    kernel.exec = CumulativeKernel<Type, State<Type>, OptionsType>::Exec;
    kernel.exec_chunked = CumulativeKernelChunked<Type, State<Type>, OptionsType>::Exec;
    return arrow::Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("Cumulative kernel not implemented for type ",
                                  type.ToString());
  }

  Result<VectorKernel> Make(const DataType& type) {
    RETURN_NOT_OK(VisitTypeInline(type, this));
    return kernel;
  }
};

template <template <typename ArgType> typename State, typename OptionsType>
void MakeVectorCumulativeStatefulFunction(FunctionRegistry* registry,
                                          const std::string func_name,
                                          const FunctionDoc doc) {
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func =
      std::make_shared<VectorFunction>(func_name, Arity::Unary(), doc, &kDefaultOptions);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());

  CumulativeStatefulKernelFactory<State, OptionsType> kernel_factory;
  for (const auto& ty : types) {
    auto kernel = kernel_factory.Make(*ty).ValueOrDie();
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// A kernel factory that forwards to CumulativeBinaryOp<Op, ...> for the given type.
// Need to use a struct because template-using declarations cannot appear in
// function scope.
template <typename Op, typename OptionsType>
struct MakeVectorCumulativeBinaryOpFunction {
  template <typename ArgType>
  using State = CumulativeBinaryOp<Op, ArgType>;

  static void Call(FunctionRegistry* registry, std::string func_name, FunctionDoc doc) {
    MakeVectorCumulativeStatefulFunction<State, OptionsType>(
        registry, std::move(func_name), std::move(doc));
  }
};

}  // namespace

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  MakeVectorCumulativeBinaryOpFunction<Add, CumulativeOptions>::Call(
      registry, "cumulative_sum", cumulative_sum_doc);
  MakeVectorCumulativeBinaryOpFunction<AddChecked, CumulativeOptions>::Call(
      registry, "cumulative_sum_checked", cumulative_sum_checked_doc);

  MakeVectorCumulativeBinaryOpFunction<Multiply, CumulativeOptions>::Call(
      registry, "cumulative_prod", cumulative_prod_doc);
  MakeVectorCumulativeBinaryOpFunction<MultiplyChecked, CumulativeOptions>::Call(
      registry, "cumulative_prod_checked", cumulative_prod_checked_doc);

  MakeVectorCumulativeBinaryOpFunction<Min, CumulativeOptions>::Call(
      registry, "cumulative_min", cumulative_min_doc);
  MakeVectorCumulativeBinaryOpFunction<Max, CumulativeOptions>::Call(
      registry, "cumulative_max", cumulative_max_doc);

  MakeVectorCumulativeStatefulFunction<CumulativeMean, CumulativeOptions>(
      registry, "cumulative_mean", cumulative_max_doc);
}

}  // namespace arrow::compute::internal
