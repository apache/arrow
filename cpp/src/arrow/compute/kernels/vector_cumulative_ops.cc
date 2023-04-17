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

#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/result.h"
#include "arrow/util/bit_util.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {

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
    if (!start || !start->is_valid) {
      return Status::Invalid("Cumulative `start` option must be non-null and valid");
    }

    // Ensure `start` option matches input type
    if (!start->type->Equals(*args.inputs[0])) {
      ARROW_ASSIGN_OR_RAISE(
          auto casted_start,
          Cast(Datum(start), args.inputs[0], CastOptions::Safe(), ctx->exec_context()));
      auto new_options = OptionsType(casted_start.scalar(), options->skip_nulls);
      return std::make_unique<State>(new_options);
    }
    return std::make_unique<State>(*options);
  }
};

// The driver kernel for all cumulative compute functions. Op is a compute kernel
// representing any binary associative operation (add, product, min, max, etc.) and
// OptionsType the options type corresponding to Op. ArgType and OutType are the input
// and output types, which will normally be the same (e.g. the cumulative sum of an array
// of Int64Type will result in an array of Int64Type).
template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct Accumulator {
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  KernelContext* ctx;
  ArgValue current_value;
  bool skip_nulls;
  bool encountered_null = false;
  NumericBuilder<OutType> builder;

  explicit Accumulator(KernelContext* ctx) : ctx(ctx), builder(ctx->memory_pool()) {}

  Status Accumulate(const ArraySpan& input) {
    Status st = Status::OK();

    if (skip_nulls || (input.GetNullCount() == 0 && !encountered_null)) {
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) {
            current_value = Op::template Call<OutValue, ArgValue, ArgValue>(
                ctx, v, current_value, &st);
            builder.UnsafeAppend(current_value);
          },
          [&]() { builder.UnsafeAppendNull(); });
    } else {
      int64_t nulls_start_idx = 0;
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) {
            if (!encountered_null) {
              current_value = Op::template Call<OutValue, ArgValue, ArgValue>(
                  ctx, v, current_value, &st);
              builder.UnsafeAppend(current_value);
              ++nulls_start_idx;
            }
          },
          [&]() { encountered_null = true; });

      RETURN_NOT_OK(builder.AppendNulls(input.length - nulls_start_idx));
    }

    return st;
  }
};

template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct CumulativeKernel {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& options = CumulativeOptionsWrapper<OptionsType>::Get(ctx);
    Accumulator<OutType, ArgType, Op, OptionsType> accumulator(ctx);
    accumulator.current_value = UnboxScalar<OutType>::Unbox(*(options.start));
    accumulator.skip_nulls = options.skip_nulls;

    RETURN_NOT_OK(accumulator.builder.Reserve(batch.length));
    RETURN_NOT_OK(accumulator.Accumulate(batch[0].array));

    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(accumulator.builder.FinishInternal(&result));
    out->value = std::move(result);
    return Status::OK();
  }
};

template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct CumulativeKernelChunked {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = CumulativeOptionsWrapper<OptionsType>::Get(ctx);
    Accumulator<OutType, ArgType, Op, OptionsType> accumulator(ctx);
    accumulator.current_value = UnboxScalar<OutType>::Unbox(*(options.start));
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
     "overflow to return an error."),
    {"values"},
    "CumulativeSumOptions"};

const FunctionDoc cumulative_sum_checked_doc{
    "Compute the cumulative sum over a numeric input",
    ("`values` must be numeric. Return an array/chunked array which is the\n"
     "cumulative sum computed over `values`. This function returns an error\n"
     "on overflow. For a variant that doesn't fail on overflow, use\n"
     "function \"cumulative_sum\"."),
    {"values"},
    "CumulativeSumOptions"};
}  // namespace

template <typename Op, typename OptionsType>
void MakeVectorCumulativeFunction(FunctionRegistry* registry, const std::string func_name,
                                  const FunctionDoc doc) {
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func =
      std::make_shared<VectorFunction>(func_name, Arity::Unary(), doc, &kDefaultOptions);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());

  for (const auto& ty : types) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::type::NO_PREALLOCATE;
    kernel.signature = KernelSignature::Make({ty}, OutputType(ty));
    kernel.exec =
        ArithmeticExecFromOp<CumulativeKernel, Op, ArrayKernelExec, OptionsType>(ty);
    kernel.exec_chunked =
        ArithmeticExecFromOp<CumulativeKernelChunked, Op, VectorKernel::ChunkedExec,
                             OptionsType>(ty);
    kernel.init = CumulativeOptionsWrapper<OptionsType>::Init;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  MakeVectorCumulativeFunction<Add, CumulativeSumOptions>(registry, "cumulative_sum",
                                                          cumulative_sum_doc);
  MakeVectorCumulativeFunction<AddChecked, CumulativeSumOptions>(
      registry, "cumulative_sum_checked", cumulative_sum_checked_doc);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
