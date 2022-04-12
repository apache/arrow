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
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common.h"
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

  explicit CumulativeOptionsWrapper(OptionsType options) : OptionsWrapper<OptionsType>(std::move(options)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    auto options = static_cast<const OptionsType*>(args.options);
    if (!options) {
      return Status::Invalid(
          "Attempted to initialize KernelState from null FunctionOptions");
    }

    const auto& start = options->start;
    if (!start || !start->is_valid) {
      return Status::Invalid("Cumulative `start` option must be non-null and valid");
    }

    // Ensure `start` option matches input type
    if (!start->type->Equals(args.inputs[0].type)) {
      ARROW_ASSIGN_OR_RAISE(auto casted_start, Cast(Datum(start), args.inputs[0].type));
      auto new_options = OptionsType(casted_start.scalar(), options->skip_nulls);
      return ::arrow::internal::make_unique<State>(new_options);
    }
    return ::arrow::internal::make_unique<State>(*options);
  }
};

// The driver kernel for all cumulative compute functions. Op is a compute kernel
// representing any binary associative operation (add, product, min, max, etc.) and
// OptionsType the options type corresponding to Op. ArgType and OutType are the input
// and output types, which will normally be the same (e.g. the cumulative sum of an array
//  of Int64Type will result in an array of Int64Type).
template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct CumulativeGeneric {
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = CumulativeOptionsWrapper<OptionsType>::Get(ctx);
    auto start = UnboxScalar<OutType>::Unbox(*(options.start));
    auto skip_nulls = options.skip_nulls;

    int64_t base_output_offset = 0;
    bool encountered_null = false;
    ArrayData* out_arr = out->mutable_array();

    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        auto st = Call(ctx, base_output_offset, *batch[0].array(), out_arr, &start,
                       skip_nulls, &encountered_null);
        out_arr->SetNullCount(arrow::kUnknownNullCount);
        return st;
      }
      case Datum::CHUNKED_ARRAY: {
        const auto& input = batch[0].chunked_array();

        for (const auto& chunk : input->chunks()) {
          RETURN_NOT_OK(Call(ctx, base_output_offset, *chunk->data(), out_arr, &start,
                             skip_nulls, &encountered_null));
          base_output_offset += chunk->length();
        }
        out_arr->SetNullCount(arrow::kUnknownNullCount);
        return Status::OK();
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_<operator>': ",
            batch[0].ToString());
    }
  }

  static Status Call(KernelContext* ctx, int64_t base_output_offset,
                     const ArrayData& input, ArrayData* output, ArgValue* accumulator,
                     bool skip_nulls, bool* encountered_null) {
    Status st = Status::OK();
    ArgValue accumulator_tmp = *accumulator;
    bool encountered_null_tmp = *encountered_null;

    auto out_bitmap = output->GetMutableValues<uint8_t>(0);
    auto out_data = output->GetMutableValues<OutValue>(1) + base_output_offset;
    int64_t curr = base_output_offset;

    auto null_func = [&]() {
      *out_data++ = OutValue{};
      encountered_null_tmp = true;
      arrow::bit_util::SetBitTo(out_bitmap, curr, false);
      ++curr;
    };

    if (skip_nulls) {
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) {
            accumulator_tmp = Op::template Call<OutValue, ArgValue, ArgValue>(
                ctx, v, accumulator_tmp, &st);
            *out_data++ = accumulator_tmp;
            ++curr;
          },
          null_func);
    } else {
      auto start_null_idx = 0;
      VisitArrayValuesInline<ArgType>(
          input,
          [&](ArgValue v) {
            if (encountered_null_tmp) {
              *out_data++ = OutValue{};
            } else {
              accumulator_tmp = Op::template Call<OutValue, ArgValue, ArgValue>(
                  ctx, v, accumulator_tmp, &st);
              *out_data++ = accumulator_tmp;
              ++start_null_idx;
            }
            ++curr;
          },
          null_func);

      auto null_length = input.length - start_null_idx;
      arrow::bit_util::SetBitsTo(out_bitmap, base_output_offset + start_null_idx,
                                 null_length, false);
    }

    *accumulator = accumulator_tmp;
    *encountered_null = encountered_null_tmp;
    return st;
  }
};

const FunctionDoc cumulative_sum_doc{
    "Compute the cumulative sum over an array/chunked array of numbers",
    ("`values` must be an array/chunked array of numeric type values.\n"
     "Return an array/chunked array which is the cumulative sum computed\n"
     "over `values`. Results will wrap around on integer overflow.\n"
     "Use function \"cumulative_sum_checked\" if you want overflow\n"
     "to return an error."),
    {"values"},
    "CumulativeSumOptions"};

const FunctionDoc cumulative_sum_checked_doc{
    "Compute the cumulative sum over an array/chunked array of numbers",
    ("`values` must be an array/chunked array of numeric type values.\n"
     "Return an array/chunked array which is the cumulative sum computed\n"
     "over `values`. This function returns an error on overflow. For a\n"
     "variant that doesn't fail on overflow, use function \"cumulative_sum\"."),
    {"values"},
    "CumulativeSumOptions"};
}  // namespace

template <typename Op, typename OptionsType>
void MakeVectorCumulativeFunction(FunctionRegistry* registry, const std::string func_name,
                                  const FunctionDoc* doc) {
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func =
      std::make_shared<VectorFunction>(func_name, Arity::Unary(), doc, &kDefaultOptions);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());

  for (const auto& ty : types) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::INTERSECTION;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = KernelSignature::Make({InputType(ty)}, OutputType(ty));
    kernel.exec = ArithmeticExecFromOp<CumulativeGeneric, Op, OptionsType>(ty);
    kernel.init = CumulativeOptionsWrapper<OptionsType>::Init;
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  MakeVectorCumulativeFunction<Add, CumulativeSumOptions>(registry, "cumulative_sum",
                                                          &cumulative_sum_doc);
  MakeVectorCumulativeFunction<AddChecked, CumulativeSumOptions>(
      registry, "cumulative_sum_checked", &cumulative_sum_checked_doc);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
