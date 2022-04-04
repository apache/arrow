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

// NOTE: Missing description of this class
template <typename OutType, typename ArgType, typename Op, typename OptionsType>
struct CumulativeGeneric {
  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = OptionsWrapper<OptionsType>::Get(ctx);
    auto skip_nulls = options.skip_nulls;

    // Cast `start` option to match output type
    // NOTE: Is at least one descriptor guaranteed? If not, need to check size
    // before indexing.
    auto out_type = batch.GetDescriptors()[0].type;
    ARROW_ASSIGN_OR_RAISE(auto cast_value, Cast(Datum(options.start), out_type));
    auto start = UnboxScalar<OutType>::Unbox(*cast_value.scalar());

    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        return Call(ctx, *batch[0].array(), start, out, skip_nulls);
      }
      case Datum::CHUNKED_ARRAY: {
        const auto& input = batch[0].chunked_array();

        for (const auto& chunk : input->chunks()) {
          RETURN_NOT_OK(Call(ctx, *chunk->data(), start, out, skip_nulls));
        }
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_<operator>': ",
            batch[0].ToString());
    }

    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& arg0, ArgValue& partial_scan,
                     Datum* out, bool skip_nulls) {
    Status st = Status::OK();
    ArrayData* out_arr = out->mutable_array();
    auto out_data = out_arr->GetMutableValues<OutValue>(1);

    bool encounted_null = false;
    auto start_null_idx = arg0.offset;

    VisitArrayValuesInline<ArgType>(
        arg0,
        [&](ArgValue v) {
          if (!skip_nulls && encounted_null) {
            *out_data++ = OutValue{};
          } else {
            partial_scan = Op::template Call<OutValue, ArgValue, ArgValue>(
                ctx, v, partial_scan, &st);
            *out_data++ = partial_scan;
            ++start_null_idx;
          }
        },
        [&]() {
          // null
          *out_data++ = OutValue{};
          encounted_null = true;
        });

    if (!skip_nulls) {
      auto out_bitmap = out_arr->GetMutableValues<uint8_t>(0);
      auto null_length = arg0.length - (start_null_idx - arg0.offset);
      out_arr->SetNullCount(null_length);
      arrow::bit_util::SetBitsTo(out_bitmap, start_null_idx, null_length, false);
    }

    return st;
  }
};

const FunctionDoc cumulative_sum_doc{
    "Compute the cumulative sum over an array of numbers",
    ("`values` must be an array of numeric type values.\n"
     "Return an array which is the cumulative sum computed over `values`.\n"
     "Results will wrap around on integer overflow.\n"
     "Use function \"cumulative_sum_checked\" if you want overflow\n"
     "to return an error."),
    {"values"},
    "CumulativeSumOptions"};

const FunctionDoc cumulative_sum_checked_doc{
    "Compute the cumulative sum over an array of numbers",
    ("`values` must be an array of numeric type values.\n"
     "Return an array which is the cumulative sum computed over `values`.\n"
     "This function returns an error on overflow. For a variant that\n"
     "doesn't fail on overflow, use function \"cumulative_sum\"."),
    {"values"},
    "CumulativeSumOptions"};

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
    kernel.init = OptionsWrapper<OptionsType>::Init;
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
