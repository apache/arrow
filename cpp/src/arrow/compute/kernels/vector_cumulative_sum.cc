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
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/result.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {

std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
  return KernelSignature::Make({InputType::Array(get_id.id)}, OutputType(FirstType));
}

}  // namespace

template <typename OutType, typename ArgType, typename Op>
struct CumulativeMeta {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = OptionsWrapper<CumulativeSumOptions>::Get(ctx);
    auto start = UnboxScalar<ArgType>::Unbox(*options.start);
    bool skip_nulls = options.skip_nulls;

    switch (batch[0].kind()) {
      case Datum::ARRAY: {
        return Op::template Call(ctx, *batch[0].array(), start, out);
      }
      case Datum::CHUNKED_ARRAY: {
        const auto& input = batch[0].chunked_array();

        for (const auto& chunk : input->chunks()) {
          RETURN_NOT_OK(Op::template Call(ctx, chunk->data(), start, out));
        }
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'cumulative_<operator>': ",
            batch[0].ToString());
    }

    return Status::OK();
  }
};

template <typename OutType, typename ArgType>
struct CumulativeSum {

  using OutValue = typename GetOutputType<OutType>::T;
  using ArgValue = typename GetViewType<ArgType>::T;

  static Status Call(KernelContext* ctx, const ArrayData& arg0, const ArgValue& sum,
                            Datum* out) {
    Status st = Status::OK();
    ArrayIterator<ArgType> arg0_it(arg0);
    RETURN_NOT_OK(OutputAdapter<OutType>::Write(ctx, out, [&]() -> OutValue {
      sum = Add::template Call<OutValue, ArgValue, ArgValue>(ctx, arg0_it(), sum,
                                                               &st);
      return sum;
    }));
    return st;
  }
};

const FunctionDoc cumulative_sum_doc(
    "Compute the cumulative sum over an array of numbers",
    ("`values` must be an array of numeric type values.\n"
     "`start` is a single value of the same type.\n"
     "Return an array which is the cumulative sum computed over `values.`\n"
     "Null entries remain in place but are not used in calucating sum.\n"
     "`start` is an optional starting sum of computation."),
    {"values", "start"});

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {
  auto cumulative_sum = std::make_shared<VectorFunction>(
      "cumulative_sum", Arity::Unary(), &cumulative_sum_doc);

  std::vector<std::shared_ptr<DataType>> types;
  types.insert(types.end(), NumericTypes().begin(), NumericTypes().end());

  for (const auto& ty : NumericTypes()) {
    VectorKernel kernel;
    kernel.can_execute_chunkwise = false;
    kernel.null_handling = NullHandling::type::INTERSECTION;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = KernelSignature::Make({InputType(ty)}, OutputType(ty));
    kernel.exec = std::move(ArithmeticExecFromOp<CumulativeMeta, CumulativeSum>(ty));
    kernel.init = OptionsWrapper<CumulativeSumOptions>::Init;
    DCHECK_OK(cumulative_sum->AddKernel(std::move(kernel)));
  }

  DCHECK_OK(registry->AddFunction(std::move(cumulative_sum)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
