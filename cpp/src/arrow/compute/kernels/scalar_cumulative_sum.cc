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
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

template <typename Type>
struct CumulativeSumFunctor {
  Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    using CType = TypeTraits<Type>::CType;
    
    const ArrayData& array = *batch[0].array();
    auto data = array.GetValues<CType*>(1, array.offset);

    const NumericScalar& start = checked_cast<const NumericScalar&>(*batch[1].scalar());
    CType sum = start.value;

    ArrayData* output = out->array().get();
    CType* out_values = checked_cast<CType*>(output->buffers[1]->mutable_data());
    output->length = array.length;

    for(size_t i = array.offset; i < array.length; ++i) {
      sum += data[i];
      out_values[i] = sum;
    }
    
    return Status::OK();
  }

  static std::shared_ptr<KernelSignature> GetSignature(detail::GetTypeId get_id) {
    return KernelSignature::Make(
        {InputType::Array(get_id.id), InputType::Scalar(get_id.id), OutputType(FirstType));
  }
};

const FunctionDoc cumulative_sum_doc(
    "Compute the cumulative sum of an array",
    ("Return an array containing the result of the cumulative sum\n"
     "computed over the input array"),
    {"values", "starting sum"});

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {

  auto add_kernel = [&](detail::GetTypeId get_id, ArrayKernelExec exec) {
    ScalarKernel kernel;
    kernel.mem_allocation = MemAllocation::type::PREALLOCATE;
    kernel.signature = CumulativeSumFunctor<NumberType>::GetSignature(get_id.id);
    kernel.exec = std::move(exec);
    DCHECK_OK(func->AddKernel(std::move(kernel)));
  };

  auto cumulative_sum =
      std::make_shared<ScalarFunction>("cumulative_sum", Arity::Binary(), &cumulative_sum_doc);
  
  for(auto ty : NumericTypes()) {
    add_kernel(ty, GenerateTypeAgnosticPrimitive<CumulativeSumFunctor>(ty));
  }

  DCHECK_OK(registry->AddFunction(std::move(flatten)));
  
}

}

}  // namespace internal
}  // namespace compute
}  // namespace arrow