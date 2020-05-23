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

#include <algorithm>
#include <limits>

#include "arrow/builder.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {

struct TakeState : public KernelState {
  explicit TakeState(const TakeOptions& options) : options(options) {}
  TakeOptions options;
};

std::unique_ptr<KernelState> InitTake(KernelContext*, const KernelInitArgs& args) {
  // NOTE: TakeOptions are currently unused, but we pass it through anyway
  auto take_options = static_cast<const TakeOptions*>(args.options);
  return std::unique_ptr<KernelState>(new TakeState{*take_options});
}

template <typename ValueType, typename IndexType>
struct TakeFunctor {
  using ValueArrayType = typename TypeTraits<ValueType>::ArrayType;
  using IndexArrayType = typename TypeTraits<IndexType>::ArrayType;
  using IS = ArrayIndexSequence<IndexType>;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    ValueArrayType values(batch[0].array());
    IndexArrayType indices(batch[1].array());
    std::shared_ptr<Array> result;
    KERNEL_RETURN_IF_ERROR(ctx, Select(ctx, values, IS(indices), &result));
    out->value = result->data();
  }
};

struct TakeKernelVisitor {
  TakeKernelVisitor(const DataType& value_type, const DataType& index_type)
      : value_type(value_type), index_type(index_type) {}

  template <typename Type>
  Status Visit(const Type&) {
    this->result = codegen::Integer<TakeFunctor, Type>(index_type.id());
    return Status::OK();
  }

  Status Create() { return VisitTypeInline(value_type, this); }

  const DataType& value_type;
  const DataType& index_type;
  ArrayKernelExec result;
};

Status GetTakeKernel(const DataType& value_type, const DataType& index_type,
                     ArrayKernelExec* exec) {
  TakeKernelVisitor visitor(value_type, index_type);
  RETURN_NOT_OK(visitor.Create());
  *exec = visitor.result;
  return Status::OK();
}

void RegisterVectorTake(FunctionRegistry* registry) {
  VectorKernel base;
  base.init = InitTake;
  base.can_execute_chunkwise = false;

  auto take = std::make_shared<VectorFunction>("take", Arity::Binary());
  OutputType out_ty(FirstType);

  auto AddKernel = [&](InputType value_ty, const DataType& example_value_ty,
                       const std::shared_ptr<DataType>& index_ty) {
    base.signature =
        KernelSignature::Make({value_ty, InputType::Array(index_ty)}, out_ty);
    DCHECK_OK(GetTakeKernel(example_value_ty, *index_ty, &base.exec));
    DCHECK_OK(take->AddKernel(base));
  };

  for (const auto& value_ty : PrimitiveTypes()) {
    for (const auto& index_ty : IntTypes()) {
      AddKernel(InputType::Array(value_ty), *value_ty, index_ty);
    }
  }
  for (const auto& value_ty : ExampleParametricTypes()) {
    for (const auto& index_ty : IntTypes()) {
      AddKernel(InputType::Array(value_ty->id()), *value_ty, index_ty);
    }
  }
  DCHECK_OK(registry->AddFunction(std::move(take)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
