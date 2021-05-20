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

#include <arrow/compute/api.h>
#include <arrow/util/logging.h>

#include "codegen_internal.h"

namespace arrow {
namespace compute {

namespace {

template <typename Type, bool swap = false, typename Enable = void>
struct IfElseFunctor {
  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const ArrayData& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& cond, const ArrayData& left,
                     const Scalar& right, ArrayData* out) {
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const Scalar& cond, const Scalar& left,
                     const Scalar& right, Scalar* out) {
    return Status::OK();
  }
};

template <typename Type>
struct ResolveExec {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    if (batch.length == 0) return Status::OK();

    if (batch[0].kind() == Datum::ARRAY) {
      if (batch[1].kind() == Datum::ARRAY) {
        if (batch[2].kind() == Datum::ARRAY) {  // AAA
          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                           *batch[2].array(), out->mutable_array());
        } else {  // AAS
          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(), *batch[1].array(),
                                           *batch[2].scalar(), out->mutable_array());
        }
      } else {
        return Status::Invalid("");
        //        if (batch[2].kind() == Datum::ARRAY) {  // ASA
        //          return IfElseFunctor<Type, true>::Call(ctx, *batch[0].array(),
        //                                                 *batch[2].array(),
        //                                                 *batch[1].scalar(),
        //                                                 out->mutable_array());
        //        } else {  // ASS
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].array(),
        //          *batch[1].scalar(),
        //                                           *batch[2].scalar(),
        //                                           out->mutable_array());
        //        }
      }
    } else {  // when cond is scalar, output will also be scalar
      if (batch[1].kind() == Datum::ARRAY) {
        return Status::Invalid("");
        //        if (batch[2].kind() == Datum::ARRAY) {  // SAA
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
        //          *batch[1].array(),
        //                                           *batch[2].array(),
        //                                           out->scalar().get());
        //        } else {  // SAS
        //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
        //          *batch[1].array(),
        //                                           *batch[2].scalar(),
        //                                           out->scalar().get());
        //        }
      } else {
        if (batch[2].kind() == Datum::ARRAY) {  // SSA
          return Status::Invalid("");
          //          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(),
          //          *batch[1].scalar(),
          //                                           *batch[2].array(),
          //                                           out->scalar().get());
        } else {  // SSS
          return IfElseFunctor<Type>::Call(ctx, *batch[0].scalar(), *batch[1].scalar(),
                                           *batch[2].scalar(), out->scalar().get());
        }
      }
    }
  }
};

void AddPrimitiveKernels(const std::shared_ptr<ScalarFunction>& scalar_function,
                         const std::vector<std::shared_ptr<DataType>>& types) {
  for (auto&& type : types) {
    auto exec = internal::GenerateTypeAgnosticPrimitive<ResolveExec>(*type);
    ScalarKernel kernel({boolean(), type, type}, type, exec);
    kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

    DCHECK_OK(scalar_function->AddKernel(std::move(kernel)));
  }
}

}  // namespace

const FunctionDoc if_else_doc{"<fill this>", ("`<fill this>"), {"cond", "left", "right"}};

namespace internal {

void RegisterScalarIfElse(FunctionRegistry* registry) {
  ScalarKernel scalar_kernel;
  scalar_kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  scalar_kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

  auto func = std::make_shared<ScalarFunction>("if_else", Arity::Ternary(), &if_else_doc);

  AddPrimitiveKernels(func, NumericTypes());
  // todo add temporal, boolean, null and binary kernels

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow