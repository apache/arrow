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

#pragma once

#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"           // IWYU pragma: export
#include "arrow/compute/cast_internal.h"  // IWYU pragma: export
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// No-op functor for identity casts
template <typename O, typename I>
struct CastFunctor<
    O, I, enable_if_t<std::is_same<O, I>::value && is_parameter_free_type<I>::value>> {
  static Status Exec(KernelContext*, const ExecSpan&, ExecResult*) {
    return Status::OK();
  }
};

Status CastFromExtension(KernelContext* ctx, const ExecSpan& batch, ExecResult* out);

// Utility for numeric casts
void CastNumberToNumberUnsafe(Type::type in_type, Type::type out_type,
                              const ArraySpan& input, ArraySpan* out);

// ----------------------------------------------------------------------
// Dictionary to other things

Status UnpackDictionary(KernelContext* ctx, const ExecSpan& batch, ExecResult* out);

Status OutputAllNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out);

Status CastFromNull(KernelContext* ctx, const ExecSpan& batch, ExecResult* out);

// Adds a cast function where CastFunctor is specialized and the input and output
// types are parameter free (have a type_singleton).
template <typename InType, typename OutType>
void AddSimpleCast(InputType in_ty, OutputType out_ty, CastFunction* func) {
  DCHECK_OK(func->AddKernel(InType::type_id, {in_ty}, out_ty,
                            CastFunctor<OutType, InType>::Exec));
}

Status ZeroCopyCastExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out);

void AddZeroCopyCast(Type::type in_type_id, InputType in_type, OutputType out_type,
                     CastFunction* func);

// OutputType::Resolver that returns a type the type from CastOptions
Result<TypeHolder> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<TypeHolder>& args);

ARROW_EXPORT extern OutputType kOutputTargetType;

// Add generic casts to out_ty from:
// - the null type
// - dictionary with out_ty as given value type
// - extension types with a compatible storage type
void AddCommonCasts(Type::type out_type_id, OutputType out_ty, CastFunction* func);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
