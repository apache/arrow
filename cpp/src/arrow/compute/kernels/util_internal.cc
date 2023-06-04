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

#include "arrow/compute/kernels/util_internal.h"

#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/compute/function.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

namespace {

Status NullToNullExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return Status::OK();
}

}  // namespace

ExecValue GetExecValue(const Datum& value) {
  ExecValue result;
  if (value.is_array()) {
    result.SetArray(*value.array());
  } else {
    result.SetScalar(value.scalar().get());
  }
  return result;
}

int64_t GetTrueCount(const ArraySpan& mask) {
  if (mask.buffers[0].data != nullptr) {
    return CountAndSetBits(mask.buffers[0].data, mask.offset, mask.buffers[1].data,
                           mask.offset, mask.length);
  } else {
    return CountSetBits(mask.buffers[1].data, mask.offset, mask.length);
  }
}

void AddNullExec(ScalarFunction* func) {
  std::vector<InputType> input_types(func->arity().num_args, InputType(Type::NA));
  DCHECK_OK(func->AddKernel(std::move(input_types), OutputType(null()), NullToNullExec));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
