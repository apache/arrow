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

#include <cstdint>
#include <string>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"

namespace arrow {
namespace compute {
namespace internal {

struct SelectionKernelData {
  InputType input;
  ArrayKernelExec exec;
};

void RegisterSelectionFunction(const std::string& name, FunctionDoc doc,
                               VectorKernel base_kernel, InputType selection_type,
                               const std::vector<SelectionKernelData>& kernels,
                               const FunctionOptions* default_options,
                               FunctionRegistry* registry);

/// \brief Allocate an ArrayData for a primitive array with a given length and bit width
///
/// \param[in] bit_width 1 or a multiple of 8
Status PreallocatePrimitiveArrayData(KernelContext* ctx, int64_t length, int bit_width,
                                     bool allocate_validity, ArrayData* out);

Status FSBFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status ListFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status LargeListFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status FSLFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status DenseUnionFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status MapFilterExec(KernelContext*, const ExecSpan&, ExecResult*);

Status VarBinaryTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status LargeVarBinaryTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status FSBTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status ListTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status LargeListTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status FSLTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status DenseUnionTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status StructTakeExec(KernelContext*, const ExecSpan&, ExecResult*);
Status MapTakeExec(KernelContext*, const ExecSpan&, ExecResult*);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
