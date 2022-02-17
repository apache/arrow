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

Status CumulativeSum(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const ArrayData& array = *batch[0].array();
  ArrayData* output = out->array().get();
  output->length = array.length;
  
  return Status::OK();
}

void RegisterVectorCumulativeSum(FunctionRegistry* registry) {

}

}

}  // namespace internal
}  // namespace compute
}  // namespace arrow