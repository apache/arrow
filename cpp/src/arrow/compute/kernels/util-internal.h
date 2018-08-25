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

#ifndef ARROW_COMPUTE_KERNELS_UTIL_INTERNAL_H
#define ARROW_COMPUTE_KERNELS_UTIL_INTERNAL_H

#include <memory>
#include <vector>

#include "arrow/compute/kernel.h"
#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {

class FunctionContext;

template <typename T>
inline const T* GetValues(const ArrayData& data, int i) {
  return reinterpret_cast<const T*>(data.buffers[i]->data()) + data.offset;
}

template <typename T>
inline T* GetMutableValues(const ArrayData* data, int i) {
  return reinterpret_cast<T*>(data->buffers[i]->mutable_data()) + data->offset;
}

static inline void CopyData(const ArrayData& input, ArrayData* output) {
  output->length = input.length;
  output->null_count = input.null_count;
  output->buffers = input.buffers;
  output->offset = input.offset;
  output->child_data = input.child_data;
}

namespace detail {

Status InvokeUnaryArrayKernel(FunctionContext* ctx, UnaryKernel* kernel,
                              const Datum& value, std::vector<Datum>* outputs);

Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right,
                               std::vector<Datum>* outputs);
Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right, Datum* output);

Datum WrapArraysLike(const Datum& value,
                     const std::vector<std::shared_ptr<Array>>& arrays);

Datum WrapDatumsLike(const Datum& value, const std::vector<Datum>& datums);

}  // namespace detail

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_UTIL_INTERNAL_H
