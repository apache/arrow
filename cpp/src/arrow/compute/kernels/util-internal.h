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

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class FunctionContext;

// \brief Make a copy of the buffers into a destination array without carrying
// the type.
static inline void ZeroCopyData(const ArrayData& input, ArrayData* output) {
  output->length = input.length;
  output->null_count = input.null_count;
  output->buffers = input.buffers;
  output->offset = input.offset;
  output->child_data = input.child_data;
}

namespace detail {

/// \brief Invoke the kernel on value using the ctx and store results in outputs.
///
/// \param[in,out] ctx The function context to use when invoking the kernel.
/// \param[in,out] kernel The kernel to execute.
/// \param[in] value The input value to execute the kernel with.
/// \param[out] outputs One ArrayData datum for each ArrayData available in value.
ARROW_EXPORT
Status InvokeUnaryArrayKernel(FunctionContext* ctx, UnaryKernel* kernel,
                              const Datum& value, std::vector<Datum>* outputs);

ARROW_EXPORT
Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right,
                               std::vector<Datum>* outputs);
ARROW_EXPORT
Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right, Datum* output);

/// \brief Assign validity bitmap to output, copying bitmap if necessary, but
/// zero-copy otherwise, so that the same value slots are valid/not-null in the
/// output
/// (sliced arrays)
/// \param[in] ctx the kernel FunctionContext
/// \param[in] input the input array
/// \param[out] output the output array
ARROW_EXPORT
Status PropagateNulls(FunctionContext* ctx, const ArrayData& input, ArrayData* output);

ARROW_EXPORT
Datum WrapArraysLike(const Datum& value,
                     const std::vector<std::shared_ptr<Array>>& arrays);

ARROW_EXPORT
Datum WrapDatumsLike(const Datum& value, const std::vector<Datum>& datums);

/// \brief Kernel used to preallocate outputs for primitive types.
class PrimitiveAllocatingUnaryKernel : public UnaryKernel {
 public:
  PrimitiveAllocatingUnaryKernel(std::unique_ptr<UnaryKernel> delegate,
                                 const std::shared_ptr<DataType>& out_type);
  PrimitiveAllocatingUnaryKernel(UnaryKernel* delegate,
                                 const std::shared_ptr<DataType>& out_type);
  /// \brief Allocates ArrayData with the necessary data buffers allocated and
  /// then written into by the delegate kernel
  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override;

  std::shared_ptr<DataType> out_type() const override;

 private:
  UnaryKernel* delegate_;
  std::shared_ptr<DataType> out_type_;
  std::unique_ptr<UnaryKernel> owned_delegate_;
};

}  // namespace detail

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_UTIL_INTERNAL_H
