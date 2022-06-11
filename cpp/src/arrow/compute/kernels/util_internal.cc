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
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace internal {

const uint8_t* GetValidityBitmap(const ArrayData& data) {
  const uint8_t* bitmap = nullptr;
  if (data.buffers[0]) {
    bitmap = data.buffers[0]->data();
  }
  return bitmap;
}

int GetBitWidth(const DataType& type) {
  return checked_cast<const FixedWidthType&>(type).bit_width();
}

PrimitiveArg GetPrimitiveArg(const ArrayData& arr) {
  PrimitiveArg arg;
  arg.is_valid = GetValidityBitmap(arr);
  arg.data = arr.buffers[1]->data();
  arg.bit_width = GetBitWidth(*arr.type);
  arg.offset = arr.offset;
  arg.length = arr.length;
  if (arg.bit_width > 1) {
    arg.data += arr.offset * arg.bit_width / 8;
  }
  // This may be kUnknownNullCount
  arg.null_count = (arg.is_valid != nullptr) ? arr.null_count.load() : 0;
  return arg;
}

// TODO(wesm): ARROW-16577: this will be unneeded later
ArrayKernelExec TrivialScalarUnaryAsArraysExec(ArrayKernelExec exec, bool use_array_span,
                                               NullHandling::type null_handling) {
  return [=](KernelContext* ctx, const ExecSpan& span, ExecResult* out) -> Status {
    if (!out->is_scalar()) {
      return exec(ctx, span, out);
    }

    if (null_handling == NullHandling::INTERSECTION && !span[0].scalar->is_valid) {
      out->scalar()->is_valid = false;
      return Status::OK();
    }

    ExecSpan span_with_arrays;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> array_in,
                          MakeArrayFromScalar(*span[0].scalar, 1));
    span_with_arrays.length = 1;
    span_with_arrays.values = {ExecValue(*array_in->data())};

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> array_out,
                          MakeArrayFromScalar(*out->scalar(), 1));

    ExecResult array_result;

    // Send either ArraySpan or ArrayData depending on what modality the kernel
    // is expecting, which we have to specify manually for now
    if (!use_array_span) {
      array_result.value = array_out->data();
      RETURN_NOT_OK(exec(ctx, span_with_arrays, &array_result));
      ARROW_ASSIGN_OR_RAISE(out->value,
                            MakeArray(array_result.array_data())->GetScalar(0));
    } else {
      DCHECK(is_fixed_width(out->type()->id()));
      ArrayData* out_data = array_out->data().get();

      // the null count will be unknown after the kernel executes
      out_data->null_count = kUnknownNullCount;

      ArraySpan* span = array_result.array_span();

      // TODO(wesm): It isn't safe to write into the memory allocated by
      // MakeArrayFromScalar because MakeArrayOfNull reuses memory across
      // buffers. So to be able to write into an ArraySpan we need to allocate
      // some memory with the same structure as array_out
      //
      // Should probably implement a "make empty" array whose buffers are all
      // safe to modify
      if (out_data->buffers[0]) {
        ARROW_ASSIGN_OR_RAISE(out_data->buffers[0],
                              out_data->buffers[0]->CopySlice(0, 1));
      }
      ARROW_ASSIGN_OR_RAISE(out_data->buffers[1], out_data->buffers[1]->CopySlice(
                                                      0, out_data->buffers[1]->size()));
      span->SetMembers(*out_data);
      RETURN_NOT_OK(exec(ctx, span_with_arrays, &array_result));

      // XXX(wesm): have to rebox the array after mutating the buffers because
      // of the cached validity bitmap buffer
      ARROW_ASSIGN_OR_RAISE(out->value, MakeArray(array_out->data())->GetScalar(0));
    }
    return Status::OK();
  };
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
