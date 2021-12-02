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

ArrayKernelExec TrivialScalarUnaryAsArraysExec(ArrayKernelExec exec,
                                               NullHandling::type null_handling) {
  return [=](KernelContext* ctx, const ExecBatch& batch, Datum* out) -> Status {
    if (out->is_array()) {
      return exec(ctx, batch, out);
    }

    if (null_handling == NullHandling::INTERSECTION && !batch[0].scalar()->is_valid) {
      out->scalar()->is_valid = false;
      return Status::OK();
    }

    ARROW_ASSIGN_OR_RAISE(Datum array_in, MakeArrayFromScalar(*batch[0].scalar(), 1));
    ARROW_ASSIGN_OR_RAISE(Datum array_out, MakeArrayFromScalar(*out->scalar(), 1));
    RETURN_NOT_OK(exec(ctx, ExecBatch{{std::move(array_in)}, 1}, &array_out));
    ARROW_ASSIGN_OR_RAISE(*out, array_out.make_array()->GetScalar(0));
    return Status::OK();
  };
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
