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

#include "arrow/compute/kernels/boolean.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/util-internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

class InvertKernel : public UnaryKernel {
  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    ArrayData* result;

    if (out->kind() == Datum::NONE) {
      out->value = ArrayData::Make(boolean(), in_data.length);
    }

    result = out->array().get();

    // Allocate or copy bitmap
    result->null_count = in_data.null_count;
    std::shared_ptr<Buffer> validity_bitmap = in_data.buffers[0];
    if (in_data.offset != 0) {
      RETURN_NOT_OK(CopyBitmap(ctx->memory_pool(), validity_bitmap->data(),
                               in_data.offset, in_data.length, &validity_bitmap));
    }
    result->buffers.push_back(validity_bitmap);

    // Allocate output data buffer
    std::shared_ptr<Buffer> data_buffer;
    RETURN_NOT_OK(InvertBitmap(ctx->memory_pool(), in_data.buffers[1]->data(),
                               in_data.offset, in_data.length, &data_buffer));
    result->buffers.push_back(data_buffer);

    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }
};

Status Invert(FunctionContext* ctx, const Datum& value, Datum* out) {
  InvertKernel kernel;

  std::vector<Datum> result;
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, &kernel, value, &result));

  *out = detail::WrapDatumsLike(value, result);
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
