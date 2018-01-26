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

#include "arrow/compute/kernels/util-internal.h"

#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

namespace arrow {
namespace compute {
namespace detail {

Status InvokeUnaryArrayKernel(FunctionContext* ctx, UnaryKernel* kernel,
                              const Datum& value, std::vector<Datum>* outputs) {
  if (value.kind() == Datum::ARRAY) {
    Datum output;
    RETURN_NOT_OK(kernel->Call(ctx, value, &output));
    outputs->push_back(output);
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    const ChunkedArray& array = *value.chunked_array();
    for (int i = 0; i < array.num_chunks(); i++) {
      Datum output;
      RETURN_NOT_OK(kernel->Call(ctx, Datum(array.chunk(i)), &output));
      outputs->push_back(output);
    }
  } else {
    return Status::Invalid("Input Datum was not array-like");
  }
  return Status::OK();
}

Datum WrapArraysLike(const Datum& value,
                     const std::vector<std::shared_ptr<Array>>& arrays) {
  // Create right kind of datum
  if (value.kind() == Datum::ARRAY) {
    return Datum(arrays[0]->data());
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    return Datum(std::make_shared<ChunkedArray>(arrays));
  } else {
    DCHECK(false) << "unhandled datum kind";
    return Datum();
  }
}

Datum WrapDatumsLike(const Datum& value, const std::vector<Datum>& datums) {
  // Create right kind of datum
  if (value.kind() == Datum::ARRAY) {
    DCHECK_EQ(1, datums.size());
    return Datum(datums[0].array());
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    std::vector<std::shared_ptr<Array>> arrays;
    for (const Datum& datum : datums) {
      DCHECK_EQ(Datum::ARRAY, datum.kind());
      arrays.emplace_back(MakeArray(datum.array()));
    }
    return Datum(std::make_shared<ChunkedArray>(arrays));
  } else {
    DCHECK(false) << "unhandled datum kind";
    return Datum();
  }
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow
