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

#ifndef ARROW_COMPUTE_KERNELS_TAKE_H
#define ARROW_COMPUTE_KERNELS_TAKE_H

#include <memory>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

#include "arrow/compute/kernel.h"

namespace arrow {

class Array;
class ChunkedArray;
class Column;
class DataType;

namespace compute {

struct ARROW_EXPORT TakeOptions {
  enum OutOfBoundMode { RAISE, WRAP, CLIP };
  TakeOptions() : mode(RAISE) {}

  OutOfBoundMode mode;
};

// Integer array indices
ARROW_EXPORT
Status Take(FunctionContext* context, const Datum& in, const Array& indices,
            const TakeOptions& options, Datum* out);

ARROW_EXPORT
Status Take(FunctionContext* context, const Array& in, const Array& indices,
            const TakeOptions& options, std::shared_ptr<Array>* out);

// Single integer index
ARROW_EXPORT
Status Take(FunctionContext* context, const Datum& in, int64_t indices,
            const TakeOptions& options, Datum* out);

ARROW_EXPORT
Status Take(FunctionContext* context, const Array& in, int64_t indices,
            const TakeOptions& options, std::shared_ptr<Array>* out);

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_KERNELS_TAKE_H
