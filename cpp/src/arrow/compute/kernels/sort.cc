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

#include "arrow/compute/kernels/sort.h"
#include <algorithm>
#include <numeric>
#include <vector>
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/compute/expression.h"
#include "arrow/compute/logical_type.h"
#include "arrow/type_traits.h"
namespace arrow {

class Array;

namespace compute {

template <typename ArrowType>
class SortKernelImpl : public SortKernel {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using BuilderType = typename TypeTraits<ArrowType>::BuilderType;
  using CType = typename TypeTraits<ArrowType>::CType;

 private:
  Status SortImpl(FunctionContext* ctx, const std::shared_ptr<ArrayType>& values,
                  std::shared_ptr<Array>* offsets) {
    std::vector<uint64_t> ind(values->length());
    std::iota(ind.begin(), ind.end(), 0);
    auto nulls_begin = ind.end();

    if (values->null_count()) {
      nulls_begin = std::stable_partition(ind.begin(), ind.end(),
                                        [&values](uint64_t ind) { return !values->IsNull(ind); });
    }
    std::stable_sort(ind.begin(), nulls_begin, [&values](uint64_t left, uint64_t right) {
      return values->Value(left) < values->Value(right);
    });
    UInt64Builder builder(ctx->memory_pool());
    builder.AppendValues(ind.begin(), ind.end());
    return builder.Finish(offsets);
  }

 public:
  explicit SortKernelImpl(const std::shared_ptr<DataType>& type) : SortKernel(type) {}

  Status Sort(FunctionContext* ctx, const std::shared_ptr<Array>& values,
              std::shared_ptr<Array>* offsets) {
    return SortImpl(ctx, std::static_pointer_cast<ArrayType>(values), offsets);
  }

  Status Call(FunctionContext* ctx, const Datum& values, Datum* offsets) {
    if (!values.is_array()) {
      return Status::Invalid("SortKernel expects array values");
    }
    auto values_array = values.make_array();
    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(this->Sort(ctx, values_array, &offsets_array));
    *offsets = offsets_array;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return type_; }
};

Status SortKernel::Make(const std::shared_ptr<DataType>& value_type,
                        std::unique_ptr<SortKernel>* out) {
  SortKernel* kernel;
  switch (value_type->id()) {
    case Type::UINT8:
      kernel = new SortKernelImpl<UInt8Type>(value_type);
      break;
    case Type::INT8:
      kernel = new SortKernelImpl<Int8Type>(value_type);
      break;
    case Type::UINT16:
      kernel = new SortKernelImpl<UInt16Type>(value_type);
      break;
    case Type::INT16:
      kernel = new SortKernelImpl<Int16Type>(value_type);
      break;
    case Type::UINT32:
      kernel = new SortKernelImpl<UInt32Type>(value_type);
      break;
    case Type::INT32:
      kernel = new SortKernelImpl<Int32Type>(value_type);
      break;
    case Type::UINT64:
      kernel = new SortKernelImpl<UInt64Type>(value_type);
      break;
    case Type::INT64:
      kernel = new SortKernelImpl<Int64Type>(value_type);
      break;
    default:
      return Status::Invalid("Use of unsaported array type in SortKernel");
  }
  out->reset(kernel);
  return Status::OK();
}

Status Sort(FunctionContext* ctx, const Array& values, std::shared_ptr<Array>* offsets) {
  Datum offsets_datum;
  RETURN_NOT_OK(Sort(ctx, Datum(values.data()), &offsets_datum));
  *offsets = offsets_datum.make_array();
  return Status::OK();
}

Status Sort(FunctionContext* ctx, const Datum& values, Datum* offsets) {
  std::unique_ptr<SortKernel> kernel;
  RETURN_NOT_OK(SortKernel::Make(values.type(), &kernel));
  return kernel->Call(ctx, values, offsets);
}

}  // namespace compute
}  // namespace arrow
