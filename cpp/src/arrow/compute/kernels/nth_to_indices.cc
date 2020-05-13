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

#include "arrow/compute/kernels/nth_to_indices.h"

#include <algorithm>
#include <utility>

#include "arrow/builder.h"
#include "arrow/compute/context.h"

namespace arrow {

class Array;

namespace compute {

struct NthToIndicesKernel : public OpKernel {
  virtual Status Call(FunctionContext* ctx, const Datum& values, int64_t n,
                      Datum* offsets) = 0;
  static std::shared_ptr<NthToIndicesKernel> Make(const DataType& value_type);
};

template <typename ArrowType>
class NthToIndicesKernelImpl final : public NthToIndicesKernel {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  Status Call(FunctionContext* ctx, const Datum& values, int64_t n,
              Datum* offsets) override {
    if (!values.is_array()) {
      return Status::Invalid("NthToIndicesKernel expects array values");
    }
    auto values_array = std::static_pointer_cast<ArrayType>(values.make_array());
    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(NthToIndices(ctx, values_array, n, &offsets_array));
    *offsets = offsets_array;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return type_; }

 private:
  std::shared_ptr<DataType> type_;

  Status NthToIndices(FunctionContext* ctx, const std::shared_ptr<ArrayType>& values,
                      int64_t n, std::shared_ptr<Array>* offsets) {
    if (n > values->length()) {
      return Status::IndexError("NthToIndices index out of bound");
    }

    int64_t buf_size = values->length() * sizeof(uint64_t);
    ARROW_ASSIGN_OR_RAISE(auto indices_buf, AllocateBuffer(buf_size, ctx->memory_pool()));

    int64_t* indices_begin = reinterpret_cast<int64_t*>(indices_buf->mutable_data());
    int64_t* indices_end = indices_begin + values->length();

    std::iota(indices_begin, indices_end, 0);
    *offsets = std::make_shared<UInt64Array>(values->length(), std::move(indices_buf));

    if (n == values->length()) {
      return Status::OK();
    }

    auto nulls_begin = indices_end;
    if (values->null_count()) {
      nulls_begin =
          std::stable_partition(indices_begin, indices_end,
                                [&values](uint64_t ind) { return !values->IsNull(ind); });
    }

    auto nth_begin = indices_begin + n;
    if (nth_begin < nulls_begin) {
      std::nth_element(indices_begin, nth_begin, nulls_begin,
                       [&values](uint64_t left, uint64_t right) {
                         return values->GetView(left) < values->GetView(right);
                       });
    }
    return Status::OK();
  }
};

#define NTH_MAKE_CASE(T) \
  case T::type_id:       \
    return std::shared_ptr<NthToIndicesKernel>(new NthToIndicesKernelImpl<T>());

std::shared_ptr<NthToIndicesKernel> NthToIndicesKernel::Make(const DataType& value_type) {
  switch (value_type.id()) {
    NTH_MAKE_CASE(UInt8Type);
    NTH_MAKE_CASE(Int8Type);
    NTH_MAKE_CASE(UInt16Type);
    NTH_MAKE_CASE(Int16Type);
    NTH_MAKE_CASE(UInt32Type);
    NTH_MAKE_CASE(Int32Type);
    NTH_MAKE_CASE(UInt64Type);
    NTH_MAKE_CASE(Int64Type);
    NTH_MAKE_CASE(FloatType);
    NTH_MAKE_CASE(DoubleType);
    NTH_MAKE_CASE(BinaryType);
    NTH_MAKE_CASE(StringType);
    default:
      return NULLPTR;
  }
}

static Status NthToIndices(FunctionContext* ctx, const Datum& values, int64_t n,
                           Datum* offsets) {
  auto type = values.type();
  ARROW_CHECK_NE(type, NULLPTR);
  auto kernel = NthToIndicesKernel::Make(*type);
  if (!kernel) {
    return Status::TypeError("No NthToIndices kernel for this type ", *type);
  }
  return kernel->Call(ctx, values, n, offsets);
}

Status NthToIndices(FunctionContext* ctx, const Array& values, int64_t n,
                    std::shared_ptr<Array>* offsets) {
  Datum offsets_datum;
  RETURN_NOT_OK(NthToIndices(ctx, Datum(values.data()), n, &offsets_datum));
  *offsets = offsets_datum.make_array();
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
