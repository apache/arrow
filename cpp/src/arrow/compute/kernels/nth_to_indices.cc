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

/// \brief Kernel implementing NthToIndices operation
class ARROW_EXPORT NthToIndicesKernel : public OpKernel {
 protected:
  std::shared_ptr<DataType> type_;

 public:
  /// \brief Kernel interface
  ///
  /// delegates to subclasses via NthToIndices()
  virtual Status Call(FunctionContext* ctx, const Datum& values, uint64_t n,
                      Datum* offsets) = 0;

  /// \brief output type of this kernel
  std::shared_ptr<DataType> out_type() const override { return uint64(); }

  /// \brief single-array implementation
  virtual Status NthToIndices(FunctionContext* ctx, const std::shared_ptr<Array>& values,
                              uint64_t n, std::shared_ptr<Array>* offsets) = 0;

  /// \brief factory for NthToIndicesKernel
  ///
  /// \param[in] value_type constructed NthToIndicesKernel will support
  ///            values of this type
  /// \param[out] out created kernel
  static Status Make(const std::shared_ptr<DataType>& value_type,
                     std::unique_ptr<NthToIndicesKernel>* out);
};

template <typename ArrayType>
bool CompareValues(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
  return array.Value(lhs) < array.Value(rhs);
}

template <typename ArrayType>
bool CompareViews(const ArrayType& array, uint64_t lhs, uint64_t rhs) {
  return array.GetView(lhs) < array.GetView(rhs);
}

template <typename ArrowType, typename Comparator>
class NthToIndicesKernelImpl : public NthToIndicesKernel {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

 public:
  explicit NthToIndicesKernelImpl(Comparator compare) : compare_(compare) {}

  Status NthToIndices(FunctionContext* ctx, const std::shared_ptr<Array>& values,
                      uint64_t n, std::shared_ptr<Array>* offsets) {
    return NthToIndicesImpl(ctx, std::static_pointer_cast<ArrayType>(values), n, offsets);
  }

  Status Call(FunctionContext* ctx, const Datum& values, uint64_t n, Datum* offsets) {
    if (!values.is_array()) {
      return Status::Invalid("NthToIndicesKernel expects array values");
    }
    auto values_array = values.make_array();
    std::shared_ptr<Array> offsets_array;
    RETURN_NOT_OK(this->NthToIndices(ctx, values_array, n, &offsets_array));
    *offsets = offsets_array;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const { return type_; }

 private:
  Comparator compare_;

  Status NthToIndicesImpl(FunctionContext* ctx, const std::shared_ptr<ArrayType>& values,
                          uint64_t n, std::shared_ptr<Array>* offsets) {
    std::shared_ptr<Buffer> indices_buf;
    int64_t buf_size = values->length() * sizeof(uint64_t);
    RETURN_NOT_OK(AllocateBuffer(ctx->memory_pool(), buf_size, &indices_buf));

    int64_t* indices_begin = reinterpret_cast<int64_t*>(indices_buf->mutable_data());
    int64_t* indices_end = indices_begin + values->length();

    std::iota(indices_begin, indices_end, 0);
    *offsets = std::make_shared<UInt64Array>(values->length(), indices_buf);

    if (n >= static_cast<uint64_t>(values->length())) {
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
                       [&values, this](uint64_t left, uint64_t right) {
                         return compare_(*values, left, right);
                       });
    }
    return Status::OK();
  }
};

template <typename ArrowType, typename Comparator>
NthToIndicesKernelImpl<ArrowType, Comparator>* MakeNthToIndicesKernelImpl(
    Comparator comparator) {
  return new NthToIndicesKernelImpl<ArrowType, Comparator>(comparator);
}

Status NthToIndicesKernel::Make(const std::shared_ptr<DataType>& value_type,
                                std::unique_ptr<NthToIndicesKernel>* out) {
  NthToIndicesKernel* kernel;
  switch (value_type->id()) {
    case Type::UINT8:
      kernel = MakeNthToIndicesKernelImpl<UInt8Type>(CompareValues<UInt8Array>);
      break;
    case Type::INT8:
      kernel = MakeNthToIndicesKernelImpl<Int8Type>(CompareValues<Int8Array>);
      break;
    case Type::UINT16:
      kernel = MakeNthToIndicesKernelImpl<UInt16Type>(CompareValues<UInt16Array>);
      break;
    case Type::INT16:
      kernel = MakeNthToIndicesKernelImpl<Int16Type>(CompareValues<Int16Array>);
      break;
    case Type::UINT32:
      kernel = MakeNthToIndicesKernelImpl<UInt32Type>(CompareValues<UInt32Array>);
      break;
    case Type::INT32:
      kernel = MakeNthToIndicesKernelImpl<Int32Type>(CompareValues<Int32Array>);
      break;
    case Type::UINT64:
      kernel = MakeNthToIndicesKernelImpl<UInt64Type>(CompareValues<UInt64Array>);
      break;
    case Type::INT64:
      kernel = MakeNthToIndicesKernelImpl<Int64Type>(CompareValues<Int64Array>);
      break;
    case Type::FLOAT:
      kernel = MakeNthToIndicesKernelImpl<FloatType>(CompareValues<FloatArray>);
      break;
    case Type::DOUBLE:
      kernel = MakeNthToIndicesKernelImpl<DoubleType>(CompareValues<DoubleArray>);
      break;
    case Type::BINARY:
      kernel = MakeNthToIndicesKernelImpl<BinaryType>(CompareViews<BinaryArray>);
      break;
    case Type::STRING:
      kernel = MakeNthToIndicesKernelImpl<StringType>(CompareViews<StringArray>);
      break;
    default:
      return Status::NotImplemented("N-th of ", *value_type, " arrays");
  }
  out->reset(kernel);
  return Status::OK();
}

Status NthToIndices(FunctionContext* ctx, const Datum& values, uint64_t n,
                    Datum* offsets) {
  std::unique_ptr<NthToIndicesKernel> kernel;
  RETURN_NOT_OK(NthToIndicesKernel::Make(values.type(), &kernel));
  return kernel->Call(ctx, values, n, offsets);
}

Status NthToIndices(FunctionContext* ctx, const Array& values, uint64_t n,
                    std::shared_ptr<Array>* offsets) {
  Datum offsets_datum;
  RETURN_NOT_OK(NthToIndices(ctx, Datum(values.data()), n, &offsets_datum));
  *offsets = offsets_datum.make_array();
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
