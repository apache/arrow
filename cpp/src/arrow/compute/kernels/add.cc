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

#include "arrow/compute/kernels/add.h"
#include "arrow/builder.h"
#include "arrow/compute/context.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

template <typename ArrowType>
class AddKernelImpl : public AddKernel {
 private:
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  std::shared_ptr<DataType> result_type_;

  Status Add(FunctionContext* ctx, const std::shared_ptr<ArrayType>& lhs,
             const std::shared_ptr<ArrayType>& rhs, std::shared_ptr<Array>* result) {
    NumericBuilder<ArrowType> builder;
    RETURN_NOT_OK(builder.Reserve(lhs->length()));
    for (int i = 0; i < lhs->length(); i++) {
      if (lhs->IsNull(i) || rhs->IsNull(i)) {
        builder.UnsafeAppendNull();
      } else {
        builder.UnsafeAppend(lhs->Value(i) + rhs->Value(i));
      }
    }
    return builder.Finish(result);
  }

 public:
  explicit AddKernelImpl(std::shared_ptr<DataType> result_type)
      : result_type_(result_type) {}

  Status Call(FunctionContext* ctx, const Datum& lhs, const Datum& rhs,
              Datum* out) override {
    if (!lhs.is_array() || !rhs.is_array()) {
      return Status::Invalid("AddKernel expects array values");
    }
    if (lhs.length() != rhs.length()) {
      return Status::Invalid("AddKernel expects arrays with the same length");
    }
    auto lhs_array = lhs.make_array();
    auto rhs_array = rhs.make_array();
    std::shared_ptr<Array> result;
    RETURN_NOT_OK(this->Add(ctx, lhs_array, rhs_array, &result));
    *out = result;
    return Status::OK();
  }

  std::shared_ptr<DataType> out_type() const override { return result_type_; }

  Status Add(FunctionContext* ctx, const std::shared_ptr<Array>& lhs,
             const std::shared_ptr<Array>& rhs, std::shared_ptr<Array>* result) override {
    auto lhs_array = std::static_pointer_cast<ArrayType>(lhs);
    auto rhs_array = std::static_pointer_cast<ArrayType>(rhs);
    return Add(ctx, lhs_array, rhs_array, result);
  }
};

Status AddKernel::Make(const std::shared_ptr<DataType>& value_type,
                       std::unique_ptr<AddKernel>* out) {
  AddKernel* kernel;
  switch (value_type->id()) {
    case Type::UINT8:
      kernel = new AddKernelImpl<UInt8Type>(value_type);
      break;
    case Type::INT8:
      kernel = new AddKernelImpl<Int8Type>(value_type);
      break;
    case Type::UINT16:
      kernel = new AddKernelImpl<UInt16Type>(value_type);
      break;
    case Type::INT16:
      kernel = new AddKernelImpl<Int16Type>(value_type);
      break;
    case Type::UINT32:
      kernel = new AddKernelImpl<UInt32Type>(value_type);
      break;
    case Type::INT32:
      kernel = new AddKernelImpl<Int32Type>(value_type);
      break;
    case Type::UINT64:
      kernel = new AddKernelImpl<UInt64Type>(value_type);
      break;
    case Type::INT64:
      kernel = new AddKernelImpl<Int64Type>(value_type);
      break;
    case Type::FLOAT:
      kernel = new AddKernelImpl<FloatType>(value_type);
      break;
    case Type::DOUBLE:
      kernel = new AddKernelImpl<DoubleType>(value_type);
      break;
    default:
      return Status::NotImplemented("Arithmetic operations on ", *value_type, " arrays");
  }
  out->reset(kernel);
  return Status::OK();
}

Status Add(FunctionContext* ctx, const Array& lhs, const Array& rhs,
           std::shared_ptr<Array>* result) {
  Datum result_datum;
  std::unique_ptr<AddKernel> kernel;
  ARROW_RETURN_IF(
      !lhs.type()->Equals(rhs.type()),
      Status::Invalid("Array types should be equal to use arithmetic kernels"));
  RETURN_NOT_OK(AddKernel::Make(lhs.type(), &kernel));
  RETURN_NOT_OK(kernel->Call(ctx, Datum(lhs.data()), Datum(rhs.data()), &result_datum));
  *result = result_datum.make_array();
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
