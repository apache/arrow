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

#include "./arrow_types.h"

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

namespace cpp11 {

const char* r6_class_name<arrow::Scalar>::get(
    const std::shared_ptr<arrow::Scalar>& scalar) {
  if (scalar->type->id() == arrow::Type::STRUCT) {
    return "StructScalar";
  }
  return "Scalar";
}

}  // namespace cpp11

// [[arrow::export]]
std::shared_ptr<arrow::Scalar> Array__GetScalar(const std::shared_ptr<arrow::Array>& x,
                                                int64_t i) {
  return ValueOrStop(x->GetScalar(i));
}

// [[arrow::export]]
std::string Scalar__ToString(const std::shared_ptr<arrow::Scalar>& s) {
  return s->ToString();
}

// [[arrow::export]]
std::shared_ptr<arrow::Scalar> StructScalar__field(
    const std::shared_ptr<arrow::StructScalar>& s, int i) {
  return ValueOrStop(s->field(i));
}

// [[arrow::export]]
std::shared_ptr<arrow::Scalar> StructScalar__GetFieldByName(
    const std::shared_ptr<arrow::StructScalar>& s, const std::string& name) {
  return ValueOrStop(s->field(name));
}

// [[arrow::export]]
std::shared_ptr<arrow::Array> MakeArrayFromScalar(
    const std::shared_ptr<arrow::Scalar>& scalar, int n) {
  if (scalar->type->id() == arrow::Type::EXTENSION) {
    auto extension_scalar = std::dynamic_pointer_cast<arrow::ExtensionScalar>(scalar);
    auto type = std::dynamic_pointer_cast<arrow::ExtensionType>(scalar->type);
    auto storage_type = type->storage_type();
    auto storage = ValueOrStop(
        arrow::MakeArrayFromScalar(*extension_scalar->value, n, gc_memory_pool()));
    return type->WrapArray(type, storage);
  } else {
    return ValueOrStop(arrow::MakeArrayFromScalar(*scalar, n, gc_memory_pool()));
  }
}

// [[arrow::export]]
bool Scalar__is_valid(const std::shared_ptr<arrow::Scalar>& s) { return s->is_valid; }

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Scalar__type(const std::shared_ptr<arrow::Scalar>& s) {
  return s->type;
}

// [[arrow::export]]
bool Scalar__Equals(const std::shared_ptr<arrow::Scalar>& lhs,
                    const std::shared_ptr<arrow::Scalar>& rhs) {
  return lhs->Equals(*rhs);
}

// [[arrow::export]]
bool Scalar__ApproxEquals(const std::shared_ptr<arrow::Scalar>& lhs,
                          const std::shared_ptr<arrow::Scalar>& rhs) {
  return lhs->ApproxEquals(*rhs);
}
