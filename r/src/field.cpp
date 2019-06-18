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

#if defined(ARROW_R_WITH_ARROW)

// [[arrow::export]]
std::shared_ptr<arrow::Field> Field__initialize(
    const std::string& name, const std::shared_ptr<arrow::DataType>& field,
    bool nullable = true) {
  return arrow::field(name, field, nullable);
}

// [[arrow::export]]
std::string Field__ToString(const std::shared_ptr<arrow::Field>& field) {
  return field->ToString();
}

// [[arrow::export]]
std::string Field__name(const std::shared_ptr<arrow::Field>& field) {
  return field->name();
}

// [[arrow::export]]
bool Field__Equals(const std::shared_ptr<arrow::Field>& field,
                   const std::shared_ptr<arrow::Field>& other) {
  return field->Equals(other);
}

// [[arrow::export]]
bool Field__nullable(const std::shared_ptr<arrow::Field>& field) {
  return field->nullable();
}

// [[arrow::export]]
std::shared_ptr<arrow::DataType> Field__type(const std::shared_ptr<arrow::Field>& field) {
  return field->type();
}

#endif
