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

#include "gandiva/llvm_types.h"

namespace gandiva {

// LLVM doesn't distinguish between signed and unsigned types.

LLVMTypes::LLVMTypes(llvm::LLVMContext& context) : context_(context) {
  arrow_id_to_llvm_type_map_ = {
      {arrow::Type::type::BOOL, i1_type()},
      {arrow::Type::type::INT8, i8_type()},
      {arrow::Type::type::INT16, i16_type()},
      {arrow::Type::type::INT32, i32_type()},
      {arrow::Type::type::INT64, i64_type()},
      {arrow::Type::type::UINT8, i8_type()},
      {arrow::Type::type::UINT16, i16_type()},
      {arrow::Type::type::UINT32, i32_type()},
      {arrow::Type::type::UINT64, i64_type()},
      {arrow::Type::type::FLOAT, float_type()},
      {arrow::Type::type::DOUBLE, double_type()},
      {arrow::Type::type::DATE64, i64_type()},
      {arrow::Type::type::TIME32, i32_type()},
      {arrow::Type::type::TIME64, i64_type()},
      {arrow::Type::type::TIMESTAMP, i64_type()},
      {arrow::Type::type::STRING, i8_ptr_type()},
      {arrow::Type::type::BINARY, i8_ptr_type()},
      {arrow::Type::type::DECIMAL, i128_type()},
  };
}

}  // namespace gandiva
