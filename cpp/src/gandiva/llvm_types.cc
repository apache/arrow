// Copyright (C) 2017-2018 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "codegen/llvm_types.h"

namespace gandiva {

LLVMTypes::LLVMTypes(llvm::LLVMContext &context)
    : context_(context) {

  arrow_id_to_llvm_type_map_ = {
      {arrow::Type::type::BOOL, i1_type()},
      {arrow::Type::type::INT32, i32_type()},
      {arrow::Type::type::INT64, i64_type()},
      {arrow::Type::type::FLOAT, float_type()},
      {arrow::Type::type::DOUBLE, double_type()},
      {arrow::Type::type::DATE64, i64_type()},
      {arrow::Type::type::TIME64, i64_type()},
      {arrow::Type::type::TIMESTAMP, i64_type()},
  };
}

} // namespace gandiva
