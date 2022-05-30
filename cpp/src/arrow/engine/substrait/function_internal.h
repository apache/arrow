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

// This API is EXPERIMENTAL.

#include "arrow/engine/substrait/extension_set.h"
#include "arrow/compute/function.h"
#include "arrow/compute/exec/expression.h"

#include "substrait/function.pb.h"  // IWYU pragma: export


namespace arrow{
namespace engine{

using ArrowToSubstrait = const std::function<Result<substrait::Expression::ScalarFunction>(const arrow::compute::Expression&, ExtensionSet*)>;
using SubstraitToArrow = std::function<Result<arrow::compute::Expression>(const substrait::Expression::ScalarFunction&)>;
class FunctionMapping {
  
  // Registration API
  Status AddArrowToSubstrait(std::string arrow_function_name, ArrowToSubstrait conversion_func);
  Status AddSubstraitToArrow(std::string substrait_function_name, SubstraitToArrow conversion_func);
  
  // Usage API
  Result<std::unique_ptr<substrait::Expression::ScalarFunction>> ToProto(const arrow::compute::Expression::Call& call, ExtensionSet* ext_set);
  Result<compute::Expression> FromProto(const substrait::Expression::ScalarFunction& call);
};


}  // namespace engine
}  // namespace arrow
