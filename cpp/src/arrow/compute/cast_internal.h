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

#pragma once

#include <memory>
#include <vector>

#include "arrow/compute/cast.h"                      // IWYU pragma: keep
#include "arrow/compute/kernel.h"                    // IWYU pragma: keep
#include "arrow/compute/kernels/codegen_internal.h"  // IWYU pragma: keep

namespace arrow {
namespace compute {
namespace internal {

using CastState = OptionsWrapper<CastOptions>;

// Cast functions are _not_ registered in the FunctionRegistry, though they use
// the same execution machinery
class CastFunction : public ScalarFunction {
 public:
  CastFunction(std::string name, Type::type out_type_id);

  Type::type out_type_id() const { return out_type_id_; }
  const std::vector<Type::type>& in_type_ids() const { return in_type_ids_; }

  Status AddKernel(Type::type in_type_id, std::vector<InputType> in_types,
                   OutputType out_type, ArrayKernelExec exec,
                   NullHandling::type = NullHandling::INTERSECTION,
                   MemAllocation::type = MemAllocation::PREALLOCATE);

  // Note, this function toggles off memory allocation and sets the init
  // function to CastInit
  Status AddKernel(Type::type in_type_id, ScalarKernel kernel);

  Result<const Kernel*> DispatchExact(
      const std::vector<TypeHolder>& types) const override;

 private:
  std::vector<Type::type> in_type_ids_;
  const Type::type out_type_id_;
};

// See kernels/scalar_cast_*.cc for these
std::vector<std::shared_ptr<CastFunction>> GetBooleanCasts();
std::vector<std::shared_ptr<CastFunction>> GetNumericCasts();
std::vector<std::shared_ptr<CastFunction>> GetTemporalCasts();
std::vector<std::shared_ptr<CastFunction>> GetBinaryLikeCasts();
std::vector<std::shared_ptr<CastFunction>> GetNestedCasts();
std::vector<std::shared_ptr<CastFunction>> GetDictionaryCasts();
std::vector<std::shared_ptr<CastFunction>> GetExtensionCasts();

ARROW_EXPORT
Result<std::shared_ptr<CastFunction>> GetCastFunction(const DataType& to_type);

}  // namespace internal
}  // namespace compute
}  // namespace arrow
