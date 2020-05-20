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

#include "arrow/compute/cast.h"

#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/cast_internal.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/registry.h"

namespace arrow {
namespace compute {

namespace internal {

std::unordered_map<Type::type, std::shared_ptr<const CastFunction>> g_cast_table;
static std::once_flag cast_table_initialized;

void AddCastFunctions(const std::vector<std::shared_ptr<CastFunction>>& funcs) {
  for (const auto& func : funcs) {
    g_cast_table[func->out_type_id()] = func;
  }
}

void InitCastTable() {
  AddCastFunctions(GetBooleanCasts());
  AddCastFunctions(GetNumericCasts());
  AddCastFunctions(GetStringCasts());
  AddCastFunctions(GetTemporalCasts());
}

void EnsureInitCastTable() { std::call_once(cast_table_initialized, InitCastTable); }

void RegisterScalarCasts(FunctionRegistry* registry) {
  EnsureInitCastTable();
  for (auto it : g_cast_table) {
    DCHECK_OK(registry->AddFunction(it.second));
  }
}

}  // namespace internal

struct CastFunction::CastFunctionImpl {
  Type::type out_type;
  std::unordered_set<Type::type> in_types;
};

CastFunction::CastFunction(std::string name, Type::type out_type)
    : ScalarFunction(std::move(name), /*arity=*/1) {
  impl_.reset(new CastFunctionImpl());
  impl_->out_type = out_type;
}

CastFunction::~CastFunction() {}

Type::type CastFunction::out_type_id() const { return impl_->out_type; }

Status CastFunction::AddKernel(Type::type in_type_id, std::vector<InputType> in_types,
                               OutputType out_type, ArrayKernelExec exec,
                               KernelInit init) {
  RETURN_NOT_OK(
      ScalarFunction::AddKernel(std::move(in_types), std::move(out_type), exec, init));
  impl_->in_types.insert(in_type_id);
  return Status::OK();
}

Status CastFunction::AddKernel(Type::type in_type_id, ScalarKernel kernel) {
  RETURN_NOT_OK(ScalarFunction::AddKernel(kernel));
  impl_->in_types.insert(in_type_id);
  return Status::OK();
}

bool CastFunction::CanCastTo(const DataType& out_type) const {
  return impl_->in_types.find(out_type.id()) != impl_->in_types.end();
}

Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options, ExecContext* ctx) {
  CastOptions options_with_to_type;
  options_with_to_type.to_type = to_type;
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const CastFunction> cast_func,
                        GetCastFunction(value.type(), to_type));
  return cast_func->Execute({Datum(value)}, &options_with_to_type, ctx);
}

Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Cast(Datum(value), to_type, options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<const CastFunction>> GetCastFunction(
    const std::shared_ptr<DataType>& from_type,
    const std::shared_ptr<DataType>& to_type) {
  auto it = internal::g_cast_table.find(from_type->id());
  if (it == internal::g_cast_table.end()) {
    return Status::NotImplemented("No cast implemented from ", from_type->ToString(),
                                  " to ", to_type->ToString());
  }
  return it->second;
}

bool CanCast(const DataType& from_type, const DataType& to_type) {
  // TODO
  auto it = internal::g_cast_table.find(from_type.id());
  if (it == internal::g_cast_table.end()) {
    return false;
  }
  return it->second->CanCastTo(to_type);
}

}  // namespace compute
}  // namespace arrow
