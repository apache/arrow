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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/compute/cast_internal.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::ToTypeName;

namespace compute {
namespace internal {

std::unordered_map<int, std::shared_ptr<CastFunction>> g_cast_table;
static std::once_flag cast_table_initialized;

void AddCastFunctions(const std::vector<std::shared_ptr<CastFunction>>& funcs) {
  for (const auto& func : funcs) {
    g_cast_table[static_cast<int>(func->out_type_id())] = func;
  }
}

void InitCastTable() {
  AddCastFunctions(GetBooleanCasts());
  AddCastFunctions(GetBinaryLikeCasts());
  AddCastFunctions(GetNestedCasts());
  AddCastFunctions(GetNumericCasts());
  AddCastFunctions(GetTemporalCasts());
}

void EnsureInitCastTable() { std::call_once(cast_table_initialized, InitCastTable); }

namespace {

// Private version of GetCastFunction with better error reporting
// if the input type is known.
Result<std::shared_ptr<CastFunction>> GetCastFunctionInternal(
    const std::shared_ptr<DataType>& to_type, const DataType* from_type = nullptr) {
  internal::EnsureInitCastTable();
  auto it = internal::g_cast_table.find(static_cast<int>(to_type->id()));
  if (it == internal::g_cast_table.end()) {
    if (from_type != nullptr) {
      return Status::NotImplemented("Unsupported cast from ", *from_type, " to ",
                                    *to_type,
                                    " (no available cast function for target type)");
    } else {
      return Status::NotImplemented("Unsupported cast to ", *to_type,
                                    " (no available cast function for target type)");
    }
  }
  return it->second;
}

}  // namespace

// Metafunction for dispatching to appropraite CastFunction. This corresponds
// to the standard SQL CAST(expr AS target_type)
class CastMetaFunction : public MetaFunction {
 public:
  CastMetaFunction() : MetaFunction("cast", Arity::Unary()) {}

  Result<const CastOptions*> ValidateOptions(const FunctionOptions* options) const {
    auto cast_options = static_cast<const CastOptions*>(options);

    if (cast_options == nullptr || cast_options->to_type == nullptr) {
      return Status::Invalid(
          "Cast requires that options be passed with "
          "the to_type populated");
    }

    return cast_options;
  }

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    ARROW_ASSIGN_OR_RAISE(auto cast_options, ValidateOptions(options));
    if (args[0].type()->Equals(*cast_options->to_type)) {
      return args[0];
    }
    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<CastFunction> cast_func,
        GetCastFunctionInternal(cast_options->to_type, args[0].type().get()));
    return cast_func->Execute(args, options, ctx);
  }
};

void RegisterScalarCast(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<CastMetaFunction>()));
}

}  // namespace internal

struct CastFunction::CastFunctionImpl {
  Type::type out_type;
  std::unordered_set<int> in_types;
};

CastFunction::CastFunction(std::string name, Type::type out_type)
    : ScalarFunction(std::move(name), Arity::Unary()) {
  impl_.reset(new CastFunctionImpl());
  impl_->out_type = out_type;
}

CastFunction::~CastFunction() {}

Type::type CastFunction::out_type_id() const { return impl_->out_type; }

Status CastFunction::AddKernel(Type::type in_type_id, ScalarKernel kernel) {
  // We use the same KernelInit for every cast
  kernel.init = internal::CastState::Init;
  RETURN_NOT_OK(ScalarFunction::AddKernel(kernel));
  impl_->in_types.insert(static_cast<int>(in_type_id));
  return Status::OK();
}

Status CastFunction::AddKernel(Type::type in_type_id, std::vector<InputType> in_types,
                               OutputType out_type, ArrayKernelExec exec,
                               NullHandling::type null_handling,
                               MemAllocation::type mem_allocation) {
  ScalarKernel kernel;
  kernel.signature = KernelSignature::Make(std::move(in_types), std::move(out_type));
  kernel.exec = exec;
  kernel.null_handling = null_handling;
  kernel.mem_allocation = mem_allocation;
  return AddKernel(in_type_id, std::move(kernel));
}

bool CastFunction::CanCastTo(const DataType& out_type) const {
  return impl_->in_types.find(static_cast<int>(out_type.id())) != impl_->in_types.end();
}

Result<const ScalarKernel*> CastFunction::DispatchExact(
    const std::vector<ValueDescr>& values) const {
  const int passed_num_args = static_cast<int>(values.size());

  // Validate arity
  if (passed_num_args != 1) {
    return Status::Invalid("Cast functions accept 1 argument but passed ",
                           passed_num_args);
  }
  std::vector<const ScalarKernel*> candidate_kernels;
  for (const auto& kernel : kernels_) {
    if (kernel.signature->MatchesInputs(values)) {
      candidate_kernels.push_back(&kernel);
    }
  }

  if (candidate_kernels.size() == 0) {
    return Status::NotImplemented("Unsupported cast from ", values[0].type->ToString(),
                                  " to ", ToTypeName(impl_->out_type), " using function ",
                                  this->name());
  } else if (candidate_kernels.size() == 1) {
    // One match, return it
    return candidate_kernels[0];
  } else {
    // Now we are in a casting scenario where we may have both a EXACT_TYPE and
    // a SAME_TYPE_ID. So we will see if there is an exact match among the
    // candidate kernels and if not we will just return the first one
    for (auto kernel : candidate_kernels) {
      const InputType& arg0 = kernel->signature->in_types()[0];
      if (arg0.kind() == InputType::EXACT_TYPE) {
        // Bingo. Return it
        return kernel;
      }
    }
    // We didn't find an exact match. So just return some kernel that matches
    return candidate_kernels[0];
  }
}

Result<Datum> Cast(const Datum& value, const CastOptions& options, ExecContext* ctx) {
  return CallFunction("cast", {value}, &options, ctx);
}

Result<Datum> Cast(const Datum& value, std::shared_ptr<DataType> to_type,
                   const CastOptions& options, ExecContext* ctx) {
  CastOptions options_with_to_type = options;
  options_with_to_type.to_type = to_type;
  return Cast(value, options_with_to_type, ctx);
}

Result<std::shared_ptr<Array>> Cast(const Array& value, std::shared_ptr<DataType> to_type,
                                    const CastOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Cast(Datum(value), to_type, options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<CastFunction>> GetCastFunction(
    const std::shared_ptr<DataType>& to_type) {
  return internal::GetCastFunctionInternal(to_type);
}

bool CanCast(const DataType& from_type, const DataType& to_type) {
  // TODO
  internal::EnsureInitCastTable();
  auto it = internal::g_cast_table.find(static_cast<int>(from_type.id()));
  if (it == internal::g_cast_table.end()) {
    return false;
  }
  return it->second->CanCastTo(to_type);
}

}  // namespace compute
}  // namespace arrow
