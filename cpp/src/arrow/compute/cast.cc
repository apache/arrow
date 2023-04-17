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
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"
#include "arrow/util/reflection_internal.h"

namespace arrow {

using internal::ToTypeName;

namespace compute {
namespace internal {

// ----------------------------------------------------------------------
// Function options

namespace {

std::unordered_map<int, std::shared_ptr<CastFunction>> g_cast_table;
std::once_flag cast_table_initialized;

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
  AddCastFunctions(GetDictionaryCasts());
  AddCastFunctions(GetExtensionCasts());
}

void EnsureInitCastTable() { std::call_once(cast_table_initialized, InitCastTable); }

const FunctionDoc cast_doc{"Cast values to another data type",
                           ("Behavior when values wouldn't fit in the target type\n"
                            "can be controlled through CastOptions."),
                           {"input"},
                           "CastOptions"};

// Metafunction for dispatching to appropriate CastFunction. This corresponds
// to the standard SQL CAST(expr AS target_type)
class CastMetaFunction : public MetaFunction {
 public:
  CastMetaFunction() : MetaFunction("cast", Arity::Unary(), cast_doc) {}

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
    // args[0].type() could be a nullptr so check for that before
    // we do anything with it.
    if (args[0].type() && args[0].type()->Equals(*cast_options->to_type)) {
      // Nested types might differ in field names but still be considered equal,
      // so we can only return non-nested types as-is.
      if (!is_nested(args[0].type()->id())) {
        return args[0];
      } else if (args[0].is_array()) {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> array,
                              ::arrow::internal::GetArrayView(
                                  args[0].array(), cast_options->to_type.owned_type));
        return Datum(array);
      } else if (args[0].is_chunked_array()) {
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<ChunkedArray> array,
            args[0].chunked_array()->View(cast_options->to_type.owned_type));
        return Datum(array);
      }
    }

    Result<std::shared_ptr<CastFunction>> result =
        GetCastFunction(*cast_options->to_type);
    if (!result.ok()) {
      Status s = result.status();
      return s.WithMessage(s.message(), " from ", *args[0].type());
    }
    return (*result)->Execute(args, options, ctx);
  }
};

static auto kCastOptionsType = GetFunctionOptionsType<CastOptions>(
    arrow::internal::DataMember("to_type", &CastOptions::to_type),
    arrow::internal::DataMember("allow_int_overflow", &CastOptions::allow_int_overflow),
    arrow::internal::DataMember("allow_time_truncate", &CastOptions::allow_time_truncate),
    arrow::internal::DataMember("allow_time_overflow", &CastOptions::allow_time_overflow),
    arrow::internal::DataMember("allow_decimal_truncate",
                                &CastOptions::allow_decimal_truncate),
    arrow::internal::DataMember("allow_float_truncate",
                                &CastOptions::allow_float_truncate),
    arrow::internal::DataMember("allow_invalid_utf8", &CastOptions::allow_invalid_utf8));
}  // namespace

void RegisterScalarCast(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(std::make_shared<CastMetaFunction>()));
  DCHECK_OK(registry->AddFunctionOptionsType(kCastOptionsType));
}

CastFunction::CastFunction(std::string name, Type::type out_type_id)
    : ScalarFunction(std::move(name), Arity::Unary(), FunctionDoc::Empty()),
      out_type_id_(out_type_id) {}

Status CastFunction::AddKernel(Type::type in_type_id, ScalarKernel kernel) {
  // We use the same KernelInit for every cast
  kernel.init = internal::CastState::Init;
  RETURN_NOT_OK(ScalarFunction::AddKernel(kernel));
  in_type_ids_.push_back(in_type_id);
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

Result<const Kernel*> CastFunction::DispatchExact(
    const std::vector<TypeHolder>& types) const {
  RETURN_NOT_OK(CheckArity(types.size()));

  std::vector<const ScalarKernel*> candidate_kernels;
  for (const auto& kernel : kernels_) {
    if (kernel.signature->MatchesInputs(types)) {
      candidate_kernels.push_back(&kernel);
    }
  }

  if (candidate_kernels.size() == 0) {
    return Status::NotImplemented("Unsupported cast from ", types[0].type->ToString(),
                                  " to ", ToTypeName(out_type_id_), " using function ",
                                  this->name());
  }

  if (candidate_kernels.size() == 1) {
    // One match, return it
    return candidate_kernels[0];
  }

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

Result<std::shared_ptr<CastFunction>> GetCastFunction(const DataType& to_type) {
  internal::EnsureInitCastTable();
  auto it = internal::g_cast_table.find(static_cast<int>(to_type.id()));
  if (it == internal::g_cast_table.end()) {
    return Status::NotImplemented("Unsupported cast to ", to_type);
  }
  return it->second;
}

}  // namespace internal

CastOptions::CastOptions(bool safe)
    : FunctionOptions(internal::kCastOptionsType),
      allow_int_overflow(!safe),
      allow_time_truncate(!safe),
      allow_time_overflow(!safe),
      allow_decimal_truncate(!safe),
      allow_float_truncate(!safe),
      allow_invalid_utf8(!safe) {}

constexpr char CastOptions::kTypeName[];

Result<Datum> Cast(const Datum& value, const CastOptions& options, ExecContext* ctx) {
  return CallFunction("cast", {value}, &options, ctx);
}

Result<Datum> Cast(const Datum& value, const TypeHolder& to_type,
                   const CastOptions& options, ExecContext* ctx) {
  CastOptions options_with_to_type = options;
  options_with_to_type.to_type = to_type;
  return Cast(value, options_with_to_type, ctx);
}

Result<std::shared_ptr<Array>> Cast(const Array& value, const TypeHolder& to_type,
                                    const CastOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result, Cast(Datum(value), to_type, options, ctx));
  return result.make_array();
}

bool CanCast(const DataType& from_type, const DataType& to_type) {
  internal::EnsureInitCastTable();
  auto it = internal::g_cast_table.find(static_cast<int>(to_type.id()));
  if (it == internal::g_cast_table.end()) {
    return false;
  }

  const internal::CastFunction* function = it->second.get();
  DCHECK_EQ(function->out_type_id(), to_type.id());

  for (auto from_id : function->in_type_ids()) {
    // XXX should probably check the output type as well
    if (from_type.id() == from_id) return true;
  }

  return false;
}

}  // namespace compute
}  // namespace arrow
