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

#include "arrow/compute/registry.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "arrow/compute/function.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {

class FunctionRegistry::FunctionRegistryImpl {
 public:
  Status AddFunction(std::shared_ptr<Function> function, bool allow_overwrite) {
    RETURN_NOT_OK(function->Validate());

    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string& name = function->name();
    auto it = name_to_function_.find(name);
    if (it != name_to_function_.end() && !allow_overwrite) {
      return Status::KeyError("Already have a function registered with name: ", name);
    }
    name_to_function_[name] = std::move(function);
    return Status::OK();
  }

  Status AddAlias(const std::string& target_name, const std::string& source_name) {
    std::lock_guard<std::mutex> mutation_guard(lock_);

    auto it = name_to_function_.find(source_name);
    if (it == name_to_function_.end()) {
      return Status::KeyError("No function registered with name: ", source_name);
    }
    name_to_function_[target_name] = it->second;
    return Status::OK();
  }

  Status AddFunctionOptionsType(const FunctionOptionsType* options_type,
                                bool allow_overwrite = false) {
    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string name = options_type->type_name();
    auto it = name_to_options_type_.find(name);
    if (it != name_to_options_type_.end() && !allow_overwrite) {
      return Status::KeyError(
          "Already have a function options type registered with name: ", name);
    }
    name_to_options_type_[name] = options_type;
    return Status::OK();
  }

  Result<std::shared_ptr<Function>> GetFunction(const std::string& name) const {
    auto it = name_to_function_.find(name);
    if (it == name_to_function_.end()) {
      return Status::KeyError("No function registered with name: ", name);
    }
    return it->second;
  }

  std::vector<std::string> GetFunctionNames() const {
    std::vector<std::string> results;
    for (auto it : name_to_function_) {
      results.push_back(it.first);
    }
    std::sort(results.begin(), results.end());
    return results;
  }

  Result<const FunctionOptionsType*> GetFunctionOptionsType(
      const std::string& name) const {
    auto it = name_to_options_type_.find(name);
    if (it == name_to_options_type_.end()) {
      return Status::KeyError("No function options type registered with name: ", name);
    }
    return it->second;
  }

  int num_functions() const { return static_cast<int>(name_to_function_.size()); }

 private:
  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<Function>> name_to_function_;
  std::unordered_map<std::string, const FunctionOptionsType*> name_to_options_type_;
};

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make() {
  return std::unique_ptr<FunctionRegistry>(new FunctionRegistry());
}

FunctionRegistry::FunctionRegistry() { impl_.reset(new FunctionRegistryImpl()); }

FunctionRegistry::~FunctionRegistry() {}

Status FunctionRegistry::AddFunction(std::shared_ptr<Function> function,
                                     bool allow_overwrite) {
  return impl_->AddFunction(std::move(function), allow_overwrite);
}

Status FunctionRegistry::AddAlias(const std::string& target_name,
                                  const std::string& source_name) {
  return impl_->AddAlias(target_name, source_name);
}

Status FunctionRegistry::AddFunctionOptionsType(const FunctionOptionsType* options_type,
                                                bool allow_overwrite) {
  return impl_->AddFunctionOptionsType(options_type, allow_overwrite);
}

Result<std::shared_ptr<Function>> FunctionRegistry::GetFunction(
    const std::string& name) const {
  return impl_->GetFunction(name);
}

std::vector<std::string> FunctionRegistry::GetFunctionNames() const {
  return impl_->GetFunctionNames();
}

Result<const FunctionOptionsType*> FunctionRegistry::GetFunctionOptionsType(
    const std::string& name) const {
  return impl_->GetFunctionOptionsType(name);
}

int FunctionRegistry::num_functions() const { return impl_->num_functions(); }

namespace internal {

static std::unique_ptr<FunctionRegistry> CreateBuiltInRegistry() {
  auto registry = FunctionRegistry::Make();

  // Scalar functions
  RegisterScalarArithmetic(registry.get());
  RegisterScalarBoolean(registry.get());
  RegisterScalarCast(registry.get());
  RegisterScalarComparison(registry.get());
  RegisterScalarNested(registry.get());
  RegisterScalarSetLookup(registry.get());
  RegisterScalarStringAscii(registry.get());
  RegisterScalarValidity(registry.get());
  RegisterScalarFillNull(registry.get());
  RegisterScalarIfElse(registry.get());
  RegisterScalarTemporal(registry.get());

  RegisterScalarOptions(registry.get());

  // Vector functions
  RegisterVectorHash(registry.get());
  RegisterVectorReplace(registry.get());
  RegisterVectorSelection(registry.get());
  RegisterVectorNested(registry.get());
  RegisterVectorSort(registry.get());

  RegisterVectorOptions(registry.get());

  // Aggregate functions
  RegisterScalarAggregateBasic(registry.get());
  RegisterScalarAggregateMode(registry.get());
  RegisterScalarAggregateQuantile(registry.get());
  RegisterScalarAggregateTDigest(registry.get());
  RegisterScalarAggregateVariance(registry.get());
  RegisterHashAggregateBasic(registry.get());

  RegisterAggregateOptions(registry.get());

  return registry;
}

}  // namespace internal

FunctionRegistry* GetFunctionRegistry() {
  static auto g_registry = internal::CreateBuiltInRegistry();
  return g_registry.get();
}

}  // namespace compute
}  // namespace arrow
