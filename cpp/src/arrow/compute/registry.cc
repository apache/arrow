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
  explicit FunctionRegistryImpl(FunctionRegistryImpl* parent = NULLPTR)
      : parent_(parent) {}
  ~FunctionRegistryImpl() {}

  Status CanAddFunction(std::shared_ptr<Function> function, bool allow_overwrite) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunction(function, allow_overwrite));
    }
    return DoAddFunction(function, allow_overwrite, /*add=*/false);
  }

  Status AddFunction(std::shared_ptr<Function> function, bool allow_overwrite) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunction(function, allow_overwrite));
    }
    return DoAddFunction(function, allow_overwrite, /*add=*/true);
  }

  Status CanAddAlias(const std::string& target_name, const std::string& source_name) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunctionName(target_name,
                                                /*allow_overwrite=*/false));
    }
    return DoAddAlias(target_name, source_name, /*add=*/false);
  }

  Status AddAlias(const std::string& target_name, const std::string& source_name) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunctionName(target_name,
                                                /*allow_overwrite=*/false));
    }
    return DoAddAlias(target_name, source_name, /*add=*/true);
  }

  Status CanAddFunctionOptionsType(const FunctionOptionsType* options_type,
                                   bool allow_overwrite = false) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunctionOptionsType(options_type, allow_overwrite));
    }
    return DoAddFunctionOptionsType(options_type, allow_overwrite, /*add=*/false);
  }

  Status AddFunctionOptionsType(const FunctionOptionsType* options_type,
                                bool allow_overwrite = false) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunctionOptionsType(options_type, allow_overwrite));
    }
    return DoAddFunctionOptionsType(options_type, allow_overwrite, /*add=*/true);
  }

  Result<std::shared_ptr<Function>> GetFunction(const std::string& name) const {
    auto it = name_to_function_.find(name);
    if (it == name_to_function_.end()) {
      if (parent_ != NULLPTR) {
        return parent_->GetFunction(name);
      }
      return Status::KeyError("No function registered with name: ", name);
    }
    return it->second;
  }

  std::vector<std::string> GetFunctionNames() const {
    std::vector<std::string> results;
    if (parent_ != NULLPTR) {
      results = parent_->GetFunctionNames();
    }
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
      if (parent_ != NULLPTR) {
        return parent_->GetFunctionOptionsType(name);
      }
      return Status::KeyError("No function options type registered with name: ", name);
    }
    return it->second;
  }

  int num_functions() const {
    return (parent_ == NULLPTR ? 0 : parent_->num_functions()) +
           static_cast<int>(name_to_function_.size());
  }

 private:
  // must not acquire mutex
  Status CanAddFunctionName(const std::string& name, bool allow_overwrite) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddFunctionName(name, allow_overwrite));
    }
    if (!allow_overwrite) {
      auto it = name_to_function_.find(name);
      if (it != name_to_function_.end()) {
        return Status::KeyError("Already have a function registered with name: ", name);
      }
    }
    return Status::OK();
  }

  // must not acquire mutex
  Status CanAddOptionsTypeName(const std::string& name, bool allow_overwrite) {
    if (parent_ != NULLPTR) {
      RETURN_NOT_OK(parent_->CanAddOptionsTypeName(name, allow_overwrite));
    }
    if (!allow_overwrite) {
      auto it = name_to_options_type_.find(name);
      if (it != name_to_options_type_.end()) {
        return Status::KeyError(
            "Already have a function options type registered with name: ", name);
      }
    }
    return Status::OK();
  }

  Status DoAddFunction(std::shared_ptr<Function> function, bool allow_overwrite,
                       bool add) {
#ifndef NDEBUG
    // This validates docstrings extensively, so don't waste time on it
    // in release builds.
    RETURN_NOT_OK(function->Validate());
#endif

    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string& name = function->name();
    RETURN_NOT_OK(CanAddFunctionName(name, allow_overwrite));
    if (add) {
      name_to_function_[name] = std::move(function);
    }
    return Status::OK();
  }

  Status DoAddAlias(const std::string& target_name, const std::string& source_name,
                    bool add) {
    // source name must exist in this registry or the parent
    // check outside mutex, in case GetFunction leads to mutex acquisition
    ARROW_ASSIGN_OR_RAISE(auto func, GetFunction(source_name));

    std::lock_guard<std::mutex> mutation_guard(lock_);

    // target name must be available in this registry and the parent
    RETURN_NOT_OK(CanAddFunctionName(target_name, /*allow_overwrite=*/false));
    if (add) {
      name_to_function_[target_name] = func;
    }
    return Status::OK();
  }

  Status DoAddFunctionOptionsType(const FunctionOptionsType* options_type,
                                  bool allow_overwrite, bool add) {
    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string name = options_type->type_name();
    RETURN_NOT_OK(CanAddOptionsTypeName(name, /*allow_overwrite=*/false));
    if (add) {
      name_to_options_type_[options_type->type_name()] = options_type;
    }
    return Status::OK();
  }

  FunctionRegistryImpl* parent_;
  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<Function>> name_to_function_;
  std::unordered_map<std::string, const FunctionOptionsType*> name_to_options_type_;
};

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make() {
  return std::unique_ptr<FunctionRegistry>(new FunctionRegistry());
}

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make(FunctionRegistry* parent) {
  return std::unique_ptr<FunctionRegistry>(new FunctionRegistry(
      new FunctionRegistry::FunctionRegistryImpl(parent->impl_.get())));
}

FunctionRegistry::FunctionRegistry() : FunctionRegistry(new FunctionRegistryImpl()) {}

FunctionRegistry::FunctionRegistry(FunctionRegistryImpl* impl) { impl_.reset(impl); }

FunctionRegistry::~FunctionRegistry() {}

Status FunctionRegistry::CanAddFunction(std::shared_ptr<Function> function,
                                        bool allow_overwrite) {
  return impl_->CanAddFunction(std::move(function), allow_overwrite);
}

Status FunctionRegistry::AddFunction(std::shared_ptr<Function> function,
                                     bool allow_overwrite) {
  return impl_->AddFunction(std::move(function), allow_overwrite);
}

Status FunctionRegistry::CanAddAlias(const std::string& target_name,
                                     const std::string& source_name) {
  return impl_->CanAddAlias(target_name, source_name);
}

Status FunctionRegistry::AddAlias(const std::string& target_name,
                                  const std::string& source_name) {
  return impl_->AddAlias(target_name, source_name);
}

Status FunctionRegistry::CanAddFunctionOptionsType(
    const FunctionOptionsType* options_type, bool allow_overwrite) {
  return impl_->CanAddFunctionOptionsType(options_type, allow_overwrite);
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
  RegisterScalarIfElse(registry.get());
  RegisterScalarNested(registry.get());
  RegisterScalarRandom(registry.get());  // Nullary
  RegisterScalarSetLookup(registry.get());
  RegisterScalarStringAscii(registry.get());
  RegisterScalarStringUtf8(registry.get());
  RegisterScalarTemporalBinary(registry.get());
  RegisterScalarTemporalUnary(registry.get());
  RegisterScalarValidity(registry.get());

  RegisterScalarOptions(registry.get());

  // Vector functions
  RegisterVectorArraySort(registry.get());
  RegisterVectorCumulativeSum(registry.get());
  RegisterVectorHash(registry.get());
  RegisterVectorNested(registry.get());
  RegisterVectorReplace(registry.get());
  RegisterVectorSelection(registry.get());
  RegisterVectorSort(registry.get());
  RegisterVectorRunLengthEncode(registry.get());
  RegisterVectorRunLengthDecode(registry.get());

  RegisterVectorOptions(registry.get());

  // Aggregate functions
  RegisterHashAggregateBasic(registry.get());
  RegisterScalarAggregateBasic(registry.get());
  RegisterScalarAggregateMode(registry.get());
  RegisterScalarAggregateQuantile(registry.get());
  RegisterScalarAggregateTDigest(registry.get());
  RegisterScalarAggregateVariance(registry.get());

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
