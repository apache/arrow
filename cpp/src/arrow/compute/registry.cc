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
  virtual ~FunctionRegistryImpl() {}

 private:
  using FuncAdd = std::function<void(const std::string&, std::shared_ptr<Function>)>;

  const FuncAdd kFuncAddNoOp = [](const std::string& name,
                                  std::shared_ptr<Function> func) {};
  const FuncAdd kFuncAddDo = [this](const std::string& name,
                                    std::shared_ptr<Function> func) {
    name_to_function_[name] = func;
  };

  Status DoAddFunction(std::shared_ptr<Function> function, bool allow_overwrite,
                       FuncAdd add) {
#ifndef NDEBUG
    // This validates docstrings extensively, so don't waste time on it
    // in release builds.
    RETURN_NOT_OK(function->Validate());
#endif

    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string& name = function->name();
    auto it = name_to_function_.find(name);
    if (it != name_to_function_.end() && !allow_overwrite) {
      return Status::KeyError("Already have a function registered with name: ", name);
    }
    add(name, std::move(function));
    return Status::OK();
  }

 public:
  virtual Status CanAddFunction(std::shared_ptr<Function> function,
                                bool allow_overwrite) {
    return DoAddFunction(function, allow_overwrite, kFuncAddNoOp);
  }

  virtual Status AddFunction(std::shared_ptr<Function> function, bool allow_overwrite) {
    return DoAddFunction(function, allow_overwrite, kFuncAddDo);
  }

 private:
  Status DoAddAlias(const std::string& target_name, const std::string& source_name,
                    FuncAdd add) {
    std::lock_guard<std::mutex> mutation_guard(lock_);

    auto func_res = GetFunction(source_name);  // must not acquire the mutex
    if (!func_res.ok()) {
      return Status::KeyError("No function registered with name: ", source_name);
    }
    add(target_name, func_res.ValueOrDie());
    return Status::OK();
  }

 public:
  virtual Status CanAddAlias(const std::string& target_name,
                             const std::string& source_name) {
    return DoAddAlias(target_name, source_name, kFuncAddNoOp);
  }

  virtual Status AddAlias(const std::string& target_name,
                          const std::string& source_name) {
    return DoAddAlias(target_name, source_name, kFuncAddDo);
  }

 private:
  using FuncOptTypeAdd = std::function<void(const FunctionOptionsType* options_type)>;

  const FuncOptTypeAdd kFuncOptTypeAddNoOp = [](const FunctionOptionsType* options_type) {
  };
  const FuncOptTypeAdd kFuncOptTypeAddDo =
      [this](const FunctionOptionsType* options_type) {
        name_to_options_type_[options_type->type_name()] = options_type;
      };

  Status DoAddFunctionOptionsType(const FunctionOptionsType* options_type,
                                  bool allow_overwrite, FuncOptTypeAdd add) {
    std::lock_guard<std::mutex> mutation_guard(lock_);

    const std::string name = options_type->type_name();
    auto it = name_to_options_type_.find(name);
    if (it != name_to_options_type_.end() && !allow_overwrite) {
      return Status::KeyError(
          "Already have a function options type registered with name: ", name);
    }
    add(options_type);
    return Status::OK();
  }

 public:
  virtual Status CanAddFunctionOptionsType(const FunctionOptionsType* options_type,
                                           bool allow_overwrite = false) {
    return DoAddFunctionOptionsType(options_type, allow_overwrite, kFuncOptTypeAddNoOp);
  }

  virtual Status AddFunctionOptionsType(const FunctionOptionsType* options_type,
                                        bool allow_overwrite = false) {
    return DoAddFunctionOptionsType(options_type, allow_overwrite, kFuncOptTypeAddDo);
  }

  virtual Result<std::shared_ptr<Function>> GetFunction(const std::string& name) const {
    auto it = name_to_function_.find(name);
    if (it == name_to_function_.end()) {
      return Status::KeyError("No function registered with name: ", name);
    }
    return it->second;
  }

  virtual std::vector<std::string> GetFunctionNames() const {
    std::vector<std::string> results;
    for (auto it : name_to_function_) {
      results.push_back(it.first);
    }
    std::sort(results.begin(), results.end());
    return results;
  }

  virtual Result<const FunctionOptionsType*> GetFunctionOptionsType(
      const std::string& name) const {
    auto it = name_to_options_type_.find(name);
    if (it == name_to_options_type_.end()) {
      return Status::KeyError("No function options type registered with name: ", name);
    }
    return it->second;
  }

  virtual int num_functions() const { return static_cast<int>(name_to_function_.size()); }

 private:
  std::mutex lock_;
  std::unordered_map<std::string, std::shared_ptr<Function>> name_to_function_;
  std::unordered_map<std::string, const FunctionOptionsType*> name_to_options_type_;
};

class FunctionRegistry::NestedFunctionRegistryImpl
    : public FunctionRegistry::FunctionRegistryImpl {
 public:
  explicit NestedFunctionRegistryImpl(FunctionRegistry::FunctionRegistryImpl* parent)
      : parent_(parent) {}

  Status CanAddFunction(std::shared_ptr<Function> function,
                        bool allow_overwrite) override {
    return parent_->CanAddFunction(function, allow_overwrite) &
           FunctionRegistry::FunctionRegistryImpl::CanAddFunction(function,
                                                                  allow_overwrite);
  }

  Status AddFunction(std::shared_ptr<Function> function, bool allow_overwrite) override {
    return parent_->CanAddFunction(function, allow_overwrite) &
           FunctionRegistry::FunctionRegistryImpl::AddFunction(function, allow_overwrite);
  }

  Status CanAddAlias(const std::string& target_name,
                     const std::string& source_name) override {
    Status st =
        FunctionRegistry::FunctionRegistryImpl::CanAddAlias(target_name, source_name);
    return st.ok() ? st : parent_->CanAddAlias(target_name, source_name);
  }

  Status AddAlias(const std::string& target_name,
                  const std::string& source_name) override {
    Status st =
        FunctionRegistry::FunctionRegistryImpl::AddAlias(target_name, source_name);
    return st.ok() ? st : parent_->AddAlias(target_name, source_name);
  }

  Status CanAddFunctionOptionsType(const FunctionOptionsType* options_type,
                                   bool allow_overwrite = false) override {
    return parent_->CanAddFunctionOptionsType(options_type, allow_overwrite) &
           FunctionRegistry::FunctionRegistryImpl::CanAddFunctionOptionsType(
               options_type, allow_overwrite);
  }

  Status AddFunctionOptionsType(const FunctionOptionsType* options_type,
                                bool allow_overwrite = false) override {
    return parent_->CanAddFunctionOptionsType(options_type, allow_overwrite) &
           FunctionRegistry::FunctionRegistryImpl::AddFunctionOptionsType(
               options_type, allow_overwrite);
  }

  Result<std::shared_ptr<Function>> GetFunction(const std::string& name) const override {
    auto func_res = FunctionRegistry::FunctionRegistryImpl::GetFunction(name);
    if (func_res.ok()) {
      return func_res;
    }
    return parent_->GetFunction(name);
  }

  std::vector<std::string> GetFunctionNames() const override {
    auto names = parent_->GetFunctionNames();
    auto more_names = FunctionRegistry::FunctionRegistryImpl::GetFunctionNames();
    names.insert(names.end(), std::make_move_iterator(more_names.begin()),
                 std::make_move_iterator(more_names.end()));
    return names;
  }

  Result<const FunctionOptionsType*> GetFunctionOptionsType(
      const std::string& name) const override {
    auto options_type_res =
        FunctionRegistry::FunctionRegistryImpl::GetFunctionOptionsType(name);
    if (options_type_res.ok()) {
      return options_type_res;
    }
    return parent_->GetFunctionOptionsType(name);
  }

  int num_functions() const override {
    return parent_->num_functions() +
           FunctionRegistry::FunctionRegistryImpl::num_functions();
  }

 private:
  FunctionRegistry::FunctionRegistryImpl* parent_;
};

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make() {
  return std::unique_ptr<FunctionRegistry>(new FunctionRegistry());
}

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make(FunctionRegistry* parent) {
  return std::unique_ptr<FunctionRegistry>(new FunctionRegistry(
      new FunctionRegistry::NestedFunctionRegistryImpl(&*parent->impl_)));
}

std::unique_ptr<FunctionRegistry> FunctionRegistry::Make(
    std::unique_ptr<FunctionRegistry> parent) {
  return FunctionRegistry::Make(&*parent);
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
