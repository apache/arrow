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

#include "gandiva/function_registry.h"

#include <iterator>
#include <utility>
#include <vector>

#include "arrow/util/logging.h"
#include "gandiva/function_registry_arithmetic.h"
#include "gandiva/function_registry_datetime.h"
#include "gandiva/function_registry_hash.h"
#include "gandiva/function_registry_math_ops.h"
#include "gandiva/function_registry_string.h"
#include "gandiva/function_registry_timestamp_arithmetic.h"

namespace gandiva {
constexpr uint32_t kMaxFunctionSignatures = 2048;

FunctionRegistry::FunctionRegistry() { pc_registry_.reserve(kMaxFunctionSignatures); }

FunctionRegistry::iterator FunctionRegistry::begin() const {
  return &(*pc_registry_.begin());
}

FunctionRegistry::iterator FunctionRegistry::end() const {
  return &(*pc_registry_.end());
}

FunctionRegistry::iterator FunctionRegistry::back() const {
  return &(pc_registry_.back());
}

const NativeFunction* FunctionRegistry::LookupSignature(
    const FunctionSignature& signature) const {
  auto got = pc_registry_map_.find(&signature);
  return got == pc_registry_map_.end() ? nullptr : got->second;
}

Status FunctionRegistry::Add(NativeFunction func) {
  if (pc_registry_.size() == kMaxFunctionSignatures) {
    return Status::CapacityError("Exceeded max function signatures limit of ",
                                 kMaxFunctionSignatures);
  }
  pc_registry_.emplace_back(std::move(func));
  auto const& last_func = pc_registry_.back();
  for (auto const& func_signature : last_func.signatures()) {
    pc_registry_map_.emplace(&func_signature, &last_func);
  }
  return arrow::Status::OK();
}

arrow::Result<std::unique_ptr<llvm::MemoryBuffer>> GetBufferFromFile(
    const std::string& bitcode_file_path) {
  auto buffer_or_error = llvm::MemoryBuffer::getFile(bitcode_file_path);

  ARROW_RETURN_IF(!buffer_or_error,
                  Status::IOError("Could not load module from bitcode file: ",
                                  bitcode_file_path +
                                      " Error: " + buffer_or_error.getError().message()));

  auto buffer = std::move(buffer_or_error.get());
  return std::move(buffer);
}

Status FunctionRegistry::Register(const std::vector<NativeFunction>& funcs,
                                  const std::string& bitcode_path) {
  ARROW_ASSIGN_OR_RAISE(auto buffer, GetBufferFromFile(bitcode_path));
  return Register(funcs, std::move(buffer));
}

arrow::Status FunctionRegistry::Register(
    const std::vector<NativeFunction>& funcs,
    std::unique_ptr<llvm::MemoryBuffer> bitcode_buffer) {
  bitcode_memory_buffers_.emplace_back(std::move(bitcode_buffer));
  for (const auto& func : funcs) {
    ARROW_RETURN_NOT_OK(FunctionRegistry::Add(func));
  }
  return Status::OK();
}

const std::vector<std::unique_ptr<llvm::MemoryBuffer>>&
FunctionRegistry::GetBitcodeBuffers() const {
  return bitcode_memory_buffers_;
}

arrow::Result<std::unique_ptr<FunctionRegistry>> MakeDefaultFunctionRegistry() {
  auto registry = std::make_unique<FunctionRegistry>();
  for (auto const& funcs :
       {GetArithmeticFunctionRegistry(), GetDateTimeFunctionRegistry(),
        GetHashFunctionRegistry(), GetMathOpsFunctionRegistry(),
        GetStringFunctionRegistry(), GetDateTimeArithmeticFunctionRegistry()}) {
    for (auto const& func_signature : funcs) {
      ARROW_RETURN_NOT_OK(registry->Add(func_signature));
    }
  }
  return std::move(registry);
}

FunctionRegistry* default_function_registry() {
  static auto maybe_default_registry = MakeDefaultFunctionRegistry();
  if (!maybe_default_registry.ok()) {
    ARROW_LOG(FATAL) << "Failed to initialize default function registry: "
                     << maybe_default_registry.status().message();
    return nullptr;
  }
  return (*maybe_default_registry).get();
}

}  // namespace gandiva
