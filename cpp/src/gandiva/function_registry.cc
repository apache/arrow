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

#include <utility>
#include <vector>

#include <llvm/Support/MemoryBuffer.h>

#include "arrow/util/logging.h"
#include "gandiva/function_registry_arithmetic.h"
#include "gandiva/function_registry_datetime.h"
#include "gandiva/function_registry_hash.h"
#include "gandiva/function_registry_math_ops.h"
#include "gandiva/function_registry_string.h"
#include "gandiva/function_registry_timestamp_arithmetic.h"

namespace gandiva {

static constexpr uint32_t kMaxFunctionSignatures = 2048;

// encapsulates an llvm memory buffer in an arrow buffer
// this is needed because we don't expose the llvm memory buffer to the outside world in
// the header file
class LLVMMemoryArrowBuffer : public arrow::Buffer {
 public:
  explicit LLVMMemoryArrowBuffer(std::unique_ptr<llvm::MemoryBuffer> llvm_buffer)
      : arrow::Buffer(reinterpret_cast<const uint8_t*>(llvm_buffer->getBufferStart()),
                      static_cast<int64_t>(llvm_buffer->getBufferSize())),
        llvm_buffer_(std::move(llvm_buffer)) {}

 private:
  std::unique_ptr<llvm::MemoryBuffer> llvm_buffer_;
};

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

  return std::move(buffer_or_error.get());
}

Status FunctionRegistry::Register(const std::vector<NativeFunction>& funcs,
                                  const std::string& bitcode_path) {
  ARROW_ASSIGN_OR_RAISE(auto llvm_buffer, GetBufferFromFile(bitcode_path));
  auto buffer = std::make_shared<LLVMMemoryArrowBuffer>(std::move(llvm_buffer));
  return Register(funcs, std::move(buffer));
}

arrow::Status FunctionRegistry::Register(const std::vector<NativeFunction>& funcs,
                                         std::shared_ptr<arrow::Buffer> bitcode_buffer) {
  bitcode_memory_buffers_.emplace_back(std::move(bitcode_buffer));
  for (const auto& func : funcs) {
    ARROW_RETURN_NOT_OK(FunctionRegistry::Add(func));
  }
  return Status::OK();
}

const std::vector<std::shared_ptr<arrow::Buffer>>& FunctionRegistry::GetBitcodeBuffers()
    const {
  return bitcode_memory_buffers_;
}

arrow::Result<std::shared_ptr<FunctionRegistry>> MakeDefaultFunctionRegistry() {
  auto registry = std::make_shared<FunctionRegistry>();
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

std::shared_ptr<FunctionRegistry> default_function_registry() {
  static auto default_registry = *MakeDefaultFunctionRegistry();
  return default_registry;
}

}  // namespace gandiva
