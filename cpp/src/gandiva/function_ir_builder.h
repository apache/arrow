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

#ifndef GANDIVA_FUNCTION_IR_BUILDER_H
#define GANDIVA_FUNCTION_IR_BUILDER_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gandiva/engine.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/llvm_types.h"

namespace gandiva {

/// @brief Base class for building IR functions.
class FunctionIRBuilder {
 public:
  explicit FunctionIRBuilder(Engine* engine) : engine_(engine) {}
  virtual ~FunctionIRBuilder() = default;

 protected:
  LLVMTypes* types() { return engine_->types(); }
  llvm::Module* module() { return engine_->module(); }
  llvm::LLVMContext* context() { return engine_->context(); }
  llvm::IRBuilder<>* ir_builder() { return engine_->ir_builder(); }

  /// Build an if-else block.
  llvm::Value* BuildIfElse(llvm::Value* condition, llvm::Type* return_type,
                           std::function<llvm::Value*()> then_func,
                           std::function<llvm::Value*()> else_func);

 private:
  Engine* engine_;
};

// TODO
#define DCHECK_IR_GE(v1, v2)
#define DCHECK_IR_LE(v1, v2)
#define DCHECK_IR_GT(v1, v2)
#define DCHECK_IR_LT(v1, v2)
#define DCHECK_IR_EQ(v1, v2)
#define DCHECK_IR_NE(v1, v2)

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_IR_BUILDER_H
