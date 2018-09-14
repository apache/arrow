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

#ifndef GANDIVA_COMPILED_EXPR_H
#define GANDIVA_COMPILED_EXPR_H

#include <llvm/IR/IRBuilder.h>
#include "gandiva/value_validity_pair.h"

namespace gandiva {

using EvalFunc = int (*)(uint8_t** buffers, uint8_t** local_bitmaps, int record_count);

/// \brief Tracks the compiled state for one expression.
class CompiledExpr {
 public:
  CompiledExpr(ValueValidityPairPtr value_validity, FieldDescriptorPtr output,
               llvm::Function* ir_function)
      : value_validity_(value_validity),
        output_(output),
        ir_function_(ir_function),
        jit_function_(NULL) {}

  ValueValidityPairPtr value_validity() const { return value_validity_; }

  FieldDescriptorPtr output() const { return output_; }

  llvm::Function* ir_function() const { return ir_function_; }

  EvalFunc jit_function() const { return jit_function_; }

  void set_jit_function(EvalFunc jit_function) { jit_function_ = jit_function; }

 private:
  // value & validities for the expression tree (root)
  ValueValidityPairPtr value_validity_;

  // output field
  FieldDescriptorPtr output_;

  // IR function in the generated code
  llvm::Function* ir_function_;

  // JIT function in the generated code (set after the module is optimised and finalized)
  EvalFunc jit_function_;
};

}  // namespace gandiva

#endif  // GANDIVA_COMPILED_EXPR_H
