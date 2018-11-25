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

#ifndef GANDIVA_LVALUE_H
#define GANDIVA_LVALUE_H

#include "arrow/util/macros.h"

#include <llvm/IR/IRBuilder.h>

namespace gandiva {

/// \brief Tracks validity/value builders in LLVM.
class LValue {
 public:
  explicit LValue(llvm::Value* data, llvm::Value* length = NULLPTR,
                  llvm::Value* validity = NULLPTR)
      : data_(data), length_(length), validity_(validity) {}

  llvm::Value* data() { return data_; }
  llvm::Value* length() { return length_; }
  llvm::Value* validity() { return validity_; }

 private:
  llvm::Value* data_;
  llvm::Value* length_;
  llvm::Value* validity_;
};

}  // namespace gandiva

#endif  // GANDIVA_LVALUE_H
