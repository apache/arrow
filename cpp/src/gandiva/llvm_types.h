/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef GANDIVA_LLVM_TYPES_H
#define GANDIVA_LLVM_TYPES_H

#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <map>
#include "gandiva/arrow.h"

namespace gandiva {

/// \brief Holder for llvm types, and mappings between arrow types and llvm types.
class LLVMTypes {
 public:
  explicit LLVMTypes(llvm::LLVMContext &context);

  llvm::Type *i1_type() {
    return llvm::Type::getInt1Ty(context_);
  }

  llvm::Type *i8_type() {
    return llvm::Type::getInt8Ty(context_);
  }

  llvm::Type *i32_type() {
    return llvm::Type::getInt32Ty(context_);
  }

  llvm::Type *i64_type() {
    return llvm::Type::getInt64Ty(context_);
  }

  llvm::Type *float_type() {
    return llvm::Type::getFloatTy(context_);
  }

  llvm::Type *double_type() {
    return llvm::Type::getDoubleTy(context_);
  }

  llvm::PointerType *i32_ptr_type() {
    return llvm::PointerType::get(i32_type(), 0);
  }

  llvm::PointerType *i64_ptr_type() {
    return llvm::PointerType::get(i64_type(), 0);
  }

  llvm::PointerType *ptr_type(llvm::Type *base_type) {
    return llvm::PointerType::get(base_type, 0);
  }

  llvm::Type *void_type() { return llvm::Type::getVoidTy(context_); }

  llvm::Constant *true_constant() {
    return llvm::ConstantInt::get(context_, llvm::APInt(1, 1));
  }

  llvm::Constant *false_constant() {
    return llvm::ConstantInt::get(context_, llvm::APInt(1, 0));
  }

  llvm::Constant *i1_constant(bool val) {
    return llvm::ConstantInt::get(context_, llvm::APInt(1, val));
  }

  llvm::Constant *i32_constant(int32_t val) {
    return llvm::ConstantInt::get(context_, llvm::APInt(32, val));
  }

  llvm::Constant *i64_constant(int64_t val) {
    return llvm::ConstantInt::get(context_, llvm::APInt(64, val));
  }

  llvm::Constant *float_constant(float val) {
    return llvm::ConstantFP::get(float_type(), val);
  }

  llvm::Constant *double_constant(double val) {
    return llvm::ConstantFP::get(double_type(), val);
  }

  /*
   * For a given data type, find the ir type used for the data vector slot.
   */
  llvm::Type *DataVecType(const DataTypePtr &data_type) {
    return IRType(data_type->id());
  }

  /*
   * For a given minor type, find the corresponding ir type.
   */
  llvm::Type *IRType(arrow::Type::type arrow_type) {
    auto found = arrow_id_to_llvm_type_map_.find(arrow_type);
    return (found == arrow_id_to_llvm_type_map_.end()) ? NULL : found->second;
  }

 private:
  std::map<arrow::Type::type, llvm::Type *> arrow_id_to_llvm_type_map_;

  llvm::LLVMContext &context_;
};

} // namespace gandiva

#endif //GANDIVA_LLVM_TYPES_H
