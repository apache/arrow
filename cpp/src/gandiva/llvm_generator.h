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

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/util/macros.h"
#include "gandiva/annotator.h"
#include "gandiva/compiled_expr.h"
#include "gandiva/configuration.h"
#include "gandiva/dex_visitor.h"
#include "gandiva/engine.h"
#include "gandiva/execution_context.h"
#include "gandiva/expr_decomposer.h"
#include "gandiva/expression_cache_key.h"
#include "gandiva/function_registry.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/llvm_types.h"
#include "gandiva/lvalue.h"
#include "gandiva/selection_vector.h"
#include "gandiva/value_validity_pair.h"
#include "gandiva/visibility.h"

namespace gandiva {

class FunctionHolder;

/// Builds an LLVM module and generates code for the specified set of expressions.
class GANDIVA_EXPORT LLVMGenerator {
 public:
  /// \brief Factory method to initialize the generator.
  static Result<std::unique_ptr<LLVMGenerator>> Make(
      const std::shared_ptr<Configuration>& config, bool cached,
      std::optional<std::reference_wrapper<GandivaObjectCache>> object_cache =
          std::nullopt);

  /// \brief Get the cache to be used for LLVM ObjectCache.
  static std::shared_ptr<Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>>
  GetCache();

  /// \brief Set LLVM ObjectCache.
  Status SetLLVMObjectCache(GandivaObjectCache& object_cache);

  /// \brief Build the code for the expression trees for default mode with a LLVM
  /// ObjectCache. Each element in the vector represents an expression tree
  Status Build(const ExpressionVector& exprs, SelectionVector::Mode mode);

  /// \brief Build the code for the expression trees for default mode. Each
  /// element in the vector represents an expression tree
  Status Build(const ExpressionVector& exprs);

  /// \brief Execute the built expression against the provided arguments for
  /// default mode.
  Status Execute(const arrow::RecordBatch& record_batch,
                 const ArrayDataVector& output_vector) const;

  /// \brief Execute the built expression against the provided arguments for
  /// all modes. Only works on the records specified in the selection_vector.
  Status Execute(const arrow::RecordBatch& record_batch,
                 const SelectionVector* selection_vector,
                 const ArrayDataVector& output_vector) const;

  SelectionVector::Mode selection_vector_mode() { return selection_vector_mode_; }
  LLVMTypes* types() { return engine_->types(); }
  llvm::Module* module() { return engine_->module(); }
  const std::string& ir() { return engine_->ir(); }

 private:
  explicit LLVMGenerator(bool cached,
                         std::shared_ptr<FunctionRegistry> function_registry);

  FRIEND_TEST(TestLLVMGenerator, VerifyPCFunctions);
  FRIEND_TEST(TestLLVMGenerator, TestAdd);
  FRIEND_TEST(TestLLVMGenerator, TestNullInternal);
  friend class TestLLVMGenerator;

  llvm::LLVMContext* context() { return engine_->context(); }
  llvm::IRBuilder<>* ir_builder() { return engine_->ir_builder(); }

  /// Visitor to generate the code for a decomposed expression.
  class Visitor : public DexVisitor {
   public:
    Visitor(LLVMGenerator* generator, llvm::Function* function,
            llvm::BasicBlock* entry_block, llvm::Value* arg_addrs,
            llvm::Value* arg_local_bitmaps, llvm::Value* arg_holder_ptrs,
            std::vector<llvm::Value*> slice_offsets, llvm::Value* arg_context_ptr,
            llvm::Value* loop_var);

    void Visit(const VectorReadValidityDex& dex) override;
    void Visit(const VectorReadFixedLenValueDex& dex) override;
    void Visit(const VectorReadVarLenValueDex& dex) override;
    void Visit(const LocalBitMapValidityDex& dex) override;
    void Visit(const TrueDex& dex) override;
    void Visit(const FalseDex& dex) override;
    void Visit(const LiteralDex& dex) override;
    void Visit(const NonNullableFuncDex& dex) override;
    void Visit(const NullableNeverFuncDex& dex) override;
    void Visit(const NullableInternalFuncDex& dex) override;
    void Visit(const IfDex& dex) override;
    void Visit(const BooleanAndDex& dex) override;
    void Visit(const BooleanOrDex& dex) override;
    void Visit(const InExprDexBase<int32_t>& dex) override;
    void Visit(const InExprDexBase<int64_t>& dex) override;
    void Visit(const InExprDexBase<float>& dex) override;
    void Visit(const InExprDexBase<double>& dex) override;
    void Visit(const InExprDexBase<gandiva::DecimalScalar128>& dex) override;
    void Visit(const InExprDexBase<std::string>& dex) override;
    template <typename Type>
    void VisitInExpression(const InExprDexBase<Type>& dex);

    LValuePtr result() { return result_; }

    bool has_arena_allocs() { return has_arena_allocs_; }

   private:
    enum BufferType { kBufferTypeValidity = 0, kBufferTypeData, kBufferTypeOffsets };

    llvm::IRBuilder<>* ir_builder() { return generator_->ir_builder(); }
    llvm::Module* module() { return generator_->module(); }

    // Generate the code to build the combined validity (bitwise and) from the
    // vector of validities.
    llvm::Value* BuildCombinedValidity(const DexVector& validities);

    // Generate the code to build the validity and the value for the given pair.
    LValuePtr BuildValueAndValidity(const ValueValidityPair& pair);

    // Generate code to build the params.
    std::vector<llvm::Value*> BuildParams(int holder_idx,
                                          const ValueValidityPairVector& args,
                                          bool with_validity, bool with_context);

    // Generate code to invoke a function call.
    LValuePtr BuildFunctionCall(const NativeFunction* func, DataTypePtr arrow_return_type,
                                std::vector<llvm::Value*>* params);

    // Generate code for an if-else condition.
    LValuePtr BuildIfElse(llvm::Value* condition, std::function<LValuePtr()> then_func,
                          std::function<LValuePtr()> else_func,
                          DataTypePtr arrow_return_type);

    // Switch to the entry_block and get reference of the validity/value/offsets buffer
    llvm::Value* GetBufferReference(int idx, BufferType buffer_type, FieldPtr field);

    // Get the slice offset of the validity/value/offsets buffer
    llvm::Value* GetSliceOffset(int idx);

    // Switch to the entry_block and get reference to the local bitmap.
    llvm::Value* GetLocalBitMapReference(int idx);

    // Clear the bit in the local bitmap, if is_valid is 'false'
    void ClearLocalBitMapIfNotValid(int local_bitmap_idx, llvm::Value* is_valid);

    LLVMGenerator* generator_;
    LValuePtr result_;
    llvm::Function* function_;
    llvm::BasicBlock* entry_block_;
    llvm::Value* arg_addrs_;
    llvm::Value* arg_local_bitmaps_;
    llvm::Value* arg_holder_ptrs_;
    std::vector<llvm::Value*> slice_offsets_;
    llvm::Value* arg_context_ptr_;
    llvm::Value* loop_var_;
    bool has_arena_allocs_;
  };

  // Generate the code for one expression for default mode, with the output of
  // the expression going to 'output'.
  Status Add(const ExpressionPtr expr, const FieldDescriptorPtr output);

  /// Generate code to load the vector at specified index in the 'arg_addrs' array.
  llvm::Value* LoadVectorAtIndex(llvm::Value* arg_addrs, llvm::Type* type, int idx,
                                 const std::string& name);

  /// Generate code to load the vector at specified index and cast it as bitmap.
  llvm::Value* GetValidityReference(llvm::Value* arg_addrs, int idx, FieldPtr field);

  /// Generate code to load the vector at specified index and cast it as data array.
  llvm::Value* GetDataReference(llvm::Value* arg_addrs, int idx, FieldPtr field);

  /// Generate code to load the vector at specified index and cast it as offsets array.
  llvm::Value* GetOffsetsReference(llvm::Value* arg_addrs, int idx, FieldPtr field);

  /// Generate code to load the vector at specified index and cast it as buffer pointer.
  llvm::Value* GetDataBufferPtrReference(llvm::Value* arg_addrs, int idx, FieldPtr field);

  /// Generate code for the value array of one expression.
  Status CodeGenExprValue(DexPtr value_expr, int num_buffers, FieldDescriptorPtr output,
                          int suffix_idx, std::string& fn_name,
                          SelectionVector::Mode selection_vector_mode);

  /// Generate code to load the local bitmap specified index and cast it as bitmap.
  llvm::Value* GetLocalBitMapReference(llvm::Value* arg_bitmaps, int idx);

  /// Generate code to get the bit value at 'position' in the bitmap.
  llvm::Value* GetPackedBitValue(llvm::Value* bitmap, llvm::Value* position);

  /// Generate code to get the bit value at 'position' in the validity bitmap.
  llvm::Value* GetPackedValidityBitValue(llvm::Value* bitmap, llvm::Value* position);

  /// Generate code to set the bit value at 'position' in the bitmap to 'value'.
  void SetPackedBitValue(llvm::Value* bitmap, llvm::Value* position, llvm::Value* value);

  /// Generate code to clear the bit value at 'position' in the bitmap if 'value'
  /// is false.
  void ClearPackedBitValueIfFalse(llvm::Value* bitmap, llvm::Value* position,
                                  llvm::Value* value);

  // Generate code to build a DecimalLValue with specified value/precision/scale.
  std::shared_ptr<DecimalLValue> BuildDecimalLValue(llvm::Value* value,
                                                    DataTypePtr arrow_type);

  /// Generate code to make a function call (to a pre-compiled IR function) which takes
  /// 'args' and has a return type 'ret_type'.
  llvm::Value* AddFunctionCall(const std::string& full_name, llvm::Type* ret_type,
                               const std::vector<llvm::Value*>& args);

  /// Compute the result bitmap for the expression.
  ///
  /// \param[in] compiled_expr the compiled expression (includes the bitmap indices to be
  ///            used for computing the validity bitmap of the result).
  /// \param[in] selection_vector the list of selected positions
  /// \param[in,out] eval_batch (includes input/output buffer addresses)
  void ComputeBitMapsForExpr(const CompiledExpr& compiled_expr,
                             const SelectionVector* selection_vector,
                             EvalBatch* eval_batch) const;

  /// Replace the %T in the trace msg with the correct type corresponding to 'type'
  /// eg. %d for int32, %ld for int64, ..
  std::string ReplaceFormatInTrace(const std::string& msg, llvm::Value* value,
                                   std::string* print_fn);

  /// Generate the code to print a trace msg with one optional argument (%T)
  void AddTrace(const std::string& msg, llvm::Value* value = NULLPTR);

  std::unique_ptr<Engine> engine_;
  std::vector<std::unique_ptr<CompiledExpr>> compiled_exprs_;
  bool cached_;
  std::shared_ptr<FunctionRegistry> function_registry_;
  Annotator annotator_;
  SelectionVector::Mode selection_vector_mode_;

  // used for debug
  bool enable_ir_traces_;
  std::vector<std::string> trace_strings_;
};

}  // namespace gandiva
