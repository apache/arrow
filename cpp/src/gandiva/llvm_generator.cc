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

#include "gandiva/llvm_generator.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "gandiva/bitmap_accumulator.h"
#include "gandiva/dex.h"
#include "gandiva/expr_decomposer.h"
#include "gandiva/expression.h"
#include "gandiva/function_registry.h"
#include "gandiva/lvalue.h"

namespace gandiva {

#define ADD_TRACE(...)     \
  if (enable_ir_traces_) { \
    AddTrace(__VA_ARGS__); \
  }

LLVMGenerator::LLVMGenerator()
    : dump_ir_(false), optimise_ir_(true), enable_ir_traces_(false) {}

Status LLVMGenerator::Make(std::shared_ptr<Configuration> config,
                           std::unique_ptr<LLVMGenerator>* llvm_generator) {
  std::unique_ptr<LLVMGenerator> llvmgen_obj(new LLVMGenerator());
  Status status = Engine::Make(config, &(llvmgen_obj->engine_));
  ARROW_RETURN_NOT_OK(status);

  *llvm_generator = std::move(llvmgen_obj);
  return Status::OK();
}

Status LLVMGenerator::Add(const ExpressionPtr expr, const FieldDescriptorPtr output) {
  int idx = static_cast<int>(compiled_exprs_.size());

  // decompose the expression to separate out value and validities.
  ExprDecomposer decomposer(function_registry_, annotator_);
  ValueValidityPairPtr value_validity;
  auto status = decomposer.Decompose(*expr->root(), &value_validity);
  ARROW_RETURN_NOT_OK(status);

  // Generate the IR function for the decomposed expression.
  llvm::Function* ir_function = nullptr;
  status = CodeGenExprValue(value_validity->value_expr(), output, idx, &ir_function);
  ARROW_RETURN_NOT_OK(status);

  std::unique_ptr<CompiledExpr> compiled_expr(
      new CompiledExpr(value_validity, output, ir_function));
  compiled_exprs_.push_back(std::move(compiled_expr));
  return Status::OK();
}

/// Build and optimise module for projection expression.
Status LLVMGenerator::Build(const ExpressionVector& exprs) {
  Status status;

  for (auto& expr : exprs) {
    auto output = annotator_.AddOutputFieldDescriptor(expr->result());
    status = Add(expr, output);
    ARROW_RETURN_NOT_OK(status);
  }

  // optimise, compile and finalize the module
  status = engine_->FinalizeModule(optimise_ir_, dump_ir_);
  ARROW_RETURN_NOT_OK(status);

  // setup the jit functions for each expression.
  for (auto& compiled_expr : compiled_exprs_) {
    llvm::Function* ir_func = compiled_expr->ir_function();
    EvalFunc fn = reinterpret_cast<EvalFunc>(engine_->CompiledFunction(ir_func));
    compiled_expr->set_jit_function(fn);
  }
  return Status::OK();
}

/// Execute the compiled module against the provided vectors.
Status LLVMGenerator::Execute(const arrow::RecordBatch& record_batch,
                              const ArrayDataVector& output_vector) {
  DCHECK_GT(record_batch.num_rows(), 0);

  auto eval_batch = annotator_.PrepareEvalBatch(record_batch, output_vector);
  DCHECK_GT(eval_batch->GetNumBuffers(), 0);

  for (auto& compiled_expr : compiled_exprs_) {
    // generate data/offset vectors.
    EvalFunc jit_function = compiled_expr->jit_function();
    jit_function(eval_batch->GetBufferArray(), eval_batch->GetLocalBitMapArray(),
                 (int64_t)eval_batch->GetExecutionContext(), record_batch.num_rows());
    // check for execution errors
    if (eval_batch->GetExecutionContext()->has_error()) {
      return Status::ExecutionError(eval_batch->GetExecutionContext()->get_error());
    }
    // generate validity vectors.
    ComputeBitMapsForExpr(*compiled_expr, *eval_batch);
  }
  return Status::OK();
}

llvm::Value* LLVMGenerator::LoadVectorAtIndex(llvm::Value* arg_addrs, int idx,
                                              const std::string& name) {
  llvm::IRBuilder<>* builder = ir_builder();
  llvm::Value* offset =
      builder->CreateGEP(arg_addrs, types()->i32_constant(idx), name + "_mem_addr");
  return builder->CreateLoad(offset, name + "_mem");
}

/// Get reference to validity array at specified index in the args list.
llvm::Value* LLVMGenerator::GetValidityReference(llvm::Value* arg_addrs, int idx,
                                                 FieldPtr field) {
  const std::string& name = field->name();
  llvm::Value* load = LoadVectorAtIndex(arg_addrs, idx, name);
  return ir_builder()->CreateIntToPtr(load, types()->i64_ptr_type(), name + "_varray");
}

/// Get reference to data array at specified index in the args list.
llvm::Value* LLVMGenerator::GetDataReference(llvm::Value* arg_addrs, int idx,
                                             FieldPtr field) {
  const std::string& name = field->name();
  llvm::Value* load = LoadVectorAtIndex(arg_addrs, idx, name);
  llvm::Type* base_type = types()->DataVecType(field->type());
  llvm::Value* ret;
  if (base_type->isPointerTy()) {
    ret = ir_builder()->CreateIntToPtr(load, base_type, name + "_darray");
  } else {
    llvm::Type* pointer_type = types()->ptr_type(base_type);
    ret = ir_builder()->CreateIntToPtr(load, pointer_type, name + "_darray");
  }
  return ret;
}

/// Get reference to offsets array at specified index in the args list.
llvm::Value* LLVMGenerator::GetOffsetsReference(llvm::Value* arg_addrs, int idx,
                                                FieldPtr field) {
  const std::string& name = field->name();
  llvm::Value* load = LoadVectorAtIndex(arg_addrs, idx, name);
  return ir_builder()->CreateIntToPtr(load, types()->i32_ptr_type(), name + "_oarray");
}

/// Get reference to local bitmap array at specified index in the args list.
llvm::Value* LLVMGenerator::GetLocalBitMapReference(llvm::Value* arg_bitmaps, int idx) {
  llvm::Value* load = LoadVectorAtIndex(arg_bitmaps, idx, "");
  return ir_builder()->CreateIntToPtr(load, types()->i64_ptr_type(),
                                      std::to_string(idx) + "_lbmap");
}

/// \brief Generate code for one expression.

// Sample IR code for "c1:int + c2:int"
//
// The C-code equivalent is :
// ------------------------------
// int expr_0(int64_t *addrs, int64_t *local_bitmaps,
//            int64_t execution_context_ptr, int64_t nrecords) {
//   int *outVec = (int *) addrs[5];
//   int *c0Vec = (int *) addrs[1];
//   int *c1Vec = (int *) addrs[3];
//   for (int loop_var = 0; loop_var < nrecords; ++loop_var) {
//     int c0 = c0Vec[loop_var];
//     int c1 = c1Vec[loop_var];
//     int out = c0 + c1;
//     outVec[loop_var] = out;
//   }
// }
//
// IR Code
// --------
//
// define i32 @expr_0(i64* %args, i64* %local_bitmaps, i64 %execution_context_ptr, , i64
// %nrecords) { entry:
//   %outmemAddr = getelementptr i64, i64* %args, i32 5
//   %outmem = load i64, i64* %outmemAddr
//   %outVec = inttoptr i64 %outmem to i32*
//   %c0memAddr = getelementptr i64, i64* %args, i32 1
//   %c0mem = load i64, i64* %c0memAddr
//   %c0Vec = inttoptr i64 %c0mem to i32*
//   %c1memAddr = getelementptr i64, i64* %args, i32 3
//   %c1mem = load i64, i64* %c1memAddr
//   %c1Vec = inttoptr i64 %c1mem to i32*
//   br label %loop
// loop:                                             ; preds = %loop, %entry
//   %loop_var = phi i64 [ 0, %entry ], [ %"loop_var+1", %loop ]
//   %"loop_var+1" = add i64 %loop_var, 1
//   %0 = getelementptr i32, i32* %c0Vec, i32 %loop_var
//   %c0 = load i32, i32* %0
//   %1 = getelementptr i32, i32* %c1Vec, i32 %loop_var
//   %c1 = load i32, i32* %1
//   %add_int_int = call i32 @add_int_int(i32 %c0, i32 %c1)
//   %2 = getelementptr i32, i32* %outVec, i32 %loop_var
//   store i32 %add_int_int, i32* %2
//   %"loop_var < nrec" = icmp slt i64 %"loop_var+1", %nrecords
//   br i1 %"loop_var < nrec", label %loop, label %exit
// exit:                                             ; preds = %loop
//   ret i32 0
// }

Status LLVMGenerator::CodeGenExprValue(DexPtr value_expr, FieldDescriptorPtr output,
                                       int suffix_idx, llvm::Function** fn) {
  llvm::IRBuilder<>* builder = ir_builder();

  // Create fn prototype :
  //   int expr_1 (long **addrs, long **bitmaps, long *context_ptr, long nrec)
  std::vector<llvm::Type*> arguments;
  arguments.push_back(types()->i64_ptr_type());
  arguments.push_back(types()->i64_ptr_type());
  arguments.push_back(types()->i64_type());
  arguments.push_back(types()->i64_type());
  llvm::FunctionType* prototype =
      llvm::FunctionType::get(types()->i32_type(), arguments, false /*isVarArg*/);

  // Create fn
  std::string func_name = "expr_" + std::to_string(suffix_idx);
  engine_->AddFunctionToCompile(func_name);
  *fn = llvm::Function::Create(prototype, llvm::GlobalValue::ExternalLinkage, func_name,
                               module());
  ARROW_RETURN_FAILURE_IF_FALSE((*fn != nullptr),
                                Status::CodeGenError("Error creating function."));
  // Name the arguments
  llvm::Function::arg_iterator args = (*fn)->arg_begin();
  llvm::Value* arg_addrs = &*args;
  arg_addrs->setName("args");
  ++args;
  llvm::Value* arg_local_bitmaps = &*args;
  arg_local_bitmaps->setName("local_bitmaps");
  ++args;
  llvm::Value* arg_context_ptr = &*args;
  arg_context_ptr->setName("context_ptr");
  ++args;
  llvm::Value* arg_nrecords = &*args;
  arg_nrecords->setName("nrecords");

  llvm::BasicBlock* loop_entry = llvm::BasicBlock::Create(*context(), "entry", *fn);
  llvm::BasicBlock* loop_body = llvm::BasicBlock::Create(*context(), "loop", *fn);
  llvm::BasicBlock* loop_exit = llvm::BasicBlock::Create(*context(), "exit", *fn);

  // Add reference to output vector (in entry block)
  builder->SetInsertPoint(loop_entry);
  llvm::Value* output_ref =
      GetDataReference(arg_addrs, output->data_idx(), output->field());

  // Loop body
  builder->SetInsertPoint(loop_body);

  // define loop_var : start with 0, +1 after each iter
  llvm::PHINode* loop_var = builder->CreatePHI(types()->i64_type(), 2, "loop_var");

  // The visitor can add code to both the entry/loop blocks.
  Visitor visitor(this, *fn, loop_entry, arg_addrs, arg_local_bitmaps, arg_context_ptr,
                  loop_var);
  value_expr->Accept(visitor);
  LValuePtr output_value = visitor.result();

  // The "current" block may have changed due to code generation in the visitor.
  llvm::BasicBlock* loop_body_tail = builder->GetInsertBlock();

  // add jump to "loop block" at the end of the "setup block".
  builder->SetInsertPoint(loop_entry);
  builder->CreateBr(loop_body);

  // save the value in the output vector.
  builder->SetInsertPoint(loop_body_tail);
  if (output->Type()->id() == arrow::Type::BOOL) {
    SetPackedBitValue(output_ref, loop_var, output_value->data());
  } else {
    llvm::Value* slot_offset = builder->CreateGEP(output_ref, loop_var);
    builder->CreateStore(output_value->data(), slot_offset);
  }
  ADD_TRACE("saving result " + output->Name() + " value %T", output_value->data());

  if (visitor.has_arena_allocs()) {
    // Reset allocations to avoid excessive memory usage. Once the result is copied to
    // the output vector (store instruction above), any memory allocations in this
    // iteration of the loop are no longer needed.
    std::vector<llvm::Value*> reset_args;
    reset_args.push_back(arg_context_ptr);
    AddFunctionCall("gdv_fn_context_arena_reset", types()->void_type(), reset_args);
  }

  // check loop_var
  loop_var->addIncoming(types()->i64_constant(0), loop_entry);
  llvm::Value* loop_update =
      builder->CreateAdd(loop_var, types()->i64_constant(1), "loop_var+1");
  loop_var->addIncoming(loop_update, loop_body_tail);

  llvm::Value* loop_var_check =
      builder->CreateICmpSLT(loop_update, arg_nrecords, "loop_var < nrec");
  builder->CreateCondBr(loop_var_check, loop_body, loop_exit);

  // Loop exit
  builder->SetInsertPoint(loop_exit);
  builder->CreateRet(types()->i32_constant(0));
  return Status::OK();
}

/// Return value of a bit in bitMap.
llvm::Value* LLVMGenerator::GetPackedBitValue(llvm::Value* bitmap,
                                              llvm::Value* position) {
  ADD_TRACE("fetch bit at position %T", position);

  llvm::Value* bitmap8 = ir_builder()->CreateBitCast(
      bitmap, types()->ptr_type(types()->i8_type()), "bitMapCast");
  return AddFunctionCall("bitMapGetBit", types()->i1_type(), {bitmap8, position});
}

/// Set the value of a bit in bitMap.
void LLVMGenerator::SetPackedBitValue(llvm::Value* bitmap, llvm::Value* position,
                                      llvm::Value* value) {
  ADD_TRACE("set bit at position %T", position);
  ADD_TRACE("  to value %T ", value);

  llvm::Value* bitmap8 = ir_builder()->CreateBitCast(
      bitmap, types()->ptr_type(types()->i8_type()), "bitMapCast");
  AddFunctionCall("bitMapSetBit", types()->void_type(), {bitmap8, position, value});
}

/// Clear the bit in bitMap if value = false.
void LLVMGenerator::ClearPackedBitValueIfFalse(llvm::Value* bitmap, llvm::Value* position,
                                               llvm::Value* value) {
  ADD_TRACE("ClearIfFalse bit at position %T", position);
  ADD_TRACE("   value %T ", value);

  llvm::Value* bitmap8 = ir_builder()->CreateBitCast(
      bitmap, types()->ptr_type(types()->i8_type()), "bitMapCast");
  AddFunctionCall("bitMapClearBitIfFalse", types()->void_type(),
                  {bitmap8, position, value});
}

/// Extract the bitmap addresses, and do an intersection.
void LLVMGenerator::ComputeBitMapsForExpr(const CompiledExpr& compiled_expr,
                                          const EvalBatch& eval_batch) {
  auto validities = compiled_expr.value_validity()->validity_exprs();

  // Extract all the source bitmap addresses.
  BitMapAccumulator accumulator(eval_batch);
  for (auto& validity_dex : validities) {
    validity_dex->Accept(accumulator);
  }

  // Extract the destination bitmap address.
  int out_idx = compiled_expr.output()->validity_idx();
  uint8_t* dst_bitmap = eval_batch.GetBuffer(out_idx);

  // Compute the destination bitmap.
  accumulator.ComputeResult(dst_bitmap);
}

llvm::Value* LLVMGenerator::AddFunctionCall(const std::string& full_name,
                                            llvm::Type* ret_type,
                                            const std::vector<llvm::Value*>& args) {
  // find the llvm function.
  llvm::Function* fn = module()->getFunction(full_name);
  DCHECK_NE(fn, nullptr) << "missing function " << full_name;

  if (enable_ir_traces_ && !full_name.compare("printf") &&
      !full_name.compare("printff")) {
    // Trace for debugging
    ADD_TRACE("invoke native fn " + full_name);
  }

  // build a call to the llvm function.
  llvm::Value* value;
  if (ret_type->isVoidTy()) {
    // void functions can't have a name for the call.
    value = ir_builder()->CreateCall(fn, args);
  } else {
    value = ir_builder()->CreateCall(fn, args, full_name);
    DCHECK(value->getType() == ret_type);
  }
  return value;
}

#define ADD_VISITOR_TRACE(...)         \
  if (generator_->enable_ir_traces_) { \
    generator_->AddTrace(__VA_ARGS__); \
  }

// Visitor for generating the code for a decomposed expression.
LLVMGenerator::Visitor::Visitor(LLVMGenerator* generator, llvm::Function* function,
                                llvm::BasicBlock* entry_block, llvm::Value* arg_addrs,
                                llvm::Value* arg_local_bitmaps,
                                llvm::Value* arg_context_ptr, llvm::Value* loop_var)
    : generator_(generator),
      function_(function),
      entry_block_(entry_block),
      arg_addrs_(arg_addrs),
      arg_local_bitmaps_(arg_local_bitmaps),
      arg_context_ptr_(arg_context_ptr),
      loop_var_(loop_var),
      has_arena_allocs_(false) {
  ADD_VISITOR_TRACE("Iteration %T", loop_var);
}

void LLVMGenerator::Visitor::Visit(const VectorReadFixedLenValueDex& dex) {
  llvm::IRBuilder<>* builder = ir_builder();

  llvm::Value* slot_ref = GetBufferReference(dex.DataIdx(), kBufferTypeData, dex.Field());

  llvm::Value* slot_value;
  if (dex.FieldType()->id() == arrow::Type::BOOL) {
    slot_value = generator_->GetPackedBitValue(slot_ref, loop_var_);
  } else {
    llvm::Value* slot_offset = builder->CreateGEP(slot_ref, loop_var_);
    slot_value = builder->CreateLoad(slot_offset, dex.FieldName());
  }

  ADD_VISITOR_TRACE("visit fixed-len data vector " + dex.FieldName() + " value %T",
                    slot_value);
  result_.reset(new LValue(slot_value));
}

void LLVMGenerator::Visitor::Visit(const VectorReadVarLenValueDex& dex) {
  llvm::IRBuilder<>* builder = ir_builder();
  llvm::Value* slot;

  // compute len from the offsets array.
  llvm::Value* offsets_slot_ref =
      GetBufferReference(dex.OffsetsIdx(), kBufferTypeOffsets, dex.Field());

  // => offset_start = offsets[loop_var]
  slot = builder->CreateGEP(offsets_slot_ref, loop_var_);
  llvm::Value* offset_start = builder->CreateLoad(slot, "offset_start");

  // => offset_end = offsets[loop_var + 1]
  llvm::Value* loop_var_next =
      builder->CreateAdd(loop_var_, generator_->types()->i64_constant(1), "loop_var+1");
  slot = builder->CreateGEP(offsets_slot_ref, loop_var_next);
  llvm::Value* offset_end = builder->CreateLoad(slot, "offset_end");

  // => len_value = offset_end - offset_start
  llvm::Value* len_value =
      builder->CreateSub(offset_end, offset_start, dex.FieldName() + "Len");

  // get the data from the data array, at offset 'offset_start'.
  llvm::Value* data_slot_ref =
      GetBufferReference(dex.DataIdx(), kBufferTypeData, dex.Field());
  llvm::Value* data_value = builder->CreateGEP(data_slot_ref, offset_start);
  ADD_VISITOR_TRACE("visit var-len data vector " + dex.FieldName() + " len %T",
                    len_value);
  result_.reset(new LValue(data_value, len_value));
}

void LLVMGenerator::Visitor::Visit(const VectorReadValidityDex& dex) {
  llvm::Value* slot_ref =
      GetBufferReference(dex.ValidityIdx(), kBufferTypeValidity, dex.Field());
  llvm::Value* validity = generator_->GetPackedBitValue(slot_ref, loop_var_);

  ADD_VISITOR_TRACE("visit validity vector " + dex.FieldName() + " value %T", validity);
  result_.reset(new LValue(validity));
}

void LLVMGenerator::Visitor::Visit(const LocalBitMapValidityDex& dex) {
  llvm::Value* slot_ref = GetLocalBitMapReference(dex.local_bitmap_idx());
  llvm::Value* validity = generator_->GetPackedBitValue(slot_ref, loop_var_);

  ADD_VISITOR_TRACE(
      "visit local bitmap " + std::to_string(dex.local_bitmap_idx()) + " value %T",
      validity);
  result_.reset(new LValue(validity));
}

void LLVMGenerator::Visitor::Visit(const TrueDex& dex) {
  result_.reset(new LValue(generator_->types()->true_constant()));
}

void LLVMGenerator::Visitor::Visit(const FalseDex& dex) {
  result_.reset(new LValue(generator_->types()->false_constant()));
}

void LLVMGenerator::Visitor::Visit(const LiteralDex& dex) {
  LLVMTypes* types = generator_->types();
  llvm::Value* value = nullptr;
  llvm::Value* len = nullptr;

  switch (dex.type()->id()) {
    case arrow::Type::BOOL:
      value = types->i1_constant(boost::get<bool>(dex.holder()));
      break;

    case arrow::Type::UINT8:
      value = types->i8_constant(boost::get<uint8_t>(dex.holder()));
      break;

    case arrow::Type::UINT16:
      value = types->i16_constant(boost::get<uint16_t>(dex.holder()));
      break;

    case arrow::Type::UINT32:
      value = types->i32_constant(boost::get<uint32_t>(dex.holder()));
      break;

    case arrow::Type::UINT64:
      value = types->i64_constant(boost::get<uint64_t>(dex.holder()));
      break;

    case arrow::Type::INT8:
      value = types->i8_constant(boost::get<int8_t>(dex.holder()));
      break;

    case arrow::Type::INT16:
      value = types->i16_constant(boost::get<int16_t>(dex.holder()));
      break;

    case arrow::Type::INT32:
      value = types->i32_constant(boost::get<int32_t>(dex.holder()));
      break;

    case arrow::Type::INT64:
      value = types->i64_constant(boost::get<int64_t>(dex.holder()));
      break;

    case arrow::Type::FLOAT:
      value = types->float_constant(boost::get<float>(dex.holder()));
      break;

    case arrow::Type::DOUBLE:
      value = types->double_constant(boost::get<double>(dex.holder()));
      break;

    case arrow::Type::STRING:
    case arrow::Type::BINARY: {
      const std::string& str = boost::get<std::string>(dex.holder());

      llvm::Constant* str_int_cast = types->i64_constant((int64_t)str.c_str());
      value = llvm::ConstantExpr::getIntToPtr(str_int_cast, types->i8_ptr_type());
      len = types->i32_constant(static_cast<int32_t>(str.length()));
      break;
    }

    case arrow::Type::DATE64:
      value = types->i64_constant(boost::get<int64_t>(dex.holder()));
      break;

    case arrow::Type::TIME32:
      value = types->i32_constant(boost::get<int32_t>(dex.holder()));
      break;

    case arrow::Type::TIME64:
      value = types->i64_constant(boost::get<int64_t>(dex.holder()));
      break;

    case arrow::Type::TIMESTAMP:
      value = types->i64_constant(boost::get<int64_t>(dex.holder()));
      break;

    default:
      DCHECK(0);
  }
  ADD_VISITOR_TRACE("visit Literal %T", value);
  result_.reset(new LValue(value, len));
}

void LLVMGenerator::Visitor::Visit(const NonNullableFuncDex& dex) {
  const std::string& function_name = dex.func_descriptor()->name();
  ADD_VISITOR_TRACE("visit NonNullableFunc base function " + function_name);

  const NativeFunction* native_function = dex.native_function();

  // build the function params (ignore validity).
  auto params = BuildParams(dex.function_holder().get(), dex.args(), false,
                            native_function->NeedsContext());

  if (native_function->CanReturnErrors()) {
    // slow path : if a function can return errors, skip invoking the function
    // unless all of the input args are valid. Otherwise, it can cause spurious errors.

    llvm::IRBuilder<>* builder = ir_builder();
    LLVMTypes* types = generator_->types();
    auto arrow_type_id = native_function->signature().ret_type()->id();
    auto result_type = types->IRType(arrow_type_id);

    // Build combined validity of the args.
    llvm::Value* is_valid = types->true_constant();
    for (auto& pair : dex.args()) {
      auto arg_validity = BuildCombinedValidity(pair->validity_exprs());
      is_valid = builder->CreateAnd(is_valid, arg_validity, "validityBitAnd");
    }

    // then block
    auto then_lambda = [&] {
      ADD_VISITOR_TRACE("fn " + function_name +
                        " can return errors : all args valid, invoke fn");
      return BuildFunctionCall(native_function, &params);
    };

    // else block
    auto else_lambda = [&] {
      ADD_VISITOR_TRACE("fn " + function_name +
                        " can return errors : not all args valid, return dummy value");
      llvm::Value* else_value = types->NullConstant(result_type);
      llvm::Value* else_value_len = nullptr;
      if (arrow::is_binary_like(arrow_type_id)) {
        else_value_len = types->i32_constant(0);
      }
      return std::make_shared<LValue>(else_value, else_value_len);
    };

    result_ = BuildIfElse(is_valid, then_lambda, else_lambda, result_type);
  } else {
    // fast path : invoke function without computing validities.
    result_ = BuildFunctionCall(native_function, &params);
  }
}

void LLVMGenerator::Visitor::Visit(const NullableNeverFuncDex& dex) {
  ADD_VISITOR_TRACE("visit NullableNever base function " + dex.func_descriptor()->name());
  const NativeFunction* native_function = dex.native_function();

  // build function params along with validity.
  auto params = BuildParams(dex.function_holder().get(), dex.args(), true,
                            native_function->NeedsContext());

  result_ = BuildFunctionCall(native_function, &params);
}

void LLVMGenerator::Visitor::Visit(const NullableInternalFuncDex& dex) {
  ADD_VISITOR_TRACE("visit NullableInternal base function " +
                    dex.func_descriptor()->name());
  llvm::IRBuilder<>* builder = ir_builder();
  LLVMTypes* types = generator_->types();

  const NativeFunction* native_function = dex.native_function();

  // build function params along with validity.
  auto params = BuildParams(dex.function_holder().get(), dex.args(), true,
                            native_function->NeedsContext());

  // add an extra arg for validity (alloced on stack).
  llvm::AllocaInst* result_valid_ptr =
      new llvm::AllocaInst(types->i8_type(), 0, "result_valid", entry_block_);
  params.push_back(result_valid_ptr);

  result_ = BuildFunctionCall(native_function, &params);

  // load the result validity and truncate to i1.
  llvm::Value* result_valid_i8 = builder->CreateLoad(result_valid_ptr);
  llvm::Value* result_valid = builder->CreateTrunc(result_valid_i8, types->i1_type());

  // set validity bit in the local bitmap.
  ClearLocalBitMapIfNotValid(dex.local_bitmap_idx(), result_valid);
}

void LLVMGenerator::Visitor::Visit(const IfDex& dex) {
  ADD_VISITOR_TRACE("visit IfExpression");
  llvm::IRBuilder<>* builder = ir_builder();
  LLVMTypes* types = generator_->types();

  // Evaluate condition.
  LValuePtr if_condition = BuildValueAndValidity(dex.condition_vv());

  // Check if the result is valid, and there is match.
  llvm::Value* validAndMatched =
      builder->CreateAnd(if_condition->data(), if_condition->validity(), "validAndMatch");

  // then block
  auto then_lambda = [&] {
    ADD_VISITOR_TRACE("branch to then block");
    LValuePtr then_lvalue = BuildValueAndValidity(dex.then_vv());
    ClearLocalBitMapIfNotValid(dex.local_bitmap_idx(), then_lvalue->validity());
    ADD_VISITOR_TRACE("IfExpression result validity %T in matching then",
                      then_lvalue->validity());
    return then_lvalue;
  };

  // else block
  auto else_lambda = [&] {
    LValuePtr else_lvalue;
    if (dex.is_terminal_else()) {
      ADD_VISITOR_TRACE("branch to terminal else block");

      else_lvalue = BuildValueAndValidity(dex.else_vv());
      // update the local bitmap with the validity.
      ClearLocalBitMapIfNotValid(dex.local_bitmap_idx(), else_lvalue->validity());
      ADD_VISITOR_TRACE("IfExpression result validity %T in terminal else",
                        else_lvalue->validity());
    } else {
      ADD_VISITOR_TRACE("branch to non-terminal else block");

      // this is a non-terminal else. let the child (nested if/else) handle validity.
      auto value_expr = dex.else_vv().value_expr();
      value_expr->Accept(*this);
      else_lvalue = result();
    }
    return else_lvalue;
  };

  // build the if-else condition.
  auto result_type = types->IRType(dex.result_type()->id());
  result_ = BuildIfElse(validAndMatched, then_lambda, else_lambda, result_type);
  if (result_type == types->i8_ptr_type()) {
    ADD_VISITOR_TRACE("IfElse result length %T", result_->length());
  }
  ADD_VISITOR_TRACE("IfElse result value %T", result_->data());
}

// Boolean AND
// if any arg is valid and false,
//   short-circuit and return FALSE (value=false, valid=true)
// else if all args are valid and true
//   return TRUE (value=true, valid=true)
// else
//   return NULL (value=true, valid=false)

void LLVMGenerator::Visitor::Visit(const BooleanAndDex& dex) {
  ADD_VISITOR_TRACE("visit BooleanAndExpression");
  llvm::IRBuilder<>* builder = ir_builder();
  LLVMTypes* types = generator_->types();
  llvm::LLVMContext* context = generator_->context();

  // Create blocks for short-circuit.
  llvm::BasicBlock* short_circuit_bb =
      llvm::BasicBlock::Create(*context, "short_circuit", function_);
  llvm::BasicBlock* non_short_circuit_bb =
      llvm::BasicBlock::Create(*context, "non_short_circuit", function_);
  llvm::BasicBlock* merge_bb = llvm::BasicBlock::Create(*context, "merge", function_);

  llvm::Value* all_exprs_valid = types->true_constant();
  for (auto& pair : dex.args()) {
    LValuePtr current = BuildValueAndValidity(*pair);

    ADD_VISITOR_TRACE("BooleanAndExpression arg value %T", current->data());
    ADD_VISITOR_TRACE("BooleanAndExpression arg valdity %T", current->validity());

    // short-circuit if valid and false
    llvm::Value* is_false = builder->CreateNot(current->data());
    llvm::Value* valid_and_false =
        builder->CreateAnd(is_false, current->validity(), "valid_and_false");

    llvm::BasicBlock* else_bb = llvm::BasicBlock::Create(*context, "else", function_);
    builder->CreateCondBr(valid_and_false, short_circuit_bb, else_bb);

    // Emit the else block.
    builder->SetInsertPoint(else_bb);
    // remember if any nulls were encountered.
    all_exprs_valid =
        builder->CreateAnd(all_exprs_valid, current->validity(), "validityBitAnd");
    // continue to evaluate the next pair in list.
  }
  builder->CreateBr(non_short_circuit_bb);

  // Short-circuit case (atleast one of the expressions is valid and false).
  // No need to set validity bit (valid by default).
  builder->SetInsertPoint(short_circuit_bb);
  ADD_VISITOR_TRACE("BooleanAndExpression result value false");
  ADD_VISITOR_TRACE("BooleanAndExpression result valdity true");
  builder->CreateBr(merge_bb);

  // non short-circuit case (All expressions are either true or null).
  // result valid if all of the exprs are non-null.
  builder->SetInsertPoint(non_short_circuit_bb);
  ClearLocalBitMapIfNotValid(dex.local_bitmap_idx(), all_exprs_valid);
  ADD_VISITOR_TRACE("BooleanAndExpression result value true");
  ADD_VISITOR_TRACE("BooleanAndExpression result valdity %T", all_exprs_valid);
  builder->CreateBr(merge_bb);

  builder->SetInsertPoint(merge_bb);
  llvm::PHINode* result_value = builder->CreatePHI(types->i1_type(), 2, "res_value");
  result_value->addIncoming(types->false_constant(), short_circuit_bb);
  result_value->addIncoming(types->true_constant(), non_short_circuit_bb);
  result_.reset(new LValue(result_value));
}

// Boolean OR
// if any arg is valid and true,
//   short-circuit and return TRUE (value=true, valid=true)
// else if all args are valid and false
//   return FALSE (value=false, valid=true)
// else
//   return NULL (value=false, valid=false)

void LLVMGenerator::Visitor::Visit(const BooleanOrDex& dex) {
  ADD_VISITOR_TRACE("visit BooleanOrExpression");
  llvm::IRBuilder<>* builder = ir_builder();
  LLVMTypes* types = generator_->types();
  llvm::LLVMContext* context = generator_->context();

  // Create blocks for short-circuit.
  llvm::BasicBlock* short_circuit_bb =
      llvm::BasicBlock::Create(*context, "short_circuit", function_);
  llvm::BasicBlock* non_short_circuit_bb =
      llvm::BasicBlock::Create(*context, "non_short_circuit", function_);
  llvm::BasicBlock* merge_bb = llvm::BasicBlock::Create(*context, "merge", function_);

  llvm::Value* all_exprs_valid = types->true_constant();
  for (auto& pair : dex.args()) {
    LValuePtr current = BuildValueAndValidity(*pair);

    ADD_VISITOR_TRACE("BooleanOrExpression arg value %T", current->data());
    ADD_VISITOR_TRACE("BooleanOrExpression arg valdity %T", current->validity());

    // short-circuit if valid and true.
    llvm::Value* valid_and_true =
        builder->CreateAnd(current->data(), current->validity(), "valid_and_true");

    llvm::BasicBlock* else_bb = llvm::BasicBlock::Create(*context, "else", function_);
    builder->CreateCondBr(valid_and_true, short_circuit_bb, else_bb);

    // Emit the else block.
    builder->SetInsertPoint(else_bb);
    // remember if any nulls were encountered.
    all_exprs_valid =
        builder->CreateAnd(all_exprs_valid, current->validity(), "validityBitAnd");
    // continue to evaluate the next pair in list.
  }
  builder->CreateBr(non_short_circuit_bb);

  // Short-circuit case (atleast one of the expressions is valid and true).
  // No need to set validity bit (valid by default).
  builder->SetInsertPoint(short_circuit_bb);
  ADD_VISITOR_TRACE("BooleanOrExpression result value true");
  ADD_VISITOR_TRACE("BooleanOrExpression result valdity true");
  builder->CreateBr(merge_bb);

  // non short-circuit case (All expressions are either false or null).
  // result valid if all of the exprs are non-null.
  builder->SetInsertPoint(non_short_circuit_bb);
  ClearLocalBitMapIfNotValid(dex.local_bitmap_idx(), all_exprs_valid);
  ADD_VISITOR_TRACE("BooleanOrExpression result value false");
  ADD_VISITOR_TRACE("BooleanOrExpression result valdity %T", all_exprs_valid);
  builder->CreateBr(merge_bb);

  builder->SetInsertPoint(merge_bb);
  llvm::PHINode* result_value = builder->CreatePHI(types->i1_type(), 2, "res_value");
  result_value->addIncoming(types->true_constant(), short_circuit_bb);
  result_value->addIncoming(types->false_constant(), non_short_circuit_bb);
  result_.reset(new LValue(result_value));
}

void LLVMGenerator::Visitor::Visit(const InExprDexBase<int32_t>& dex) {
  VisitInExpression<int32_t>(dex);
}

void LLVMGenerator::Visitor::Visit(const InExprDexBase<int64_t>& dex) {
  VisitInExpression<int64_t>(dex);
}

void LLVMGenerator::Visitor::Visit(const InExprDexBase<std::string>& dex) {
  VisitInExpression<std::string>(dex);
}

template <typename Type>
void LLVMGenerator::Visitor::VisitInExpression(const InExprDexBase<Type>& dex) {
  ADD_VISITOR_TRACE("visit In Expression");
  LLVMTypes* types = generator_->types();
  std::vector<llvm::Value*> params;

  const InExprDex<Type>& dex_instance = dynamic_cast<const InExprDex<Type>&>(dex);
  /* add the holder at the beginning */
  llvm::Constant* ptr_int_cast =
      types->i64_constant((int64_t)(dex_instance.in_holder().get()));
  params.push_back(ptr_int_cast);

  /* eval expr result */
  for (auto& pair : dex.args()) {
    DexPtr value_expr = pair->value_expr();
    value_expr->Accept(*this);
    LValue& result_ref = *result();
    params.push_back(result_ref.data());

    /* length if the result is a string */
    if (result_ref.length() != nullptr) {
      params.push_back(result_ref.length());
    }

    /* push the validity of eval expr result */
    llvm::Value* validity_expr = BuildCombinedValidity(pair->validity_exprs());
    params.push_back(validity_expr);
  }

  llvm::Type* ret_type = types->IRType(arrow::Type::type::BOOL);

  llvm::Value* value =
      generator_->AddFunctionCall(dex.runtime_function(), ret_type, params);
  result_.reset(new LValue(value));
}

LValuePtr LLVMGenerator::Visitor::BuildIfElse(llvm::Value* condition,
                                              std::function<LValuePtr()> then_func,
                                              std::function<LValuePtr()> else_func,
                                              llvm::Type* result_type) {
  llvm::IRBuilder<>* builder = ir_builder();
  llvm::LLVMContext* context = generator_->context();
  LLVMTypes* types = generator_->types();

  // Create blocks for the then, else and merge cases.
  llvm::BasicBlock* then_bb = llvm::BasicBlock::Create(*context, "then", function_);
  llvm::BasicBlock* else_bb = llvm::BasicBlock::Create(*context, "else", function_);
  llvm::BasicBlock* merge_bb = llvm::BasicBlock::Create(*context, "merge", function_);

  builder->CreateCondBr(condition, then_bb, else_bb);

  // Emit the then block.
  builder->SetInsertPoint(then_bb);
  LValuePtr then_lvalue = then_func();
  builder->CreateBr(merge_bb);

  // refresh then_bb for phi (could have changed due to code generation of then_vv).
  then_bb = builder->GetInsertBlock();

  // Emit the else block.
  builder->SetInsertPoint(else_bb);
  LValuePtr else_lvalue = else_func();
  builder->CreateBr(merge_bb);

  // refresh else_bb for phi (could have changed due to code generation of else_vv).
  else_bb = builder->GetInsertBlock();

  // Emit the merge block.
  builder->SetInsertPoint(merge_bb);
  llvm::PHINode* result_value = builder->CreatePHI(result_type, 2, "res_value");
  result_value->addIncoming(then_lvalue->data(), then_bb);
  result_value->addIncoming(else_lvalue->data(), else_bb);

  llvm::PHINode* result_length = nullptr;
  if (result_type == types->i8_ptr_type()) {
    result_length = builder->CreatePHI(types->i32_type(), 2, "res_length");
    result_length->addIncoming(then_lvalue->length(), then_bb);
    result_length->addIncoming(else_lvalue->length(), else_bb);
  }
  return std::make_shared<LValue>(result_value, result_length);
}

LValuePtr LLVMGenerator::Visitor::BuildValueAndValidity(const ValueValidityPair& pair) {
  // generate code for value
  auto value_expr = pair.value_expr();
  value_expr->Accept(*this);
  auto value = result()->data();
  auto length = result()->length();

  // generate code for validity
  auto validity = BuildCombinedValidity(pair.validity_exprs());

  return std::make_shared<LValue>(value, length, validity);
}

LValuePtr LLVMGenerator::Visitor::BuildFunctionCall(const NativeFunction* func,
                                                    std::vector<llvm::Value*>* params) {
  auto arrow_return_type = func->signature().ret_type()->id();
  auto llvm_return_type = generator_->types()->IRType(arrow_return_type);

  // add extra arg for return length for variable len return types (alloced on stack).
  llvm::AllocaInst* result_len_ptr = nullptr;
  if (arrow::is_binary_like(arrow_return_type)) {
    result_len_ptr = new llvm::AllocaInst(generator_->types()->i32_type(), 0,
                                          "result_len", entry_block_);
    params->push_back(result_len_ptr);
    has_arena_allocs_ = true;
  }

  // Make the function call
  llvm::IRBuilder<>* builder = ir_builder();
  auto value = generator_->AddFunctionCall(func->pc_name(), llvm_return_type, *params);
  auto value_len =
      (result_len_ptr == nullptr) ? nullptr : builder->CreateLoad(result_len_ptr);
  return std::make_shared<LValue>(value, value_len);
}

std::vector<llvm::Value*> LLVMGenerator::Visitor::BuildParams(
    FunctionHolder* holder, const ValueValidityPairVector& args, bool with_validity,
    bool with_context) {
  LLVMTypes* types = generator_->types();
  std::vector<llvm::Value*> params;

  // add context if required.
  if (with_context) {
    params.push_back(arg_context_ptr_);
  }

  // if the function has holder, add the holder pointer.
  if (holder != nullptr) {
    auto ptr = types->i64_constant((int64_t)holder);
    params.push_back(ptr);
  }

  // build the function params, along with the validities.
  for (auto& pair : args) {
    // build value.
    DexPtr value_expr = pair->value_expr();
    value_expr->Accept(*this);
    LValue& result_ref = *result();
    params.push_back(result_ref.data());

    // build length (for var len data types)
    if (result_ref.length() != nullptr) {
      params.push_back(result_ref.length());
    }

    // build validity.
    if (with_validity) {
      llvm::Value* validity_expr = BuildCombinedValidity(pair->validity_exprs());
      params.push_back(validity_expr);
    }
  }

  return params;
}

// Bitwise-AND of a vector of bits to get the combined validity.
llvm::Value* LLVMGenerator::Visitor::BuildCombinedValidity(const DexVector& validities) {
  llvm::IRBuilder<>* builder = ir_builder();
  LLVMTypes* types = generator_->types();

  llvm::Value* isValid = types->true_constant();
  for (auto& dex : validities) {
    dex->Accept(*this);
    isValid = builder->CreateAnd(isValid, result()->data(), "validityBitAnd");
  }
  ADD_VISITOR_TRACE("combined validity is %T", isValid);
  return isValid;
}

llvm::Value* LLVMGenerator::Visitor::GetBufferReference(int idx, BufferType buffer_type,
                                                        FieldPtr field) {
  llvm::IRBuilder<>* builder = ir_builder();

  // Switch to the entry block to create a reference.
  llvm::BasicBlock* saved_block = builder->GetInsertBlock();
  builder->SetInsertPoint(entry_block_);

  llvm::Value* slot_ref = nullptr;
  switch (buffer_type) {
    case kBufferTypeValidity:
      slot_ref = generator_->GetValidityReference(arg_addrs_, idx, field);
      break;

    case kBufferTypeData:
      slot_ref = generator_->GetDataReference(arg_addrs_, idx, field);
      break;

    case kBufferTypeOffsets:
      slot_ref = generator_->GetOffsetsReference(arg_addrs_, idx, field);
      break;
  }

  // Revert to the saved block.
  builder->SetInsertPoint(saved_block);
  return slot_ref;
}

llvm::Value* LLVMGenerator::Visitor::GetLocalBitMapReference(int idx) {
  llvm::IRBuilder<>* builder = ir_builder();

  // Switch to the entry block to create a reference.
  llvm::BasicBlock* saved_block = builder->GetInsertBlock();
  builder->SetInsertPoint(entry_block_);

  llvm::Value* slot_ref = generator_->GetLocalBitMapReference(arg_local_bitmaps_, idx);

  // Revert to the saved block.
  builder->SetInsertPoint(saved_block);
  return slot_ref;
}

/// The local bitmap is pre-filled with 1s. Clear only if invalid.
void LLVMGenerator::Visitor::ClearLocalBitMapIfNotValid(int local_bitmap_idx,
                                                        llvm::Value* is_valid) {
  llvm::Value* slot_ref = GetLocalBitMapReference(local_bitmap_idx);
  generator_->ClearPackedBitValueIfFalse(slot_ref, loop_var_, is_valid);
}

// Hooks for tracing/printfs.
//
// replace %T with the type-specific format specifier.
// For some reason, float/double literals are getting lost when printing with the generic
// printf. so, use a wrapper instead.
std::string LLVMGenerator::ReplaceFormatInTrace(const std::string& in_msg,
                                                llvm::Value* value,
                                                std::string* print_fn) {
  std::string msg = in_msg;
  std::size_t pos = msg.find("%T");
  if (pos == std::string::npos) {
    DCHECK(0);
    return msg;
  }

  llvm::Type* type = value->getType();
  const char* fmt = "";
  if (type->isIntegerTy(1) || type->isIntegerTy(8) || type->isIntegerTy(16) ||
      type->isIntegerTy(32)) {
    fmt = "%d";
  } else if (type->isIntegerTy(64)) {
    // bigint
    fmt = "%lld";
  } else if (type->isFloatTy()) {
    // float
    fmt = "%f";
    *print_fn = "print_float";
  } else if (type->isDoubleTy()) {
    // float
    fmt = "%lf";
    *print_fn = "print_double";
  } else if (type->isPointerTy()) {
    // string
    fmt = "%s";
  } else {
    DCHECK(0);
  }
  msg.replace(pos, 2, fmt);
  return msg;
}

void LLVMGenerator::AddTrace(const std::string& msg, llvm::Value* value) {
  if (!enable_ir_traces_) {
    return;
  }

  std::string dmsg = "IR_TRACE:: " + msg + "\n";
  std::string print_fn_name = "printf";
  if (value != nullptr) {
    dmsg = ReplaceFormatInTrace(dmsg, value, &print_fn_name);
  }
  trace_strings_.push_back(dmsg);

  // cast this to an llvm pointer.
  const char* str = trace_strings_.back().c_str();
  llvm::Constant* str_int_cast = types()->i64_constant((int64_t)str);
  llvm::Constant* str_ptr_cast =
      llvm::ConstantExpr::getIntToPtr(str_int_cast, types()->i8_ptr_type());

  std::vector<llvm::Value*> args;
  args.push_back(str_ptr_cast);
  if (value != nullptr) {
    args.push_back(value);
  }
  AddFunctionCall(print_fn_name, types()->i32_type(), args);
}

}  // namespace gandiva
