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
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include "gandiva/expression.h"
#include "codegen/dex.h"
#include "codegen/function_registry.h"
#include "codegen/llvm_generator.h"
#include "codegen/lvalue.h"

namespace gandiva {

LLVMGenerator::LLVMGenerator() :
  in_replay_(false),
  optimise_ir_(true),
  enable_ir_traces_(false) {}

Status LLVMGenerator::Make(std::unique_ptr<LLVMGenerator> *llvm_generator) {
  std::unique_ptr<LLVMGenerator> llvmgen_obj(new LLVMGenerator());
  Status status = Engine::Make(&(llvmgen_obj->engine_));
  GANDIVA_RETURN_NOT_OK(status);
  llvmgen_obj->types_ = new LLVMTypes(*(llvmgen_obj->engine_)->context());
  *llvm_generator = std::move(llvmgen_obj);
  return Status::OK();
}

LLVMGenerator::~LLVMGenerator() {
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); ++it) {
    delete *it;
  }
  delete types_;
}

Status LLVMGenerator::Add(const ExpressionPtr expr,
                          const FieldDescriptorPtr output) {
  int idx = compiled_exprs_.size();

  // decompose the expression to separate out value and validities.
  ValueValidityPairPtr value_validity = expr->Decompose(function_registry_,
                                                        annotator_);

  // Generate the IR function for the decomposed expression.
  llvm::Function *ir_function = nullptr;

  Status status = CodeGenExprValue(value_validity->value_expr(),
                                   output,
                                   idx,
                                   &ir_function);
  GANDIVA_RETURN_NOT_OK(status);

  CompiledExpr *compiled_expr = new CompiledExpr(value_validity, output, ir_function);
  compiled_exprs_.push_back(compiled_expr);
  return Status::OK();
}

/*
 * Build and optimise module for projection expression.
 */
Status LLVMGenerator::Build(const ExpressionVector &exprs) {
  for (auto it = exprs.begin(); it != exprs.end(); it++) {
    ExpressionPtr expr = *it;

    auto output = annotator_.AddOutputFieldDescriptor(expr->result());
    Add(expr, output);
  }

  // optimise, compile and finalize the module
  Status result = engine_->FinalizeModule(optimise_ir_, in_replay_);
  GANDIVA_RETURN_NOT_OK(result);

  // setup the jit functions for each expression.
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); it++) {
    CompiledExpr *compiled_expr = *it;
    llvm::Function *ir_func = compiled_expr->ir_function();
    EvalFunc fn = reinterpret_cast<EvalFunc>(engine_->CompiledFunction(ir_func));
    compiled_expr->set_jit_function(fn);
  }
  return Status::OK();
}

/*
 * Execute the compiled module against the provided vectors.
 */
Status LLVMGenerator::Execute(const arrow::RecordBatch &record_batch,
                           const arrow::ArrayVector &outputs) {
  DCHECK_GT(record_batch.num_rows(), 0);

  auto eval_batch = annotator_.PrepareEvalBatch(record_batch, outputs);
  DCHECK_GT(eval_batch->num_buffers(), 0);

  // generate bitmap vectors, by doing an intersection.
  for (auto it = compiled_exprs_.begin(); it != compiled_exprs_.end(); it++) {
    CompiledExpr *compiled_expr = *it;

    // generate data/offset vectors.
    EvalFunc jit_function = compiled_expr->jit_function();
    jit_function(eval_batch->buffers(), record_batch.num_rows());

    // generate validity vectors.
    ComputeBitMapsForExpr(compiled_expr, eval_batch->buffers(), eval_batch->num_buffers(),
                          record_batch.num_rows());
  }
  return Status::OK();
}

llvm::Value *LLVMGenerator::LoadVectorAtIndex(llvm::Value *arg_addrs,
                                              int idx,
                                              const std::string &name) {
  llvm::IRBuilder<> &builder = ir_builder();
  llvm::Value *offset = builder.CreateGEP(arg_addrs,
                                          types_->i32_constant(idx),
                                          name + "_mem_addr");
  return builder.CreateLoad(offset, name + "_mem");
}

/*
 * Get reference to validity array at specified index in the args list.
 */
llvm::Value *LLVMGenerator::GetValidityReference(llvm::Value *arg_addrs,
                                                 int idx,
                                                 FieldPtr field) {
  const std::string &name = field->name();
  llvm::Value *load = LoadVectorAtIndex(arg_addrs, idx, name);
  return ir_builder().CreateIntToPtr(load, types_->i64_ptr_type(), name + "_varray");
}

/*
 * Get reference to data array at specified index in the args list.
 */
llvm::Value *LLVMGenerator::GetDataReference(llvm::Value *arg_addrs,
                                             int idx,
                                             FieldPtr field) {
  const std::string &name = field->name();
  llvm::Value *load = LoadVectorAtIndex(arg_addrs, idx, name);
  llvm::Type *base_type = types_->DataVecType(field->type());
  llvm::Type *pointer_type = types_->ptr_type(base_type);
  return ir_builder().CreateIntToPtr(load, pointer_type, name + "_darray");
}

/*
 * Generate code for one expression.
 *
 * Sample IR code for "c1:int + c2:int"
 *
 * The C-code equivalent is :
 * ------------------------------
 * int expr_0(long *addrs, int nrecords) {
 *   int *outVec = (int *) addrs[5];
 *   int *c0Vec = (int *) addrs[1];
 *   int *c1Vec = (int *) addrs[3];
 *   for (int loop_var = 0; loop_var < nrecords; ++loop_var) {
 *     int c0 = c0Vec[loop_var];
 *     int c1 = c1Vec[loop_var];
 *     int out = c0 + c1;
 *     outVec[loop_var] = out;
 *   }
 * }
 *
 * IR Code
 * --------
 *
 * define i32 @expr_0(i64* %args, i32 %nrecords) {
 * entry:
 *   %outmemAddr = getelementptr i64, i64* %args, i32 5
 *   %outmem = load i64, i64* %outmemAddr
 *   %outVec = inttoptr i64 %outmem to i32*
 *   %c0memAddr = getelementptr i64, i64* %args, i32 1
 *   %c0mem = load i64, i64* %c0memAddr
 *   %c0Vec = inttoptr i64 %c0mem to i32*
 *   %c1memAddr = getelementptr i64, i64* %args, i32 3
 *   %c1mem = load i64, i64* %c1memAddr
 *   %c1Vec = inttoptr i64 %c1mem to i32*
 *   br label %loop
 * loop:                                             ; preds = %loop, %entry
 *   %loop_var = phi i32 [ 0, %entry ], [ %"loop_var+1", %loop ]
 *   %"loop_var+1" = add i32 %loop_var, 1
 *   %0 = getelementptr i32, i32* %c0Vec, i32 %loop_var
 *   %c0 = load i32, i32* %0
 *   %1 = getelementptr i32, i32* %c1Vec, i32 %loop_var
 *   %c1 = load i32, i32* %1
 *   %add_int_int = call i32 @add_int_int(i32 %c0, i32 %c1)
 *   %2 = getelementptr i32, i32* %outVec, i32 %loop_var
 *   store i32 %add_int_int, i32* %2
 *   %"loop_var < nrec" = icmp slt i32 %"loop_var+1", %nrecords
 *   br i1 %"loop_var < nrec", label %loop, label %exit
 * exit:                                             ; preds = %loop
 *   ret i32 0
 * }
 *
 */
Status LLVMGenerator::CodeGenExprValue(DexPtr value_expr,
                                       FieldDescriptorPtr output,
                                       int suffix_idx,
                                       llvm::Function **fn) {
  llvm::IRBuilder<> &builder = ir_builder();

  // Create fn prototype :
  //   int expr_1 (long **addrs, int nrec)
  std::vector<llvm::Type *> arguments;
  arguments.push_back(types_->i64_ptr_type());
  arguments.push_back(types_->i32_type());
  llvm::FunctionType *prototype = llvm::FunctionType::get(types_->i32_type(),
                                                          arguments,
                                                          false /*isVarArg*/);

  // Create fn
  std::string func_name = "expr_" + std::to_string(suffix_idx);
  engine_->AddFunctionToCompile(func_name);
  *fn = llvm::Function::Create(prototype,
                               llvm::GlobalValue::ExternalLinkage,
                               func_name,
                               module());
  GANDIVA_RETURN_FAILURE_IF_FALSE((fn != NULL),
                                  Status::CodeGenError("Error creating function."));
  // Name the arguments
  llvm::Function::arg_iterator args = (*fn)->arg_begin();
  llvm::Value *arg_addrs = &*args;
  arg_addrs->setName("args");
  ++args;
  llvm::Value *arg_nrecords = &*args;
  arg_nrecords->setName("nrecords");
  ++args;

  llvm::BasicBlock *loop_entry = llvm::BasicBlock::Create(context(), "entry", *fn);
  llvm::BasicBlock *loop_body = llvm::BasicBlock::Create(context(), "loop", *fn);
  llvm::BasicBlock *loop_exit = llvm::BasicBlock::Create(context(), "exit", *fn);

  // Add reference to output vector (in entry block)
  builder.SetInsertPoint(loop_entry);
  llvm::Value *output_ref = GetDataReference(arg_addrs,
                                             output->data_idx(),
                                             output->field());

  // Loop body
  builder.SetInsertPoint(loop_body);

  // define loop_var : start with 0, +1 after each iter
  llvm::PHINode *loop_var = builder.CreatePHI(types_->i32_type(), 2, "loop_var");
  loop_var->addIncoming(types_->i32_constant(0), loop_entry);
  llvm::Value *loop_update = builder.CreateAdd(loop_var,
                                               types_->i32_constant(1),
                                               "loop_var+1");
  loop_var->addIncoming(loop_update, loop_body);

  // The visitor can add code to both the entry/loop blocks.
  Visitor visitor(this, *fn, loop_entry, loop_body, arg_addrs, loop_var);
  value_expr->Accept(&visitor);
  LValuePtr output_value = visitor.result();

  // add jump to "loop block" at the end of the "setup block".
  builder.SetInsertPoint(loop_entry);
  builder.CreateBr(loop_body);

  // save the value in the output vector
  builder.SetInsertPoint(loop_body);

  if (output->Type()->id() == arrow::Type::BOOL) {
    SetPackedBitValue(output_ref, loop_var, output_value->data());
  } else {
    llvm::Value *slot_offset = builder.CreateGEP(output_ref, loop_var);
    builder.CreateStore(output_value->data(), slot_offset);
  }
  AddTrace("saving result " + output->Name() + " value %T", output_value->data());

  // check loop_var
  llvm::Value *loop_var_check = builder.CreateICmpSLT(loop_update,
                                                      arg_nrecords,
                                                      "loop_var < nrec");
  builder.CreateCondBr(loop_var_check, loop_body, loop_exit);

  // Loop exit
  builder.SetInsertPoint(loop_exit);
  builder.CreateRet(types_->i32_constant(0));
  return Status::OK();
}

/*
 * Return value of a bit in bitMap.
 */
llvm::Value *LLVMGenerator::GetPackedBitValue(llvm::Value *bitmap,
                                              llvm::Value *position) {
  AddTrace("fetch bit at position %T", position);

  llvm::Value *bitmap8 = ir_builder().CreateBitCast(bitmap,
                                                    types_->ptr_type(types_->i8_type()),
                                                    "bitMapCast");
  return AddFunctionCall("bitMapGetBit", types_->i1_type(), {bitmap8, position});
}

/*
 * Set the value of a bit in bitMap.
 */
void LLVMGenerator::SetPackedBitValue(llvm::Value *bitmap,
                                      llvm::Value *position,
                                      llvm::Value *value) {
  AddTrace("set bit at position %T", position);
  AddTrace("  to value %T ", value);

  llvm::Value *bitmap8 = ir_builder().CreateBitCast(bitmap,
                                                    types_->ptr_type(types_->i8_type()),
                                                    "bitMapCast");
  AddFunctionCall("bitMapSetBit", types_->void_type(), {bitmap8, position, value});
}

/*
 * Extract the bitmap addresses, and do an intersection.
 */
void LLVMGenerator::ComputeBitMapsForExpr(CompiledExpr *compiled_expr,
                                          uint8_t **buffers,
                                          int num_buffers,
                                          int record_count) {
  auto validities = compiled_expr->value_validity()->validity_exprs();

  std::vector<uint8_t *> src_bitmaps;
  for (auto it = validities.begin(); it != validities.end(); it++) {
    Dex *validity_dex = (*it).get();
    VectorReadValidityDex *value_dex =
        dynamic_cast<VectorReadValidityDex *>(validity_dex);

    DCHECK_LT(value_dex->ValidityIdx(), num_buffers);
    src_bitmaps.push_back(buffers[value_dex->ValidityIdx()]);
  }

  int out_idx = compiled_expr->output()->validity_idx();
  DCHECK_LT(out_idx, num_buffers);
  uint8_t *dst_bitmap = buffers[out_idx];

  IntersectBitMaps(dst_bitmap, src_bitmaps, record_count);
}

/*
 * Compute the intersection of multiple bitmaps.
 */
void
LLVMGenerator::IntersectBitMaps(uint8_t *dst_map,
                                const std::vector<uint8_t *> &src_maps,
                                int num_records) {
  uint64_t *dst_map64 = reinterpret_cast<uint64_t *>(dst_map);
  int num_words = (num_records + 63) / 64; // aligned to 8-byte.
  int num_bytes = num_words * 8;
  int nmaps = src_maps.size();

  switch (nmaps) {
    case 0: {
      /* no src_maps bitmap. simply set all bits */
      memset(dst_map, 0xff, num_bytes);
      break;
    }

    case 1: {
      /* one src_maps bitmap. copy to dst_map */
      memcpy(dst_map, src_maps[0], num_bytes);
      break;
    }

    case 2: {
      /* two src_maps bitmaps. do 64-bit ANDs */
      uint64_t *src_maps0_64 = reinterpret_cast<uint64_t *>(src_maps[0]);
      uint64_t *src_maps1_64 = reinterpret_cast<uint64_t *>(src_maps[1]);
      for (int i = 0; i < num_words; ++i) {
        dst_map64[i] = src_maps0_64[i] & src_maps1_64[i];
      }
      break;
    }

    default: {
      /* > 2 src_maps bitmaps. do 64-bit ANDs */
      uint64_t *src_maps0_64 = reinterpret_cast<uint64_t *>(src_maps[0]);
      memcpy(dst_map64, src_maps0_64, num_bytes);
      for (int m = 1; m < nmaps; ++m) {
        for (int i = 0; i < num_words; ++i) {
          uint64_t *src_mapsm_64 = reinterpret_cast<uint64_t *>(src_maps[m]);
          dst_map64[i] &= src_mapsm_64[i];
        }
      }

      break;
    }
  }
}

llvm::Value *LLVMGenerator::AddFunctionCall(const std::string &full_name,
                                            llvm::Type *ret_type,
                                            const std::vector<llvm::Value *> &args) {
  // add to list of functions that need to be compiled
  engine_->AddFunctionToCompile(full_name);

  // find the llvm function.
  llvm::Function *fn = module()->getFunction(full_name);
  DCHECK(fn != NULL);

  if (enable_ir_traces_ &&
      full_name.compare("printf") &&
      full_name.compare("printff")) {
    // Trace for debugging
    AddTrace("invoke native fn " + full_name);
  }

  // build a call to the llvm function.
  if (ret_type->isVoidTy()) {
    // void functions can't have a name for the call.
    return ir_builder().CreateCall(fn, args);
  } else {
    llvm::Value *value = ir_builder().CreateCall(fn, args, full_name);
    DCHECK(value->getType() == ret_type);
    return value;
  }
}

// Visitor for generating the code for a decomposed expression.
LLVMGenerator::Visitor::Visitor(LLVMGenerator *generator,
                                llvm::Function *function,
                                llvm::BasicBlock *entry_block,
                                llvm::BasicBlock *loop_block,
                                llvm::Value *arg_addrs,
                                llvm::Value *loop_var)
    : generator_(generator),
      entry_block_(entry_block),
      loop_block_(loop_block),
      arg_addrs_(arg_addrs),
      loop_var_(loop_var) {

  AddTrace("Iteration %T", loop_var);
}

void LLVMGenerator::Visitor::Visit(const VectorReadValueDex &dex) {
  llvm::IRBuilder<> &builder = ir_builder();

  builder.SetInsertPoint(entry_block_);
  llvm::Value *slot_ref = generator_->GetDataReference(arg_addrs_,
                                                       dex.DataIdx(),
                                                       dex.Field());

  builder.SetInsertPoint(loop_block_);
  llvm::Value *slot_value;
  if (dex.FieldType()->id() == arrow::Type::BOOL) {
    slot_value = generator_->GetPackedBitValue(slot_ref, loop_var_);
  } else {
    llvm::Value *slot_offset = builder.CreateGEP(slot_ref, loop_var_);
    slot_value = builder.CreateLoad(slot_offset, dex.FieldName());
  }

  AddTrace("visit data vector " + dex.FieldName() + " value %T", slot_value);
  result_.reset(new LValue(slot_value));
}

void LLVMGenerator::Visitor::Visit(const VectorReadValidityDex &dex) {
  llvm::IRBuilder<> &builder = ir_builder();

  builder.SetInsertPoint(entry_block_);
  llvm::Value *slot_ref = generator_->GetValidityReference(arg_addrs_,
                                                           dex.ValidityIdx(),
                                                           dex.Field());

  builder.SetInsertPoint(loop_block_);
  llvm::Value *validity = generator_->GetPackedBitValue(slot_ref, loop_var_);
  AddTrace("visit validity vector " + dex.FieldName() + " value %T", validity);
  result_.reset(new LValue(validity));
}

void LLVMGenerator::Visitor::Visit(const LiteralDex &dex) {
  // TODO
}

void LLVMGenerator::Visitor::Visit(const NonNullableFuncDex &dex) {
  AddTrace("visit NonNullableFunc base function " + dex.func_descriptor()->name());
  LLVMTypes *types = generator_->types_;

  // build the function params.
  std::vector<llvm::Value *> args;
  for (auto it = dex.args().begin(); it != dex.args().end(); it++) {
    // add value
    DexPtr value_expr = (*it)->value_expr();

    value_expr->Accept(this);
    args.push_back(result()->data());
  }

  const NativeFunction *native_function = dex.native_function();
  llvm::Type *ret_type = types->IRType(native_function->signature().ret_type()->id());
  llvm::Value *value = generator_->AddFunctionCall(native_function->pc_name(),
                                                   ret_type,
                                                   args);
  result_.reset(new LValue(value));
}

void LLVMGenerator::Visitor::Visit(const NullableNeverFuncDex &dex) {
  AddTrace("visit NullableNever base function " + dex.func_descriptor()->name());
  LLVMTypes *types = generator_->types_;

  // build the function params, along with the validities.
  std::vector<llvm::Value *> args;
  for (auto it = dex.args().begin(); it != dex.args().end(); it++) {
    ValueValidityPairPtr pair = *it;

    // build value.
    DexPtr value_expr = pair->value_expr();
    value_expr->Accept(this);
    args.push_back(result()->data());

    // build validity.
    llvm::Value *validity_expr = BuildCombinedValidity(pair->validity_exprs());
    args.push_back(validity_expr);
  }

  const NativeFunction *native_function = dex.native_function();
  llvm::Type *ret_type = types->IRType(native_function->signature().ret_type()->id());
  llvm::Value *value = generator_->AddFunctionCall(native_function->pc_name(),
                                                   ret_type,
                                                   args);
  result_.reset(new LValue(value));
}

/*
 * Bitwise-AND of a vector of bits to get the combined validity.
 */
llvm::Value *LLVMGenerator::Visitor::BuildCombinedValidity(const DexVector &validities) {
  llvm::IRBuilder<> &builder = ir_builder();
  LLVMTypes *types = generator_->types_;

  llvm::Value *isValid = types->true_constant();
  for (auto it = validities.begin(); it != validities.end(); it++) {
    (*it)->Accept(this);
    isValid = builder.CreateAnd(isValid, result()->data(), "validityBitAnd");
  }
  AddTrace("combined validity is %T", isValid);
  return isValid;
}

/*
 * Hooks for tracing/printfs.
 *
 * replace %T with the type-specific format specifier.
 * For some reason, float/double literals are getting lost when printing with the generic
 * printf. so, use a wrapper instead.
 */
std::string LLVMGenerator::ReplaceFormatInTrace(const std::string &in_msg,
                                                llvm::Value *value,
                                                std::string *print_fn) {
  std::string msg = in_msg;
  std::size_t pos = msg.find("%T");
  if (pos == std::string::npos) {
    assert(0);
    return msg;
  }

  llvm::Type *type = value->getType();
  const char *fmt = "";
  if (type->isIntegerTy(1) || type->isIntegerTy(32)) {
    // bit or int
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
  } else {
    assert(0);
  }
  msg.replace(pos, 2, fmt);
  return msg;
}

void LLVMGenerator::AddTrace(const std::string &msg, llvm::Value *value) {
  if (!enable_ir_traces_) {
    return;
  }

  std::string dmsg = "IR_TRACE:: " + msg + "\n";
  std::string print_fn_name = "printf";
  if (value) {
    dmsg = ReplaceFormatInTrace(dmsg, value, &print_fn_name);
  }
  trace_strings_.push_back(dmsg);

  // cast this to an llvm pointer.
  const char *str = trace_strings_.back().c_str();
  llvm::Constant *str_int_cast = types_->i64_constant((int64_t)str);
  llvm::Constant *str_ptr_cast = llvm::ConstantExpr::getIntToPtr(
                                   str_int_cast,
                                   types_->ptr_type(types_->i8_type()));

  std::vector<llvm::Value *> args;
  args.push_back(str_ptr_cast);
  if (value) {
    args.push_back(value);
  }
  AddFunctionCall(print_fn_name, types_->i32_type(), args);
}

} // namespace gandiva
