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

#include <sstream>
#include <utility>

#include "arrow/status.h"
#include "gandiva/decimal_ir.h"
#include "gandiva/decimal_type_util.h"

// Algorithms adapted from Apache Impala

namespace gandiva {

#define ADD_TRACE_32(msg, value) \
  if (enable_ir_traces_) {       \
    AddTrace32(msg, value);      \
  }
#define ADD_TRACE_128(msg, value) \
  if (enable_ir_traces_) {        \
    AddTrace128(msg, value);      \
  }

const char* DecimalIR::kScaleMultipliersName = "gandivaScaleMultipliers";

/// Populate globals required by decimal IR.
/// TODO: can this be done just once ?
void DecimalIR::AddGlobals(Engine* engine) {
  auto types = engine->types();

  // populate vector : [ 1, 10, 100, 1000, ..]
  std::string value = "1";
  std::vector<llvm::Constant*> scale_multipliers;
  for (int i = 0; i < DecimalTypeUtil::kMaxPrecision + 1; ++i) {
    auto multiplier =
        llvm::ConstantInt::get(llvm::Type::getInt128Ty(*engine->context()), value, 10);
    scale_multipliers.push_back(multiplier);
    value.append("0");
  }

  auto array_type =
      llvm::ArrayType::get(types->i128_type(), DecimalTypeUtil::kMaxPrecision + 1);
  auto initializer = llvm::ConstantArray::get(
      array_type, llvm::ArrayRef<llvm::Constant*>(scale_multipliers));

  auto globalScaleMultipliers = new llvm::GlobalVariable(
      *engine->module(), array_type, true /*constant*/,
      llvm::GlobalValue::LinkOnceAnyLinkage, initializer, kScaleMultipliersName);
  globalScaleMultipliers->setAlignment(16);
}

// Lookup intrinsic functions
void DecimalIR::InitializeIntrinsics() {
  sadd_with_overflow_fn_ = llvm::Intrinsic::getDeclaration(
      module(), llvm::Intrinsic::sadd_with_overflow, types()->i128_type());
  DCHECK_NE(sadd_with_overflow_fn_, nullptr);

  smul_with_overflow_fn_ = llvm::Intrinsic::getDeclaration(
      module(), llvm::Intrinsic::smul_with_overflow, types()->i128_type());
  DCHECK_NE(smul_with_overflow_fn_, nullptr);

  i128_with_overflow_struct_type_ =
      sadd_with_overflow_fn_->getFunctionType()->getReturnType();
}

// CPP:  return kScaleMultipliers[scale]
llvm::Value* DecimalIR::GetScaleMultiplier(llvm::Value* scale) {
  auto const_array = module()->getGlobalVariable(kScaleMultipliersName);
  auto ptr = ir_builder()->CreateGEP(const_array, {types()->i32_constant(0), scale});
  return ir_builder()->CreateLoad(ptr);
}

// CPP:  x <= y ? y : x
llvm::Value* DecimalIR::GetHigherScale(llvm::Value* x_scale, llvm::Value* y_scale) {
  llvm::Value* le = ir_builder()->CreateICmpSLE(x_scale, y_scale);
  return ir_builder()->CreateSelect(le, y_scale, x_scale);
}

// CPP: return (increase_scale_by <= 0)  ?
//              in_value : in_value * GetScaleMultiplier(increase_scale_by)
llvm::Value* DecimalIR::IncreaseScale(llvm::Value* in_value,
                                      llvm::Value* increase_scale_by) {
  llvm::Value* le_zero =
      ir_builder()->CreateICmpSLE(increase_scale_by, types()->i32_constant(0));
  // then block
  auto then_lambda = [&] { return in_value; };

  // else block
  auto else_lambda = [&] {
    llvm::Value* multiplier = GetScaleMultiplier(increase_scale_by);
    return ir_builder()->CreateMul(in_value, multiplier);
  };

  return BuildIfElse(le_zero, types()->i128_type(), then_lambda, else_lambda);
}

// CPP: return (increase_scale_by <= 0)  ?
//              {in_value,false} : {in_value * GetScaleMultiplier(increase_scale_by),true}
//
// The return value also indicates if there was an overflow while increasing the scale.
DecimalIR::ValueWithOverflow DecimalIR::IncreaseScaleWithOverflowCheck(
    llvm::Value* in_value, llvm::Value* increase_scale_by) {
  llvm::Value* le_zero =
      ir_builder()->CreateICmpSLE(increase_scale_by, types()->i32_constant(0));

  // then block
  auto then_lambda = [&] {
    ValueWithOverflow ret{in_value, types()->false_constant()};
    return ret.AsStruct(this);
  };

  // else block
  auto else_lambda = [&] {
    llvm::Value* multiplier = GetScaleMultiplier(increase_scale_by);
    return ir_builder()->CreateCall(smul_with_overflow_fn_, {in_value, multiplier});
  };

  auto ir_struct =
      BuildIfElse(le_zero, i128_with_overflow_struct_type_, then_lambda, else_lambda);
  return ValueWithOverflow::MakeFromStruct(this, ir_struct);
}

// CPP: return (reduce_scale_by <= 0)  ?
//              in_value : in_value / GetScaleMultiplier(reduce_scale_by)
//
// ReduceScale cannot cause an overflow.
llvm::Value* DecimalIR::ReduceScale(llvm::Value* in_value, llvm::Value* reduce_scale_by) {
  auto le_zero = ir_builder()->CreateICmpSLE(reduce_scale_by, types()->i32_constant(0));
  // then block
  auto then_lambda = [&] { return in_value; };

  // else block
  auto else_lambda = [&] {
    // TODO : handle rounding.
    llvm::Value* multiplier = GetScaleMultiplier(reduce_scale_by);
    return ir_builder()->CreateSDiv(in_value, multiplier);
  };

  return BuildIfElse(le_zero, types()->i128_type(), then_lambda, else_lambda);
}

/// @brief Fast-path for add
/// Adjust x and y to the same scale, and add them.
llvm::Value* DecimalIR::AddFastPath(const ValueFull& x, const ValueFull& y) {
  auto higher_scale = GetHigherScale(x.scale(), y.scale());
  ADD_TRACE_32("AddFastPath : higher_scale", higher_scale);

  // CPP : x_scaled = IncreaseScale(x_value, higher_scale - x_scale)
  auto x_delta = ir_builder()->CreateSub(higher_scale, x.scale());
  auto x_scaled = IncreaseScale(x.value(), x_delta);
  ADD_TRACE_128("AddFastPath : x_scaled", x_scaled);

  // CPP : y_scaled = IncreaseScale(y_value, higher_scale - y_scale)
  auto y_delta = ir_builder()->CreateSub(higher_scale, y.scale());
  auto y_scaled = IncreaseScale(y.value(), y_delta);
  ADD_TRACE_128("AddFastPath : y_scaled", y_scaled);

  auto sum = ir_builder()->CreateAdd(x_scaled, y_scaled);
  ADD_TRACE_128("AddFastPath : sum", sum);
  return sum;
}

// @brief Add with overflow check.
/// Adjust x and y to the same scale, add them, and reduce sum to output scale.
/// If there is an overflow, the sum is set to 0.
DecimalIR::ValueWithOverflow DecimalIR::AddWithOverflowCheck(const ValueFull& x,
                                                             const ValueFull& y,
                                                             const ValueFull& out) {
  auto higher_scale = GetHigherScale(x.scale(), y.scale());
  ADD_TRACE_32("AddWithOverflowCheck : higher_scale", higher_scale);

  // CPP : x_scaled = IncreaseScale(x_value, higher_scale - x.scale())
  auto x_delta = ir_builder()->CreateSub(higher_scale, x.scale());
  auto x_scaled = IncreaseScaleWithOverflowCheck(x.value(), x_delta);
  ADD_TRACE_128("AddWithOverflowCheck : x_scaled", x_scaled.value());

  // CPP : y_scaled = IncreaseScale(y_value, higher_scale - y_scale)
  auto y_delta = ir_builder()->CreateSub(higher_scale, y.scale());
  auto y_scaled = IncreaseScaleWithOverflowCheck(y.value(), y_delta);
  ADD_TRACE_128("AddWithOverflowCheck : y_scaled", y_scaled.value());

  // CPP : sum = x_scaled + y_scaled
  auto sum_ir_struct = ir_builder()->CreateCall(sadd_with_overflow_fn_,
                                                {x_scaled.value(), y_scaled.value()});
  auto sum = ValueWithOverflow::MakeFromStruct(this, sum_ir_struct);
  ADD_TRACE_128("AddWithOverflowCheck : sum", sum.value());

  // CPP : overflow ? 0 : sum / GetScaleMultiplier(max_scale - out_scale)
  auto overflow = GetCombinedOverflow({x_scaled, y_scaled, sum});
  ADD_TRACE_32("AddWithOverflowCheck : overflow", overflow);
  auto then_lambda = [&] {
    // if there is an overflow, the value returned won't be used. so, save the division.
    return types()->i128_constant(0);
  };
  auto else_lambda = [&] {
    auto reduce_scale_by = ir_builder()->CreateSub(higher_scale, out.scale());
    return ReduceScale(sum.value(), reduce_scale_by);
  };
  auto sum_descaled =
      BuildIfElse(overflow, types()->i128_type(), then_lambda, else_lambda);
  return ValueWithOverflow(sum_descaled, overflow);
}

// This is pretty complex, so use CPP fns.
llvm::Value* DecimalIR::AddLarge(const ValueFull& x, const ValueFull& y,
                                 const ValueFull& out) {
  auto block = ir_builder()->GetInsertBlock();
  auto out_high_ptr = new llvm::AllocaInst(types()->i64_type(), 0, "out_hi", block);
  auto out_low_ptr = new llvm::AllocaInst(types()->i64_type(), 0, "out_low", block);
  auto x_split = ValueSplit::MakeFromInt128(this, x.value());
  auto y_split = ValueSplit::MakeFromInt128(this, y.value());

  std::vector<llvm::Value*> args = {
      x_split.high(),  x_split.low(), x.precision(), x.scale(),
      y_split.high(),  y_split.low(), y.precision(), y.scale(),
      out.precision(), out.scale(),   out_high_ptr,  out_low_ptr,
  };
  ir_builder()->CreateCall(module()->getFunction("add_large_decimal128_decimal128"),
                           args);

  auto out_high = ir_builder()->CreateLoad(out_high_ptr);
  auto out_low = ir_builder()->CreateLoad(out_low_ptr);
  auto sum = ValueSplit(out_high, out_low).AsInt128(this);
  ADD_TRACE_128("AddLarge : sum", sum);
  return sum;
}

/// The output scale/precision cannot be arbitary values. The algo here depends on them
/// to be the same as computed in DecimalTypeSql.
/// TODO: enforce this.
Status DecimalIR::BuildAdd() {
  // Create fn prototype :
  // int128_t
  // add_decimal128_decimal128(int128_t x_value, int32_t x_precision, int32_t x_scale,
  //                           int128_t y_value, int32_t y_precision, int32_t y_scale
  //                           int32_t out_precision, int32_t out_scale)
  auto i32 = types()->i32_type();
  auto i128 = types()->i128_type();
  auto function = BuildFunction("add_decimal128_decimal128", i128,
                                {
                                    {"x_value", i128},
                                    {"x_precision", i32},
                                    {"x_scale", i32},
                                    {"y_value", i128},
                                    {"y_precision", i32},
                                    {"y_scale", i32},
                                    {"out_precision", i32},
                                    {"out_scale", i32},
                                });

  auto arg_iter = function->arg_begin();
  ValueFull x(&arg_iter[0], &arg_iter[1], &arg_iter[2]);
  ValueFull y(&arg_iter[3], &arg_iter[4], &arg_iter[5]);
  ValueFull out(nullptr, &arg_iter[6], &arg_iter[7]);

  auto entry = llvm::BasicBlock::Create(*context(), "entry", function);
  ir_builder()->SetInsertPoint(entry);

  // CPP :
  // if (out_precision < 38) {
  //   return AddFastPath(x, y)
  // } else {
  //   ret = AddWithOverflowCheck(x, y)
  //   if (ret.overflow)
  //     return AddLarge(x, y)
  //   else
  //     return ret.value;
  // }
  llvm::Value* lt_max_precision = ir_builder()->CreateICmpSLT(
      out.precision(), types()->i32_constant(DecimalTypeUtil::kMaxPrecision));
  auto then_lambda = [&] {
    // fast-path add
    return AddFastPath(x, y);
  };
  auto else_lambda = [&] {
    if (kUseOverflowIntrinsics) {
      // do the add and check if there was overflow
      auto ret = AddWithOverflowCheck(x, y, out);

      // if there is an overflow, switch to the AddLarge codepath.
      return BuildIfElse(ret.overflow(), types()->i128_type(),
                         [&] { return AddLarge(x, y, out); },
                         [&] { return ret.value(); });
    } else {
      return AddLarge(x, y, out);
    }
  };
  auto value =
      BuildIfElse(lt_max_precision, types()->i128_type(), then_lambda, else_lambda);

  // store result to out
  ir_builder()->CreateRet(value);
  return Status::OK();
}

Status DecimalIR::AddFunctions(Engine* engine) {
  auto decimal_ir = std::make_shared<DecimalIR>(engine);

  // Populate global variables used by decimal operations.
  decimal_ir->AddGlobals(engine);

  // Lookup intrinsic functions
  decimal_ir->InitializeIntrinsics();

  // build "add"
  return decimal_ir->BuildAdd();
}

// Do an bitwise-or of all the overflow bits.
llvm::Value* DecimalIR::GetCombinedOverflow(
    std::vector<DecimalIR::ValueWithOverflow> vec) {
  llvm::Value* res = types()->false_constant();
  for (auto& val : vec) {
    res = ir_builder()->CreateOr(res, val.overflow());
  }
  return res;
}

DecimalIR::ValueSplit DecimalIR::ValueSplit::MakeFromInt128(DecimalIR* decimal_ir,
                                                            llvm::Value* in) {
  auto builder = decimal_ir->ir_builder();
  auto types = decimal_ir->types();

  auto high = builder->CreateLShr(in, types->i128_constant(64));
  high = builder->CreateTrunc(high, types->i64_type());
  auto low = builder->CreateTrunc(in, types->i64_type());
  return ValueSplit(high, low);
}

/// Convert IR struct {%i64, %i64} to cpp class ValueSplit
DecimalIR::ValueSplit DecimalIR::ValueSplit::MakeFromStruct(DecimalIR* decimal_ir,
                                                            llvm::Value* dstruct) {
  auto builder = decimal_ir->ir_builder();
  auto high = builder->CreateExtractValue(dstruct, 0);
  auto low = builder->CreateExtractValue(dstruct, 1);
  return DecimalIR::ValueSplit(high, low);
}

llvm::Value* DecimalIR::ValueSplit::AsInt128(DecimalIR* decimal_ir) const {
  auto builder = decimal_ir->ir_builder();
  auto types = decimal_ir->types();

  auto value = builder->CreateSExt(high_, types->i128_type());
  value = builder->CreateShl(value, types->i128_constant(64));
  value = builder->CreateAdd(value, builder->CreateZExt(low_, types->i128_type()));
  return value;
}

/// Convert IR struct {%i128, %i1} to cpp class ValueWithOverflow
DecimalIR::ValueWithOverflow DecimalIR::ValueWithOverflow::MakeFromStruct(
    DecimalIR* decimal_ir, llvm::Value* dstruct) {
  auto builder = decimal_ir->ir_builder();
  auto value = builder->CreateExtractValue(dstruct, 0);
  auto overflow = builder->CreateExtractValue(dstruct, 1);
  return DecimalIR::ValueWithOverflow(value, overflow);
}

/// Convert to IR struct {%i128, %i1}
llvm::Value* DecimalIR::ValueWithOverflow::AsStruct(DecimalIR* decimal_ir) const {
  auto builder = decimal_ir->ir_builder();

  auto undef = llvm::UndefValue::get(decimal_ir->i128_with_overflow_struct_type_);
  auto struct_val = builder->CreateInsertValue(undef, value(), 0);
  return builder->CreateInsertValue(struct_val, overflow(), 1);
}

/// debug traces
void DecimalIR::AddTrace(const std::string& fmt, std::vector<llvm::Value*> args) {
  DCHECK(enable_ir_traces_);

  auto ir_str = ir_builder()->CreateGlobalStringPtr(fmt);
  args.insert(args.begin(), ir_str);
  ir_builder()->CreateCall(module()->getFunction("printf"), args, "trace");
}

void DecimalIR::AddTrace32(const std::string& msg, llvm::Value* value) {
  AddTrace("DECIMAL_IR_TRACE:: " + msg + " %d\n", {value});
}

void DecimalIR::AddTrace128(const std::string& msg, llvm::Value* value) {
  // convert i128 into two i64s for printing
  auto split = ValueSplit::MakeFromInt128(this, value);
  AddTrace("DECIMAL_IR_TRACE:: " + msg + " %llx:%llx (%lld:%llu)\n",
           {split.high(), split.low(), split.high(), split.low()});
}

}  // namespace gandiva
