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

#ifndef GANDIVA_DECIMAL_ADD_IR_BUILDER_H
#define GANDIVA_DECIMAL_ADD_IR_BUILDER_H

#include <memory>
#include <string>
#include <vector>

#include "gandiva/function_ir_builder.h"

namespace gandiva {

/// @brief Decimal IR functions
class DecimalIR : public FunctionIRBuilder {
 public:
  explicit DecimalIR(Engine* engine)
      : FunctionIRBuilder(engine), enable_ir_traces_(false) {}

  /// Build decimal IR functions and add them to the engine.
  static Status AddFunctions(Engine* engine);

  void EnableTraces() { enable_ir_traces_ = true; }

 private:
  /// The intrinsic fn for divide with small divisors is about 10x slower, so not
  /// using these.
  static const bool kUseOverflowIntrinsics = false;

  // Holder for an i128 value, and a bit indicating overflow.
  class ValueWithOverflow {
   public:
    ValueWithOverflow(llvm::Value* value, llvm::Value* overflow)
        : value_(value), overflow_(overflow) {}

    // Make from IR struct
    static ValueWithOverflow MakeFromIR(DecimalIR* decimal_ir, llvm::Value* dstruct);

    // Build a corresponding IR struct
    llvm::Value* BuildIRStruct(DecimalIR* decimal_ir) const;

    llvm::Value* value() const { return value_; }
    llvm::Value* overflow() const { return overflow_; }

   private:
    llvm::Value* value_;
    llvm::Value* overflow_;
  };

  // Holder for an i128 value, split into two i64s
  class ValueSplit {
   public:
    ValueSplit(llvm::Value* high, llvm::Value* low) : high_(high), low_(low) {}

    // Make from i128 value
    static ValueSplit Make(DecimalIR* decimal_ir, llvm::Value* in);

    // Make from IR struct
    static ValueSplit MakeFromIR(DecimalIR* decimal_ir, llvm::Value* dstruct);

    // Combine the two parts into an i128
    llvm::Value* Combine(DecimalIR* decimal_ir) const;

    llvm::Value* high() const { return high_; }
    llvm::Value* low() const { return low_; }

   private:
    llvm::Value* high_;
    llvm::Value* low_;
  };

  // Add global variables to the module.
  static void AddGlobals(Engine* engine);

  // Get intrinsic functions that are used by decimal operations.
  void GetIntrinsics();

  // Create IR builder for decimal add function.
  static Status MakeAdd(Engine* engine, std::shared_ptr<FunctionIRBuilder>* out);

  // Get the multiplier for specified scale (i.e 10^scale)
  llvm::Value* GetScaleMultiplier(llvm::Value* scale);

  // Get the higher of the two scales
  llvm::Value* GetHigherScale(llvm::Value* x_scale, llvm::Value* y_scale);

  // Increase scale of 'in_value' by 'increase_scale_by'.
  // - If 'increase_scale_by' is <= 0, does nothing.
  llvm::Value* IncreaseScale(llvm::Value* in_value, llvm::Value* increase_scale_by);

  // Similar to IncreaseScale. but, also check if there is overflow.
  ValueWithOverflow IncreaseScaleWithOverflowCheck(llvm::Value* in_value,
                                                   llvm::Value* increase_scale_by);

  // Reduce scale of 'in_value' by 'reduce_scale_by'.
  // - If 'reduce_scale_by' is <= 0, does nothing.
  llvm::Value* ReduceScale(llvm::Value* in_value, llvm::Value* reduce_scale_by);

  // Fast path of add: guaranteed no overflow
  llvm::Value* AddFastPath(llvm::Value* x_value, llvm::Value* x_scale,
                           llvm::Value* y_value, llvm::Value* y_scale);

  // Similar to AddFastPath, but check if there's an overflow.
  ValueWithOverflow AddWithOverflowCheck(llvm::Value* x_value, llvm::Value* x_scale,
                                         llvm::Value* y_value, llvm::Value* y_scale,
                                         llvm::Value* out_scale);

  // Do addition of large integers (both positive and negative).
  llvm::Value* AddLarge(llvm::Value* x_value, llvm::Value* x_precision,
                        llvm::Value* x_scale, llvm::Value* y_value,
                        llvm::Value* y_precision, llvm::Value* y_scale,
                        llvm::Value* out_precision, llvm::Value* out_scale);

  // Get the combined overflow (logical or).
  llvm::Value* GetCombinedOverflow(std::vector<ValueWithOverflow> values);

  // Build the function for adding decimals.
  Status BuildAdd();

  // Add a trace in IR code.
  void AddTrace(const std::string& fmt, std::vector<llvm::Value*> args);

  // Add a trace msg along with a 32-bit integer.
  void AddTrace32(const std::string& msg, llvm::Value* value);

  // Add a trace msg along with a 128-bit integer.
  void AddTrace128(const std::string& msg, llvm::Value* value);

  // name of the global variable having the array of scale multipliers.
  static const char kScaleMultipliersName[];

  // Intrinsic functions
  llvm::Function* sadd_with_overflow_fn_;
  llvm::Function* smul_with_overflow_fn_;

  // struct { i128: value, i1: overflow}
  llvm::Type* i128_with_overflow_struct_type_;

  // if set to true, ir traces are enabled. Useful for debugging.
  bool enable_ir_traces_;

  // trace strings - these need to be kept around since the execution happens much
  // after the code build.
  std::vector<std::unique_ptr<uint8_t>> trace_strings_;
};

}  // namespace gandiva

#endif  // GANDIVA_FUNCTION_IR_BUILDER_H
