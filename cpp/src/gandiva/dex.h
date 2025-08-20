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

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "gandiva/dex_visitor.h"
#include "gandiva/field_descriptor.h"
#include "gandiva/func_descriptor.h"
#include "gandiva/function_holder.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/in_holder.h"
#include "gandiva/literal_holder.h"
#include "gandiva/native_function.h"
#include "gandiva/value_validity_pair.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Decomposed expression : the validity and value are separated.
class GANDIVA_EXPORT Dex {
 public:
  /// Derived classes should simply invoke the Visit api of the visitor.
  virtual void Accept(DexVisitor& visitor) = 0;
  virtual ~Dex() = default;
};

/// Base class for other Vector related Dex.
class GANDIVA_EXPORT VectorReadBaseDex : public Dex {
 public:
  explicit VectorReadBaseDex(FieldDescriptorPtr field_desc) : field_desc_(field_desc) {}

  const std::string& FieldName() const { return field_desc_->Name(); }

  DataTypePtr FieldType() const { return field_desc_->Type(); }

  FieldPtr Field() const { return field_desc_->field(); }

 protected:
  FieldDescriptorPtr field_desc_;
};

/// validity component of a ValueVector
class GANDIVA_EXPORT VectorReadValidityDex : public VectorReadBaseDex {
 public:
  explicit VectorReadValidityDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int ValidityIdx() const { return field_desc_->validity_idx(); }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// value component of a fixed-len ValueVector
class GANDIVA_EXPORT VectorReadFixedLenValueDex : public VectorReadBaseDex {
 public:
  explicit VectorReadFixedLenValueDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int DataIdx() const { return field_desc_->data_idx(); }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// value component of a variable-len ValueVector
class GANDIVA_EXPORT VectorReadVarLenValueDex : public VectorReadBaseDex {
 public:
  explicit VectorReadVarLenValueDex(FieldDescriptorPtr field_desc)
      : VectorReadBaseDex(field_desc) {}

  int DataIdx() const { return field_desc_->data_idx(); }

  int OffsetsIdx() const { return field_desc_->offsets_idx(); }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// validity based on a local bitmap.
class GANDIVA_EXPORT LocalBitMapValidityDex : public Dex {
 public:
  explicit LocalBitMapValidityDex(int local_bitmap_idx)
      : local_bitmap_idx_(local_bitmap_idx) {}

  int local_bitmap_idx() const { return local_bitmap_idx_; }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

 private:
  int local_bitmap_idx_;
};

/// base function expression
class GANDIVA_EXPORT FuncDex : public Dex {
 public:
  FuncDex(FuncDescriptorPtr func_descriptor, const NativeFunction* native_function,
          FunctionHolderPtr function_holder, int function_holder_idx,
          const ValueValidityPairVector& args)
      : func_descriptor_(func_descriptor),
        native_function_(native_function),
        function_holder_(function_holder),
        function_holder_idx_(function_holder_idx),
        args_(args) {}

  FuncDescriptorPtr func_descriptor() const { return func_descriptor_; }

  const NativeFunction* native_function() const { return native_function_; }

  int get_holder_idx() const { return function_holder_idx_; }

  const ValueValidityPairVector& args() const { return args_; }

 private:
  FuncDescriptorPtr func_descriptor_;
  const NativeFunction* native_function_;
  FunctionHolderPtr function_holder_;
  int function_holder_idx_;
  ValueValidityPairVector args_;
};

/// A function expression that only deals with non-null inputs, and generates non-null
/// outputs.
class GANDIVA_EXPORT NonNullableFuncDex : public FuncDex {
 public:
  NonNullableFuncDex(FuncDescriptorPtr func_descriptor,
                     const NativeFunction* native_function,
                     FunctionHolderPtr function_holder, int function_holder_idx,
                     const ValueValidityPairVector& args)
      : FuncDex(func_descriptor, native_function, function_holder, function_holder_idx,
                args) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// A function expression that deals with nullable inputs, but generates non-null
/// outputs.
class GANDIVA_EXPORT NullableNeverFuncDex : public FuncDex {
 public:
  NullableNeverFuncDex(FuncDescriptorPtr func_descriptor,
                       const NativeFunction* native_function,
                       FunctionHolderPtr function_holder, int function_holder_idx,
                       const ValueValidityPairVector& args)
      : FuncDex(func_descriptor, native_function, function_holder, function_holder_idx,
                args) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// A function expression that deals with nullable inputs, and
/// nullable outputs.
class GANDIVA_EXPORT NullableInternalFuncDex : public FuncDex {
 public:
  NullableInternalFuncDex(FuncDescriptorPtr func_descriptor,
                          const NativeFunction* native_function,
                          FunctionHolderPtr function_holder, int function_holder_idx,
                          const ValueValidityPairVector& args, int local_bitmap_idx)
      : FuncDex(func_descriptor, native_function, function_holder, function_holder_idx,
                args),
        local_bitmap_idx_(local_bitmap_idx) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

  /// The validity of the function result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

 private:
  int local_bitmap_idx_;
};

/// special validity type that always returns true.
class GANDIVA_EXPORT TrueDex : public Dex {
  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// special validity type that always returns false.
class GANDIVA_EXPORT FalseDex : public Dex {
  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// decomposed expression for a literal.
class GANDIVA_EXPORT LiteralDex : public Dex {
 public:
  LiteralDex(DataTypePtr type, const LiteralHolder& holder)
      : type_(type), holder_(holder) {}

  const DataTypePtr& type() const { return type_; }

  const LiteralHolder& holder() const { return holder_; }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

 private:
  DataTypePtr type_;
  LiteralHolder holder_;
};

/// decomposed if-else expression.
class GANDIVA_EXPORT IfDex : public Dex {
 public:
  IfDex(ValueValidityPairPtr condition_vv, ValueValidityPairPtr then_vv,
        ValueValidityPairPtr else_vv, DataTypePtr result_type, int local_bitmap_idx,
        bool is_terminal_else)
      : condition_vv_(condition_vv),
        then_vv_(then_vv),
        else_vv_(else_vv),
        result_type_(result_type),
        local_bitmap_idx_(local_bitmap_idx),
        is_terminal_else_(is_terminal_else) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

  const ValueValidityPair& condition_vv() const { return *condition_vv_; }
  const ValueValidityPair& then_vv() const { return *then_vv_; }
  const ValueValidityPair& else_vv() const { return *else_vv_; }

  /// The validity of the result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

  /// is this a terminal else ? i.e no nested if-else underneath.
  bool is_terminal_else() const { return is_terminal_else_; }

  const DataTypePtr& result_type() const { return result_type_; }

 private:
  ValueValidityPairPtr condition_vv_;
  ValueValidityPairPtr then_vv_;
  ValueValidityPairPtr else_vv_;
  DataTypePtr result_type_;
  int local_bitmap_idx_;
  bool is_terminal_else_;
};

// decomposed boolean expression.
class GANDIVA_EXPORT BooleanDex : public Dex {
 public:
  BooleanDex(const ValueValidityPairVector& args, int local_bitmap_idx)
      : args_(args), local_bitmap_idx_(local_bitmap_idx) {}

  const ValueValidityPairVector& args() const { return args_; }

  /// The validity of the result is saved in this bitmap.
  int local_bitmap_idx() const { return local_bitmap_idx_; }

 private:
  ValueValidityPairVector args_;
  int local_bitmap_idx_;
};

/// Boolean-AND expression
class GANDIVA_EXPORT BooleanAndDex : public BooleanDex {
 public:
  BooleanAndDex(const ValueValidityPairVector& args, int local_bitmap_idx)
      : BooleanDex(args, local_bitmap_idx) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

/// Boolean-OR expression
class GANDIVA_EXPORT BooleanOrDex : public BooleanDex {
 public:
  BooleanOrDex(const ValueValidityPairVector& args, int local_bitmap_idx)
      : BooleanDex(args, local_bitmap_idx) {}

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
};

// decomposed in expression.
template <typename Type>
class InExprDex;

template <typename Type>
class InExprDexBase : public Dex {
 public:
  InExprDexBase(const ValueValidityPairVector& args,
                const std::unordered_set<Type>& values)
      : args_(args) {
    in_holder_.reset(new InHolder<Type>(values));
  }

  const ValueValidityPairVector& args() const { return args_; }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

  const std::string& runtime_function() const { return runtime_function_; }

  const std::shared_ptr<InHolder<Type>>& in_holder() const { return in_holder_; }

  void set_holder_idx(int holder_idx) { holder_idx_ = holder_idx; }

  int get_holder_idx() const { return holder_idx_; }

 protected:
  ValueValidityPairVector args_;
  std::string runtime_function_;
  std::shared_ptr<InHolder<Type>> in_holder_;
  int holder_idx_;
};

template <>
class InExprDexBase<gandiva::DecimalScalar128> : public Dex {
 public:
  InExprDexBase(const ValueValidityPairVector& args,
                const std::unordered_set<gandiva::DecimalScalar128>& values,
                int32_t precision, int32_t scale)
      : args_(args), precision_(precision), scale_(scale) {
    in_holder_.reset(new InHolder<gandiva::DecimalScalar128>(values));
  }

  int32_t get_precision() const { return precision_; }

  int32_t get_scale() const { return scale_; }

  const ValueValidityPairVector& args() const { return args_; }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

  const std::string& runtime_function() const { return runtime_function_; }

  const std::shared_ptr<InHolder<gandiva::DecimalScalar128>>& in_holder() const {
    return in_holder_;
  }

  void set_holder_idx(int holder_idx) { holder_idx_ = holder_idx; }

  int get_holder_idx() const { return holder_idx_; }

 protected:
  ValueValidityPairVector args_;
  std::string runtime_function_;
  std::shared_ptr<InHolder<gandiva::DecimalScalar128>> in_holder_;
  int32_t precision_, scale_;
  int holder_idx_;
};

template <>
class InExprDex<int32_t> : public InExprDexBase<int32_t> {
 public:
  InExprDex(const ValueValidityPairVector& args,
            const std::unordered_set<int32_t>& values)
      : InExprDexBase(args, values) {
    runtime_function_ = "gdv_fn_in_expr_lookup_int32";
  }
};

template <>
class InExprDex<int64_t> : public InExprDexBase<int64_t> {
 public:
  InExprDex(const ValueValidityPairVector& args,
            const std::unordered_set<int64_t>& values)
      : InExprDexBase(args, values) {
    runtime_function_ = "gdv_fn_in_expr_lookup_int64";
  }
};

template <>
class InExprDex<float> : public InExprDexBase<float> {
 public:
  InExprDex(const ValueValidityPairVector& args, const std::unordered_set<float>& values)
      : InExprDexBase(args, values) {
    runtime_function_ = "gdv_fn_in_expr_lookup_float";
  }
};

template <>
class InExprDex<double> : public InExprDexBase<double> {
 public:
  InExprDex(const ValueValidityPairVector& args, const std::unordered_set<double>& values)
      : InExprDexBase(args, values) {
    runtime_function_ = "gdv_fn_in_expr_lookup_double";
  }
};

template <>
class InExprDex<gandiva::DecimalScalar128>
    : public InExprDexBase<gandiva::DecimalScalar128> {
 public:
  InExprDex(const ValueValidityPairVector& args,
            const std::unordered_set<gandiva::DecimalScalar128>& values,
            int32_t precision, int32_t scale)
      : InExprDexBase<gandiva::DecimalScalar128>(args, values, precision, scale) {
    runtime_function_ = "gdv_fn_in_expr_lookup_decimal";
  }
};

template <>
class InExprDex<std::string> : public InExprDexBase<std::string> {
 public:
  InExprDex(const ValueValidityPairVector& args,
            const std::unordered_set<std::string>& values)
      : InExprDexBase(args, values) {
    runtime_function_ = "gdv_fn_in_expr_lookup_utf8";
  }
};

class PreEvalInExprDex : public Dex {
 public:
  PreEvalInExprDex(const ValueValidityPairPtr& eval_expr_vv,
                   const ValueValidityPairPtr& condition_eval_expr_vv)
      : eval_expr_vv_(eval_expr_vv), condition_eval_expr_vv_(condition_eval_expr_vv){};
  const ValueValidityPairPtr& eval_expr_vv() const { return eval_expr_vv_; }
  const ValueValidityPairPtr& condition_eval_expr_vv() const {
    return condition_eval_expr_vv_;
  }

  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }

 private:
  ValueValidityPairPtr eval_expr_vv_;
  ValueValidityPairPtr condition_eval_expr_vv_;
};

class ReadProxyDex : public Dex {
 public:
  ReadProxyDex(DataTypePtr type) : type_(type){};
  void Accept(DexVisitor& visitor) override { visitor.Visit(*this); }
  const DataTypePtr& type() const { return type_; }

 private:
  DataTypePtr type_;
};

}  // namespace gandiva
