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

#include <cmath>
#include <string>

#include "arrow/util/logging.h"
#include "gandiva/decimal_scalar.h"
#include "gandiva/visibility.h"

namespace gandiva {

class VectorReadValidityDex;
class VectorReadFixedLenValueDex;
class VectorReadVarLenValueDex;
class LocalBitMapValidityDex;
class LiteralDex;
class TrueDex;
class FalseDex;
class NonNullableFuncDex;
class NullableNeverFuncDex;
class NullableInternalFuncDex;
class IfDex;
class BooleanAndDex;
class BooleanOrDex;
template <typename Type>
class InExprDexBase;

/// \brief Visitor for decomposed expression.
class GANDIVA_EXPORT DexVisitor {
 public:
  virtual ~DexVisitor() = default;

  virtual void Visit(const VectorReadValidityDex& dex) = 0;
  virtual void Visit(const VectorReadFixedLenValueDex& dex) = 0;
  virtual void Visit(const VectorReadVarLenValueDex& dex) = 0;
  virtual void Visit(const LocalBitMapValidityDex& dex) = 0;
  virtual void Visit(const TrueDex& dex) = 0;
  virtual void Visit(const FalseDex& dex) = 0;
  virtual void Visit(const LiteralDex& dex) = 0;
  virtual void Visit(const NonNullableFuncDex& dex) = 0;
  virtual void Visit(const NullableNeverFuncDex& dex) = 0;
  virtual void Visit(const NullableInternalFuncDex& dex) = 0;
  virtual void Visit(const IfDex& dex) = 0;
  virtual void Visit(const BooleanAndDex& dex) = 0;
  virtual void Visit(const BooleanOrDex& dex) = 0;
  virtual void Visit(const InExprDexBase<int32_t>& dex) = 0;
  virtual void Visit(const InExprDexBase<int64_t>& dex) = 0;
  virtual void Visit(const InExprDexBase<float>& dex) = 0;
  virtual void Visit(const InExprDexBase<double>& dex) = 0;
  virtual void Visit(const InExprDexBase<gandiva::DecimalScalar128>& dex) = 0;
  virtual void Visit(const InExprDexBase<std::string>& dex) = 0;
};

/// Default implementation with only DCHECK().
#define VISIT_DCHECK(DEX_CLASS) \
  void Visit(const DEX_CLASS& dex) override { ARROW_DCHECK(0); }

class GANDIVA_EXPORT DexDefaultVisitor : public DexVisitor {
  VISIT_DCHECK(VectorReadValidityDex)
  VISIT_DCHECK(VectorReadFixedLenValueDex)
  VISIT_DCHECK(VectorReadVarLenValueDex)
  VISIT_DCHECK(LocalBitMapValidityDex)
  VISIT_DCHECK(TrueDex)
  VISIT_DCHECK(FalseDex)
  VISIT_DCHECK(LiteralDex)
  VISIT_DCHECK(NonNullableFuncDex)
  VISIT_DCHECK(NullableNeverFuncDex)
  VISIT_DCHECK(NullableInternalFuncDex)
  VISIT_DCHECK(IfDex)
  VISIT_DCHECK(BooleanAndDex)
  VISIT_DCHECK(BooleanOrDex)
  VISIT_DCHECK(InExprDexBase<int32_t>)
  VISIT_DCHECK(InExprDexBase<int64_t>)
  VISIT_DCHECK(InExprDexBase<float>)
  VISIT_DCHECK(InExprDexBase<double>)
  VISIT_DCHECK(InExprDexBase<gandiva::DecimalScalar128>)
  VISIT_DCHECK(InExprDexBase<std::string>)
};

}  // namespace gandiva
