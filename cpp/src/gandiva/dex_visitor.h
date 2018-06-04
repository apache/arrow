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
#ifndef GANDIVA_DEX_DEXVISITOR_H
#define GANDIVA_DEX_DEXVISITOR_H

#include "gandiva/logging.h"

namespace gandiva {

class VectorReadValidityDex;
class VectorReadValueDex;
class LocalBitMapValidityDex;
class LiteralDex;
class NonNullableFuncDex;
class NullableNeverFuncDex;
class NullableInternalFuncDex;
class IfDex;

/// \brief Visitor for decomposed expression.
class DexVisitor {
 public:
  virtual void Visit(const VectorReadValidityDex &dex) = 0;
  virtual void Visit(const VectorReadValueDex &dex) = 0;
  virtual void Visit(const LocalBitMapValidityDex &dex) = 0;
  virtual void Visit(const LiteralDex &dex) = 0;
  virtual void Visit(const NonNullableFuncDex &dex) = 0;
  virtual void Visit(const NullableNeverFuncDex &dex) = 0;
  virtual void Visit(const NullableInternalFuncDex &dex) = 0;
  virtual void Visit(const IfDex &dex) = 0;
};

/// Default implementation with only DCHECK().
#define VISIT_DCHECK(DEX_CLASS)               \
  void Visit(const DEX_CLASS &dex) override { \
    DCHECK(0);                                \
  }

class DexDefaultVisitor : public DexVisitor {
  VISIT_DCHECK(VectorReadValidityDex);
  VISIT_DCHECK(VectorReadValueDex);
  VISIT_DCHECK(LocalBitMapValidityDex);
  VISIT_DCHECK(LiteralDex);
  VISIT_DCHECK(NonNullableFuncDex);
  VISIT_DCHECK(NullableNeverFuncDex);
  VISIT_DCHECK(NullableInternalFuncDex);
  VISIT_DCHECK(IfDex);
};

} // namespace gandiva

#endif //GANDIVA_DEX_DEXVISITOR_H
