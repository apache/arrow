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
#ifndef GANDIVA_TYPES_H
#define GANDIVA_TYPES_H

#include <memory>
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/function_signature.h"
#include "gandiva/gandiva_aliases.h"

namespace gandiva {

class NativeFunction;
class FunctionRegistry;
/// \brief Exports types supported by Gandiva for processing.
///
/// Has helper methods for clients to programatically discover
/// data types and functions supported by Gandiva.
class ExpressionRegistry {
 public:
  using iterator = const NativeFunction *;
  ExpressionRegistry();
  ~ExpressionRegistry();
  static DataTypeVector supported_types() { return supported_types_; }
  class FunctionSignatureIterator {
   public:
    FunctionSignatureIterator(iterator it) : it_(it) {}

    bool operator!=(const FunctionSignatureIterator &func_sign_it);

    FunctionSignature operator*();

    iterator operator++(int);

   private:
    iterator it_;
  };
  const FunctionSignatureIterator function_signature_begin();
  const FunctionSignatureIterator function_signature_end() const;

 private:
  static DataTypeVector supported_types_;
  static DataTypeVector InitSupportedTypes();
  static void AddArrowTypesToVector(arrow::Type::type &type, DataTypeVector &vector);
  std::unique_ptr<FunctionRegistry> function_registry_;
};
}  // namespace gandiva
#endif  // GANDIVA_TYPES_H
