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
#include <vector>

#include "gandiva/arrow.h"
#include "gandiva/function_signature.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/visibility.h"

namespace gandiva {

class NativeFunction;
class FunctionRegistry;
/// \brief Exports types supported by Gandiva for processing.
///
/// Has helper methods for clients to programmatically discover
/// data types and functions supported by Gandiva.
class GANDIVA_EXPORT ExpressionRegistry {
 public:
  using native_func_iterator_type = const NativeFunction*;
  using func_sig_iterator_type = const FunctionSignature*;
  ExpressionRegistry();
  ~ExpressionRegistry();
  static DataTypeVector supported_types() { return supported_types_; }
  class GANDIVA_EXPORT FunctionSignatureIterator {
   public:
    explicit FunctionSignatureIterator(native_func_iterator_type nf_it,
                                       native_func_iterator_type nf_it_end_);
    explicit FunctionSignatureIterator(func_sig_iterator_type fs_it);

    bool operator!=(const FunctionSignatureIterator& func_sign_it);

    FunctionSignature operator*();

    func_sig_iterator_type operator++(int);

   private:
    native_func_iterator_type native_func_it_;
    const native_func_iterator_type native_func_it_end_;
    func_sig_iterator_type func_sig_it_;
  };
  const FunctionSignatureIterator function_signature_begin();
  const FunctionSignatureIterator function_signature_end() const;

 private:
  static DataTypeVector supported_types_;
  std::unique_ptr<FunctionRegistry> function_registry_;
};

/// \brief Get the list of all function signatures.
GANDIVA_EXPORT
std::vector<std::shared_ptr<FunctionSignature>> GetRegisteredFunctionSignatures();

}  // namespace gandiva
