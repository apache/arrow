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

#include "gandiva/llvm_includes.h"

namespace gandiva::internal {

inline bool HasRetAttr(const llvm::AttributeList& attributes,
                       llvm::Attribute::AttrKind kind) {
#if LLVM_VERSION_MAJOR >= 14
  return attributes.hasRetAttr(kind);
#else
  return attributes.hasAttribute(llvm::AttributeList::ReturnIndex, kind);
#endif
}

inline llvm::AttributeList AddRetAttr(llvm::LLVMContext& context,
                                      const llvm::AttributeList& attributes,
                                      llvm::Attribute::AttrKind kind) {
#if LLVM_VERSION_MAJOR >= 14
  return attributes.addRetAttribute(context, kind);
#else
  return attributes.addAttribute(context, llvm::AttributeList::ReturnIndex, kind);
#endif
}

inline void AddNativeBoolZExtAttrs(llvm::Function& function) {
  // Gandiva uses i1 parameters and results in native C++ mappings only for bool.
  const auto* function_type = function.getFunctionType();
  if (function_type->getReturnType()->isIntegerTy(1)) {
    // A native bool result must be zero-extended by the callee before it crosses
    // the ABI boundary. This matches Clang's lowering of C++ bool.
    function.setAttributes(AddRetAttr(function.getContext(), function.getAttributes(),
                                      llvm::Attribute::ZExt));
  }

  for (unsigned i = 0; i < function_type->getNumParams(); ++i) {
    if (function_type->getParamType(i)->isIntegerTy(1)) {
      // The caller must pass a native bool as 0 or 1.
      // LLVM 23 can replace `icmp ne (and X, 1), 0` with `trunc X to i1`; i1 only defines
      // bit 0, so this ABI attribute is required to normalize the value at the call.
      // https://github.com/llvm/llvm-project/pull/178977
      function.addParamAttr(i, llvm::Attribute::ZExt);
    }
  }
}

inline void CopyZExtAttrs(const llvm::Function& function, llvm::CallInst& call) {
  // https://llvm.org/docs/LangRef.html#parameter-attributes
  // "ABI attributes must be specified both at the function declaration/definition and
  // call-site, otherwise the behavior may be undefined. ABI attributes cannot be safely
  // dropped."
  //
  // TODO: Copy other ABI attributes as well. This currently copies only `zeroext`,
  // which is required for Gandiva's native bool parameters and results.
  if (HasRetAttr(function.getAttributes(), llvm::Attribute::ZExt)) {
    call.setAttributes(
        AddRetAttr(call.getContext(), call.getAttributes(), llvm::Attribute::ZExt));
  }

  for (unsigned i = 0; i < function.arg_size(); ++i) {
    if (function.hasParamAttribute(i, llvm::Attribute::ZExt)) {
      call.addParamAttr(i, llvm::Attribute::ZExt);
    }
  }
}

}  // namespace gandiva::internal
