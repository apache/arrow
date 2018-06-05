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

#include "codegen/expr_decomposer.h"

#include <memory>
#include <string>
#include <vector>
#include "codegen/dex.h"
#include "codegen/node.h"
#include "codegen/function_registry.h"
#include "codegen/function_signature.h"
#include "codegen/annotator.h"

namespace gandiva {

// Decompose a field node - simply seperate out validity & value arrays.
void ExprDecomposer::Visit(const FieldNode &node) {
  auto desc = annotator_.CheckAndAddInputFieldDescriptor(node.field());

  DexPtr validity_dex = std::make_shared<VectorReadValidityDex>(desc);
  DexPtr value_dex = std::make_shared<VectorReadValueDex>(desc);
  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
}

// Decompose a field node - wherever possible, merge the validity vectors of the
// child nodes.
void ExprDecomposer::Visit(const FunctionNode &node) {
  auto desc = node.descriptor();
  FunctionSignature signature(desc->name(),
                              desc->params(),
                              desc->return_type());
  const NativeFunction *native_function = registry_.LookupSignature(signature);
  DCHECK(native_function) << "Missing Signature " << signature.ToString();

  // decompose the children.
  std::vector<ValueValidityPairPtr> args;
  for (auto &child : node.children()) {
    child->Accept(*this);
    args.push_back(result());
  }

  if (native_function->result_nullable_type() == RESULT_NULL_IF_NULL) {
    // These functions are decomposable, merge the validity bits of the children.

    std::vector<DexPtr> merged_validity;
    for (auto &decomposed : args) {
      // Merge the validity_expressions of the children to build a combined validity
      // expression.
      merged_validity.insert(merged_validity.end(),
                             decomposed->validity_exprs().begin(),
                             decomposed->validity_exprs().end());
    }

    auto value_dex = std::make_shared<NonNullableFuncDex>(desc, native_function, args);
    result_ = std::make_shared<ValueValidityPair>(merged_validity, value_dex);
  } else if (native_function->result_nullable_type() == RESULT_NULL_NEVER) {
    // These functions always output valid results. So, no validity dex.
    auto value_dex = std::make_shared<NullableNeverFuncDex>(desc, native_function, args);
    result_ = std::make_shared<ValueValidityPair>(value_dex);
  } else {
    DCHECK(native_function->result_nullable_type() == RESULT_NULL_INTERNAL);

    // Add a local bitmap to track the output validity.
    int local_bitmap_idx = annotator_.AddLocalBitMap();
    auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);

    auto value_dex = std::make_shared<NullableInternalFuncDex>(desc,
                                                               native_function,
                                                               args,
                                                               local_bitmap_idx);
    result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  }
}

// Decompose an IfNode
void ExprDecomposer::Visit(const IfNode &node) {
  // Add a local bitmap to track the output validity.
  int local_bitmap_idx = annotator_.AddLocalBitMap();
  auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);

  node.condition()->Accept(*this);
  auto condition_vv = result();

  node.then_node()->Accept(*this);
  auto then_vv = result();

  node.else_node()->Accept(*this);
  auto else_vv = result();

  auto value_dex = std::make_shared<IfDex>(condition_vv,
                                           then_vv,
                                           else_vv,
                                           node.return_type(),
                                           local_bitmap_idx);

  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
}

void ExprDecomposer::Visit(const LiteralNode &node) {
  auto value_dex = std::make_shared<LiteralDex>(node.return_type(), node.holder());
  result_ = std::make_shared<ValueValidityPair>(value_dex);
}

} // namespace gandiva
