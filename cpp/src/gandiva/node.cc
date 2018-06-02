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

#include <memory>
#include <string>
#include <vector>
#include "gandiva/node.h"
#include "codegen/dex.h"
#include "codegen/function_registry.h"
#include "codegen/function_signature.h"
#include "codegen/annotator.h"

namespace gandiva {

ValueValidityPairPtr FieldNode::Decompose(const FunctionRegistry &registry,
                                          Annotator &annotator) {
  FieldDescriptorPtr desc = annotator.CheckAndAddInputFieldDescriptor(field_);

  DexPtr validity_dex = std::make_shared<VectorReadValidityDex>(desc);
  DexPtr value_dex = std::make_shared<VectorReadValueDex>(desc);
  return std::make_shared<ValueValidityPair>(validity_dex, value_dex);
}

ValueValidityPairPtr FunctionNode::Decompose(const FunctionRegistry &registry,
                                             Annotator &annotator) {
  FunctionSignature signature(desc_->name(),
                              desc_->params(),
                              desc_->return_type());
  const NativeFunction *native_function = registry.LookupSignature(signature);
  DCHECK(native_function);

  // decompose the children.
  std::vector<ValueValidityPairPtr> args;
  for (auto &child : children_) {
    ValueValidityPairPtr decomposed = child->Decompose(registry, annotator);
    args.push_back(decomposed);
  }

  if (native_function->result_nullable_type() == RESULT_NULL_IF_NULL) {
    // NULL_IF_NULL functions are decomposable, merge the validity bits of the children.

    std::vector<DexPtr> merged_validity;

    for (auto &decomposed : args) {
      // Merge the validity_expressions of the children to build a combined validity
      // expression.
      merged_validity.insert(merged_validity.end(),
                             decomposed->validity_exprs().begin(),
                             decomposed->validity_exprs().end());
    }

    auto value_dex = std::make_shared<NonNullableFuncDex>(desc_, native_function, args);
    return std::make_shared<ValueValidityPair>(merged_validity, value_dex);
  } else if (native_function->result_nullable_type() == RESULT_NULL_NEVER) {
    // These functions always output valid results. So, no validity dex.
    auto value_dex = std::make_shared<NullableNeverFuncDex>(desc_, native_function, args);
    return std::make_shared<ValueValidityPair>(value_dex);
  } else {
    DCHECK(native_function->result_nullable_type() == RESULT_NULL_INTERNAL);

    // Add a local bitmap to track the output validity.
    int local_bitmap_idx = annotator.AddLocalBitMap();
    auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);

    auto value_dex = std::make_shared<NullableInternalFuncDex>(desc_,
                                                               native_function,
                                                               args,
                                                               local_bitmap_idx);
    return std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  }
}

NodePtr FunctionNode::CreateFunction(const std::string &name,
                                     const NodeVector &children,
                                     DataTypePtr retType) {
  DataTypeVector paramTypes;
  for (auto &child : children) {
    paramTypes.push_back(child->return_type());
  }

  auto func_desc = FuncDescriptorPtr(new FuncDescriptor(name, paramTypes, retType));
  return NodePtr(new FunctionNode(func_desc, children, retType));
}

} // namespace gandiva
