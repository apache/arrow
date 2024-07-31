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

#include "gandiva/expr_decomposer.h"

#include <memory>
#include <stack>
#include <string>
#include <unordered_set>
#include <vector>

#include "gandiva/annotator.h"
#include "gandiva/dex.h"
#include "gandiva/function_holder_maker_registry.h"
#include "gandiva/function_registry.h"
#include "gandiva/function_signature.h"
#include "gandiva/in_holder.h"
#include "gandiva/node.h"
#include "gandiva/regex_functions_holder.h"

namespace gandiva {

// Decompose a field node - simply separate out validity & value arrays.
Status ExprDecomposer::Visit(const FieldNode& node) {
  auto desc = annotator_.CheckAndAddInputFieldDescriptor(node.field());

  DexPtr validity_dex = std::make_shared<VectorReadValidityDex>(desc);
  DexPtr value_dex;
  if (desc->HasOffsetsIdx()) {
    value_dex = std::make_shared<VectorReadVarLenValueDex>(desc);
  } else {
    value_dex = std::make_shared<VectorReadFixedLenValueDex>(desc);
  }
  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  return Status::OK();
}

// Try and optimize a function node, by substituting with cheaper alternatives.
// eg. replacing 'like' with 'starts_with' can save function calls at evaluation
// time.
const FunctionNode ExprDecomposer::TryOptimize(const FunctionNode& node) {
  if (node.descriptor()->name() == "like") {
    return LikeHolder::TryOptimize(node);
  } else {
    return node;
  }
}

// Decompose a field node - wherever possible, merge the validity vectors of the
// child nodes.
Status ExprDecomposer::Visit(const FunctionNode& in_node) {
  auto node = TryOptimize(in_node);
  auto desc = node.descriptor();
  FunctionSignature signature(desc->name(), desc->params(), desc->return_type());
  const NativeFunction* native_function = registry_.LookupSignature(signature);
  DCHECK(native_function) << "Missing Signature " << signature.ToString();

  // decompose the children.
  std::vector<ValueValidityPairPtr> args;
  for (auto& child : node.children()) {
    auto status = child->Accept(*this);
    ARROW_RETURN_NOT_OK(status);

    args.push_back(result());
  }

  // Make a function holder, if required.
  std::shared_ptr<FunctionHolder> holder;
  int holder_idx = -1;
  if (native_function->NeedsFunctionHolder()) {
    auto function_holder_maker_registry = registry_.GetFunctionHolderMakerRegistry();
    ARROW_ASSIGN_OR_RAISE(holder,
                          function_holder_maker_registry.Make(desc->name(), node));
    holder_idx = annotator_.AddHolderPointer(holder.get());
  }

  if (native_function->result_nullable_type() == kResultNullIfNull) {
    // These functions are decomposable, merge the validity bits of the children.

    std::vector<DexPtr> merged_validity;
    for (auto& decomposed : args) {
      // Merge the validity_expressions of the children to build a combined validity
      // expression.
      merged_validity.insert(merged_validity.end(), decomposed->validity_exprs().begin(),
                             decomposed->validity_exprs().end());
    }

    auto value_dex = std::make_shared<NonNullableFuncDex>(desc, native_function, holder,
                                                          holder_idx, args);
    result_ = std::make_shared<ValueValidityPair>(merged_validity, value_dex);
  } else if (native_function->result_nullable_type() == kResultNullNever) {
    // These functions always output valid results. So, no validity dex.
    auto value_dex = std::make_shared<NullableNeverFuncDex>(desc, native_function, holder,
                                                            holder_idx, args);
    result_ = std::make_shared<ValueValidityPair>(value_dex);
  } else {
    DCHECK(native_function->result_nullable_type() == kResultNullInternal);

    // Add a local bitmap to track the output validity.
    int local_bitmap_idx = annotator_.AddLocalBitMap();
    auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);

    auto value_dex = std::make_shared<NullableInternalFuncDex>(
        desc, native_function, holder, holder_idx, args, local_bitmap_idx);
    result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  }
  return Status::OK();
}

// Decompose an IfNode
Status ExprDecomposer::Visit(const IfNode& node) {
  // nested_if_else_ might get overwritten when visiting the condition-node, so
  // saving the value to a local variable and resetting nested_if_else_ to false
  bool svd_nested_if_else = nested_if_else_;
  nested_if_else_ = false;

  PushConditionEntry(node);
  auto status = node.condition()->Accept(*this);
  ARROW_RETURN_NOT_OK(status);
  auto condition_vv = result();
  PopConditionEntry(node);

  // Add a local bitmap to track the output validity.
  int local_bitmap_idx = PushThenEntry(node, svd_nested_if_else);
  status = node.then_node()->Accept(*this);
  ARROW_RETURN_NOT_OK(status);
  auto then_vv = result();
  PopThenEntry(node);

  PushElseEntry(node, local_bitmap_idx);
  nested_if_else_ = (dynamic_cast<IfNode*>(node.else_node().get()) != nullptr);

  status = node.else_node()->Accept(*this);
  ARROW_RETURN_NOT_OK(status);
  auto else_vv = result();
  bool is_terminal_else = PopElseEntry(node);

  auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);
  auto value_dex =
      std::make_shared<IfDex>(condition_vv, then_vv, else_vv, node.return_type(),
                              local_bitmap_idx, is_terminal_else);

  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  return Status::OK();
}

// Decompose a BooleanNode
Status ExprDecomposer::Visit(const BooleanNode& node) {
  // decompose the children.
  std::vector<ValueValidityPairPtr> args;
  for (auto& child : node.children()) {
    auto status = child->Accept(*this);
    ARROW_RETURN_NOT_OK(status);

    args.push_back(result());
  }

  // Add a local bitmap to track the output validity.
  int local_bitmap_idx = annotator_.AddLocalBitMap();
  auto validity_dex = std::make_shared<LocalBitMapValidityDex>(local_bitmap_idx);

  std::shared_ptr<BooleanDex> value_dex;
  switch (node.expr_type()) {
    case BooleanNode::AND:
      value_dex = std::make_shared<BooleanAndDex>(args, local_bitmap_idx);
      break;
    case BooleanNode::OR:
      value_dex = std::make_shared<BooleanOrDex>(args, local_bitmap_idx);
      break;
  }
  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  return Status::OK();
}

Status ExprDecomposer::Visit(const InExpressionNode<gandiva::DecimalScalar128>& node) {
  /* decompose the children. */
  std::vector<ValueValidityPairPtr> args;
  auto status = node.eval_expr()->Accept(*this);
  ARROW_RETURN_NOT_OK(status);
  args.push_back(result());
  /* In always outputs valid results, so no validity dex */
  auto value_dex = std::make_shared<InExprDex<gandiva::DecimalScalar128>>(
      args, node.values(), node.get_precision(), node.get_scale());
  int holder_idx = annotator_.AddHolderPointer(value_dex->in_holder().get());
  value_dex->set_holder_idx(holder_idx);
  result_ = std::make_shared<ValueValidityPair>(value_dex);
  return Status::OK();
}

template <typename ctype>
Status ExprDecomposer::VisitInGeneric(const InExpressionNode<ctype>& node) {
  /* decompose the children. */
  std::vector<ValueValidityPairPtr> args;
  auto status = node.eval_expr()->Accept(*this);
  ARROW_RETURN_NOT_OK(status);
  args.push_back(result());
  /* In always outputs valid results, so no validity dex */
  auto value_dex = std::make_shared<InExprDex<ctype>>(args, node.values());
  int holder_idx = annotator_.AddHolderPointer(value_dex->in_holder().get());
  value_dex->set_holder_idx(holder_idx);
  result_ = std::make_shared<ValueValidityPair>(value_dex);
  return Status::OK();
}

Status ExprDecomposer::Visit(const InExpressionNode<int32_t>& node) {
  return VisitInGeneric<int32_t>(node);
}

Status ExprDecomposer::Visit(const InExpressionNode<int64_t>& node) {
  return VisitInGeneric<int64_t>(node);
}

Status ExprDecomposer::Visit(const InExpressionNode<float>& node) {
  return VisitInGeneric<float>(node);
}

Status ExprDecomposer::Visit(const InExpressionNode<double>& node) {
  return VisitInGeneric<double>(node);
}

Status ExprDecomposer::Visit(const InExpressionNode<std::string>& node) {
  return VisitInGeneric<std::string>(node);
}

Status ExprDecomposer::Visit(const LiteralNode& node) {
  auto value_dex = std::make_shared<LiteralDex>(node.return_type(), node.holder());
  DexPtr validity_dex;
  if (node.is_null()) {
    validity_dex = std::make_shared<FalseDex>();
  } else {
    validity_dex = std::make_shared<TrueDex>();
  }
  result_ = std::make_shared<ValueValidityPair>(validity_dex, value_dex);
  return Status::OK();
}

// The below functions use a stack to detect :
// a. nested if-else expressions.
//    In such cases,  the local bitmap can be re-used.
// b. detect terminal else expressions
//    The non-terminal else expressions do not need to track validity (the if statement
//    that has a match will do it).
// Both of the above optimisations save CPU cycles during expression evaluation.

int ExprDecomposer::PushThenEntry(const IfNode& node, bool reuse_bitmap) {
  int local_bitmap_idx;

  if (reuse_bitmap) {
    // we also need stack in addition to reuse_bitmap flag since we
    // can also enter other if-else nodes when we visit the condition-node
    // (which themselves might be nested) before we visit then-node
    DCHECK_EQ(if_entries_stack_.empty(), false) << "PushThenEntry: stack is empty";
    DCHECK_EQ(if_entries_stack_.top()->entry_type_, kStackEntryElse)
        << "PushThenEntry: top of stack is not of type entry_else";
    auto top = if_entries_stack_.top().get();

    // inside a nested else statement (i.e if-else-if). use the parent's bitmap.
    local_bitmap_idx = top->local_bitmap_idx_;

    // clear the is_terminal bit in the current top entry (else).
    top->is_terminal_else_ = false;
  } else {
    // alloc a new bitmap.
    local_bitmap_idx = annotator_.AddLocalBitMap();
  }

  // push new entry to the stack.
  std::unique_ptr<IfStackEntry> entry(new IfStackEntry(
      node, kStackEntryThen, false /*is_terminal_else*/, local_bitmap_idx));
  if_entries_stack_.emplace(std::move(entry));
  return local_bitmap_idx;
}

void ExprDecomposer::PopThenEntry(const IfNode& node) {
  DCHECK_EQ(if_entries_stack_.empty(), false) << "PopThenEntry: found empty stack";

  auto top = if_entries_stack_.top().get();
  DCHECK_EQ(top->entry_type_, kStackEntryThen)
      << "PopThenEntry: found " << top->entry_type_ << " expected then";
  DCHECK_EQ(&top->if_node_, &node) << "PopThenEntry: found mismatched node";

  if_entries_stack_.pop();
}

void ExprDecomposer::PushElseEntry(const IfNode& node, int local_bitmap_idx) {
  std::unique_ptr<IfStackEntry> entry(new IfStackEntry(
      node, kStackEntryElse, true /*is_terminal_else*/, local_bitmap_idx));
  if_entries_stack_.emplace(std::move(entry));
}

bool ExprDecomposer::PopElseEntry(const IfNode& node) {
  DCHECK_EQ(if_entries_stack_.empty(), false) << "PopElseEntry: found empty stack";

  auto top = if_entries_stack_.top().get();
  DCHECK_EQ(top->entry_type_, kStackEntryElse)
      << "PopElseEntry: found " << top->entry_type_ << " expected else";
  DCHECK_EQ(&top->if_node_, &node) << "PopElseEntry: found mismatched node";
  bool is_terminal_else = top->is_terminal_else_;

  if_entries_stack_.pop();
  return is_terminal_else;
}

void ExprDecomposer::PushConditionEntry(const IfNode& node) {
  std::unique_ptr<IfStackEntry> entry(new IfStackEntry(node, kStackEntryCondition));
  if_entries_stack_.emplace(std::move(entry));
}

void ExprDecomposer::PopConditionEntry(const IfNode& node) {
  DCHECK_EQ(if_entries_stack_.empty(), false) << "PopConditionEntry: found empty stack";

  auto top = if_entries_stack_.top().get();
  DCHECK_EQ(top->entry_type_, kStackEntryCondition)
      << "PopConditionEntry: found " << top->entry_type_ << " expected condition";
  DCHECK_EQ(&top->if_node_, &node) << "PopConditionEntry: found mismatched node";
  if_entries_stack_.pop();
}

}  // namespace gandiva
