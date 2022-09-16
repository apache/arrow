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

#include "gandiva/type_inference.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "gandiva/arrow.h"
#include "gandiva/function_registry.h"
#include "gandiva/function_signature.h"
#include "gandiva/literal_holder.h"
#include "gandiva/node.h"

namespace gandiva {

/// \brief FunctionSignature that allows null types
class FunctionSignaturePattern {
 public:
  FunctionSignaturePattern(std::string base_name, DataTypeVector param_types,
                           DataTypePtr ret_type);

  explicit FunctionSignaturePattern(const FunctionSignature& signature);

  DataTypePtr ret_type() const { return ret_type_; }

  const std::string& base_name() const { return base_name_; }

  DataTypeVector param_types() const { return param_types_; }

  std::string ToString() const;

  /// \brief Check if the candidate signature is a possible completion to this
  bool IsCompatibleWith(const FunctionSignature& candidate) const;

 private:
  std::string base_name_;
  DataTypeVector param_types_;
  DataTypePtr ret_type_;
};

FunctionSignaturePattern::FunctionSignaturePattern(std::string base_name,
                                                   DataTypeVector param_types,
                                                   DataTypePtr ret_type)
    : base_name_(base_name), param_types_(param_types), ret_type_(ret_type) {}

FunctionSignaturePattern::FunctionSignaturePattern(const FunctionSignature& signature)
    : base_name_(signature.base_name()),
      param_types_(signature.param_types()),
      ret_type_(signature.ret_type()) {}

std::string FunctionSignaturePattern::ToString() const {
  std::stringstream s;
  s << (ret_type_ == nullptr ? "untyped" : ret_type_->ToString()) << " " << base_name_
    << "(";
  for (uint32_t i = 0; i < param_types_.size(); i++) {
    if (i > 0) {
      s << ", ";
    }

    s << (param_types_[i] == nullptr ? "untyped" : param_types_[i]->ToString());
  }

  s << ")";
  return s.str();
}

bool FunctionSignaturePattern::IsCompatibleWith(
    const FunctionSignature& candidate) const {
  if (ret_type() != nullptr && ret_type() != candidate.ret_type()) {
    return false;
  }
  if (param_types().size() != candidate.param_types().size()) {
    return false;
  }
  auto arity = candidate.param_types().size();
  for (size_t i = 0ul; i < arity; ++i) {
    if (param_types()[i] != nullptr && param_types()[i] != candidate.param_types()[i]) {
      return false;
    }
  }
  return true;
}

/// \brief Extract a common pattern from multiple signatures, nullptr act as wildcard
/// For example, int(double, int) and int(double, double) -> int(double, nullptr)
/// It assumes 1. input is not empty 2. every input has the same arity
FunctionSignaturePattern ExtractPattern(
    const std::vector<const FunctionSignature*>& signatures) {
  auto arity = signatures[0]->param_types().size();

  std::string base_name = signatures[0]->base_name();
  std::vector<DataTypePtr> params = signatures[0]->param_types();
  DataTypePtr return_type = signatures[0]->ret_type();

  for (const auto& signature : signatures) {
    if (return_type != nullptr && signature->ret_type() != return_type) {
      return_type = nullptr;
    }
    for (size_t i = 0ul; i < arity; ++i) {
      if (params[i] != nullptr && signature->param_types()[i] != params[i]) {
        params[i] = nullptr;
      }
    }
  }

  return {base_name, params, return_type};
}

Status TypeInferenceVisitor::Infer(NodePtr input, NodePtr* result) {
  Status status;

  /// First pass, bottom up propagation of types
  all_typed_ = true;
  status = input->Accept(*this);
  if (!status.ok()) {
    return status;
  }

  // std::cout << "first pass: " << result_->ToString() << std::endl;

  if (all_typed_) {
    *result = result_;
    return Status::OK();
  }

  /// Second pass, top down propagation of types, and tags untyped literals with default
  /// types
  tag_default_type_ = true;
  all_typed_ = true;
  status = input->Accept(*this);
  if (!status.ok()) {
    return status;
  }
  // std::cout << "second pass: " << result_->ToString() << std::endl;

  *result = result_;
  return Status::OK();
}

/// \brief Field type is known, do nothing.
Status TypeInferenceVisitor::Visit(const FieldNode& node) {
  result_ = node.GetSharedPtr();
  return Status::OK();
}

/// \brief Try to infer the type
Status TypeInferenceVisitor::Visit(const FunctionNode& node) {
  // std::cout << "func visit " << node.ToString() << std::endl;

  Status status;
  std::vector<NodePtr> children;
  std::vector<DataTypePtr> param_types;
  for (const auto& child : node.children()) {
    status = child->Accept(*this);
    if (!status.ok()) {
      return status;
    }
    children.emplace_back(result_);
    param_types.emplace_back(result_->return_type());
  }
  FunctionSignaturePattern current_pattern(node.descriptor()->name(), param_types,
                                           node.return_type());

  auto candidates = registry_.GetSignaturesByFunctionName(node.descriptor()->name());
  std::vector<const FunctionSignature*> compatible_signatures;
  std::copy_if(candidates.begin(), candidates.end(),
               std::back_inserter(compatible_signatures),
               [&current_pattern](const FunctionSignature* candidate) {
                 return current_pattern.IsCompatibleWith(*candidate);
               });

  if (compatible_signatures.empty()) {
    std::stringstream error_stream;
    error_stream << "No valid signature compatible with pattern "
                 << current_pattern.ToString() << std::endl;
    error_stream << "All available signatures:" << std::endl;
    for (const auto* candidate : candidates) {
      error_stream << candidate->ToString() << std::endl;
    }
    return Status::TypeError(error_stream.str());
  }

  if (compatible_signatures.size() == 1) {
    current_pattern = FunctionSignaturePattern(*compatible_signatures[0]);
  } else {
    all_typed_ = false;
    current_pattern = ExtractPattern(compatible_signatures);
  }

  for (size_t i = 0; i < current_pattern.param_types().size(); ++i) {
    children[i]->set_return_type(current_pattern.param_types()[i]);
  }
  result_ = std::make_shared<FunctionNode>(current_pattern.base_name(), children,
                                           current_pattern.ret_type());

  // std::cout << "func result " << result_->ToString() << std::endl;
  return Status::OK();
}

Status TypeInferenceVisitor::Visit(const IfNode& node) {
  Status status;
  std::array<NodePtr, 3> children{node.condition(), node.then_node(), node.else_node()};
  children[0]->set_return_type(arrow::boolean());
  for (auto& child : children) {
    status = child->Accept(*this);
    if (!status.ok()) {
      return status;
    }
    child = result_;
  }

  std::unordered_set<DataTypePtr> types;
  if (node.return_type() != nullptr) {
    types.insert(node.return_type());
  }
  if (children[1]->return_type() != nullptr) {
    types.insert(children[1]->return_type());
  }
  if (children[2]->return_type() != nullptr) {
    types.insert(children[2]->return_type());
  }

  if (types.size() == 0) {
    all_typed_ = false;
    result_ = std::make_shared<IfNode>(children[0], children[1], children[2], nullptr);
    return Status::OK();
  }

  if (types.size() == 1) {
    const auto& type = *types.begin();
    children[1]->set_return_type(type);
    children[2]->set_return_type(type);
    result_ = std::make_shared<IfNode>(children[0], children[1], children[2], type);
    return Status::OK();
  }

  auto error_node =
      std::make_shared<IfNode>(children[0], children[1], children[2], node.return_type());
  return Status::TypeError(error_node->ToString() + " has conflicting types.");
}

#define MAKE_LITERAL(atype, ctype)                              \
  case arrow::Type::atype:                                      \
    *node = std::make_shared<LiteralNode>(                      \
        type, LiteralHolder(static_cast<ctype>(value)), false); \
    break;

template <typename T>
Status MakeLiteralNode(const DataTypePtr& type, T value, NodePtr* node) {
  switch (type->id()) {
    MAKE_LITERAL(BOOL, bool);
    MAKE_LITERAL(INT8, int8_t);
    MAKE_LITERAL(INT16, int16_t);
    MAKE_LITERAL(INT32, int32_t);
    MAKE_LITERAL(INT64, int64_t);
    MAKE_LITERAL(UINT8, uint8_t);
    MAKE_LITERAL(UINT16, uint16_t);
    MAKE_LITERAL(UINT32, uint32_t);
    MAKE_LITERAL(UINT64, uint64_t);
    MAKE_LITERAL(FLOAT, float);
    MAKE_LITERAL(DOUBLE, double);
    default:
      // should be impossible to reach here
      return Status::TypeError("Impossible mismatched literal type " + type->ToString());
  }
  return Status::OK();
}

Status TypeInferenceVisitor::Visit(const LiteralNode& node) {
  auto return_type = node.return_type();
  if (return_type != nullptr &&
      return_type->id() == kLiteralHolderTypes[node.holder().index()]) {
    result_ = node.GetSharedPtr();
    return Status::OK();
  }

  if (return_type == nullptr) {
    if (tag_default_type_) {
      if (node.holder().index() == 2) {  // double
        return_type = arrow::float32();
      } else if (node.holder().index() == 10) {  // uint64
        return_type = arrow::int32();
      } else {
        // Should be impossible to reach here
        return Status::TypeError("Impossible untyped literal holder type" +
                                 std::to_string(node.holder().index()));
      }
    } else {
      all_typed_ = false;
      result_ = node.GetSharedPtr();
      return Status::OK();
    }
  }

  Status status;
  if (node.holder().index() == 2) {  // double
    status = MakeLiteralNode(return_type, *node.holder().get<double>(), &result_);
    if (!status.ok()) {
      return status;
    }
  } else if (node.holder().index() == 10) {  // uint64
    status = MakeLiteralNode(return_type, *node.holder().get<uint64_t>(), &result_);
    if (!status.ok()) {
      return status;
    }
  } else {
    // Should be impossible to reach here
    return Status::TypeError("Impossible untyped literal holder type" +
                             std::to_string(node.holder().index()));
  }

  return Status::OK();
}

Status TypeInferenceVisitor::Visit(const BooleanNode& node) {
  // std::cout << "bool visit " << node.ToString() << std::endl;
  Status status;
  std::vector<NodePtr> children = node.children();
  for (auto& child : children) {
    child->set_return_type(arrow::boolean());
    status = child->Accept(*this);
    if (!status.ok()) {
      return status;
    }
    child = result_;
  }

  result_ = std::make_shared<BooleanNode>(node.expr_type(), children);
  if (result_->return_type() != arrow::boolean() ||
      std::any_of(children.begin(), children.end(), [](const NodePtr& child) {
        return child->return_type() != arrow::boolean();
      })) {
    all_typed_ = false;
  }

  // std::cout << "bool result " << result_->ToString() << std::endl;

  return Status::OK();
}

Status TypeInferenceVisitor::Visit(const InExpressionNode<int32_t>& node) {
  return Status::OK();
}
Status TypeInferenceVisitor::Visit(const InExpressionNode<int64_t>& node) {
  return Status::OK();
}
Status TypeInferenceVisitor::Visit(const InExpressionNode<float>& node) {
  return Status::OK();
}
Status TypeInferenceVisitor::Visit(const InExpressionNode<double>& node) {
  return Status::OK();
}
Status TypeInferenceVisitor::Visit(
    const InExpressionNode<gandiva::DecimalScalar128>& node) {
  return Status::OK();
}
Status TypeInferenceVisitor::Visit(const InExpressionNode<std::string>& node) {
  return Status::OK();
}

}  // namespace gandiva
