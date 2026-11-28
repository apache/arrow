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

#include "gandiva/expression_cache_key.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <type_traits>
#include <variant>
#include <vector>

#include "arrow/util/hash_util.h"
#include "gandiva/node.h"

namespace gandiva {

namespace {

// Encodes the expression tree for the cache key. `ToString()` is ambiguous, because
// literals and field names can contain expression syntax. This encoding has one possible
// parse: a node starts with a one byte tag, variable size tokens carry their length and
// child lists carry their count. Keys are process-local, so values are raw bytes.
//
// A cache hit skips validation, so the key must tell apart everything that validation
// could tell apart. That is why a node's return type is written, except for nodes that
// fix it themselves (field, boolean and IN nodes). Null nodes and types are encoded, not
// dereferenced, because the key is built before validation.
class ExpressionKeyVisitor : public NodeVisitor {
 public:
  std::string MakeKey(const Expression* expression, const char* kind) {
    key_.clear();
    has_like_ = false;

    AppendBytes(&key_, kind);
    if (!expression) {
      key_ += kNullExpression;
      return key_;
    }
    key_ += kExpression;
    if (expression->result()) {
      key_ += kResult;
      AppendType(expression->result()->type());
    } else {
      key_ += kNullResult;
    }
    AppendNode(expression->root());
    return key_;
  }

  bool has_like() const { return has_like_; }

  Status Visit(const FieldNode& node) override {
    key_ += kFieldNode;
    AppendField(node.field());
    return Status::OK();
  }

  Status Visit(const FunctionNode& node) override {
    key_ += kFunctionNode;
    AppendType(node.return_type());
    AppendBytes(&key_, node.descriptor()->name());
    has_like_ |= node.descriptor()->name() == "like";
    AppendNodes(node.children());
    return Status::OK();
  }

  Status Visit(const IfNode& node) override {
    key_ += kIfNode;
    AppendType(node.return_type());
    AppendNode(node.condition());
    AppendNode(node.then_node());
    AppendNode(node.else_node());
    return Status::OK();
  }

  Status Visit(const LiteralNode& node) override {
    key_ += kLiteralNode;
    AppendType(node.return_type());
    key_ += node.is_null() ? kNullLiteral : kValueLiteral;
    // The variant index fixes the value size, so only strings carry a length.
    key_ += static_cast<char>(node.holder().index());
    std::visit([this](const auto& value) { AppendValue(&key_, value); }, node.holder());
    return Status::OK();
  }

  Status Visit(const BooleanNode& node) override {
    key_ += kBooleanNode;
    key_ += static_cast<char>(node.expr_type());
    AppendNodes(node.children());
    return Status::OK();
  }

  Status Visit(const InExpressionNode<int32_t>& node) override {
    return AppendIn(node, 'a');
  }
  Status Visit(const InExpressionNode<int64_t>& node) override {
    return AppendIn(node, 'b');
  }
  Status Visit(const InExpressionNode<float>& node) override {
    return AppendIn(node, 'c');
  }
  Status Visit(const InExpressionNode<double>& node) override {
    return AppendIn(node, 'd');
  }
  Status Visit(const InExpressionNode<std::string>& node) override {
    return AppendIn(node, 's');
  }

  Status Visit(const InExpressionNode<DecimalScalar128>& node) override {
    key_ += kInNode;
    key_ += 'D';
    AppendVarint(&key_, static_cast<uint64_t>(node.get_precision()));
    AppendVarint(&key_, static_cast<uint64_t>(node.get_scale()));
    return AppendInValues(node);
  }

 private:
  static constexpr char kNullExpression = 'X';
  static constexpr char kExpression = 'E';
  static constexpr char kResult = 'R';
  static constexpr char kNullResult = 'r';
  static constexpr char kNullNode = 'z';
  static constexpr char kFieldNode = 'f';
  static constexpr char kFunctionNode = 'c';
  static constexpr char kIfNode = 'i';
  static constexpr char kLiteralNode = 'l';
  static constexpr char kBooleanNode = 'b';
  static constexpr char kInNode = 'I';
  static constexpr char kNullLiteral = 'n';
  static constexpr char kValueLiteral = 'v';
  static constexpr char kNullType = 'N';
  static constexpr char kType = 'T';
  static constexpr char kNullField = 'Z';
  static constexpr char kField = 'F';
  static constexpr char kNullable = '1';
  static constexpr char kNonNullable = '0';

  static void AppendVarint(std::string* out, uint64_t value) {
    while (value >= 0x80) {
      out->push_back(static_cast<char>(value | 0x80));
      value >>= 7;
    }
    out->push_back(static_cast<char>(value));
  }

  static void AppendBytes(std::string* out, const std::string& value) {
    AppendVarint(out, value.size());
    out->append(value);
  }

  template <typename T>
  static void AppendValue(std::string* out, const T& value) {
    static_assert(std::is_arithmetic_v<T>);
    // Raw bytes keep NaN payloads and signed zero.
    out->append(reinterpret_cast<const char*>(&value), sizeof(value));
  }

  static void AppendValue(std::string* out, const std::string& value) {
    AppendBytes(out, value);
  }

  static void AppendValue(std::string* out, const DecimalScalar128& value) {
    AppendValue(out, value.value().high_bits());
    AppendValue(out, value.value().low_bits());
    AppendValue(out, value.precision());
    AppendValue(out, value.scale());
  }

  void AppendType(const DataTypePtr& type) {
    if (!type) {
      key_ += kNullType;
      return;
    }
    key_ += kType;
    // Types are compared by content, not by pointer. The fingerprint holds the type
    // parameters (precision, scale, time unit, time zone). A type without one also
    // writes its `ToString()`. A non-empty fingerprint has nothing after it, so the
    // key still has one parse.
    AppendVarint(&key_, static_cast<uint64_t>(type->id()));
    const std::string& fingerprint = type->fingerprint();
    AppendBytes(&key_, fingerprint);
    if (fingerprint.empty()) {
      AppendBytes(&key_, type->ToString());
    }
  }

  void AppendField(const FieldPtr& field) {
    if (!field) {
      key_ += kNullField;
      return;
    }
    key_ += kField;
    AppendBytes(&key_, field->name());
    AppendType(field->type());
    key_ += field->nullable() ? kNullable : kNonNullable;
  }

  void AppendNode(const NodePtr& node) {
    if (!node) {
      key_ += kNullNode;
      return;
    }
    // Every visit here returns OK. Validation is a separate pass.
    ARROW_UNUSED(node->Accept(*this));
  }

  void AppendNodes(const NodeVector& nodes) {
    AppendVarint(&key_, nodes.size());
    for (const auto& node : nodes) {
      AppendNode(node);
    }
  }

  template <typename T>
  Status AppendIn(const InExpressionNode<T>& node, char kind) {
    key_ += kInNode;
    key_ += kind;
    AppendType(node.type());
    return AppendInValues(node);
  }

  template <typename T>
  Status AppendInValues(const InExpressionNode<T>& node) {
    AppendNode(node.eval_expr());
    // The value set is unordered, so write the values in byte order.
    std::vector<std::string> values;
    values.reserve(node.values().size());
    for (const auto& value : node.values()) {
      values.emplace_back();
      AppendValue(&values.back(), value);
    }
    std::sort(values.begin(), values.end());
    AppendVarint(&key_, values.size());
    for (const auto& value : values) {
      key_ += value;
    }
    return Status::OK();
  }

  std::string key_;
  bool has_like_ = false;
};

}  // namespace

ExpressionCacheKey::ExpressionCacheKey(SchemaPtr schema,
                                       std::shared_ptr<Configuration> configuration,
                                       ExpressionVector expression_vector,
                                       SelectionVector::Mode mode)
    : schema_(schema), mode_(mode), uniquifier_(0), configuration_(configuration) {
  static const int kSeedValue = 4;
  size_t result = kSeedValue;
  for (auto& expr : expression_vector) {
    ExpressionKeyVisitor visitor;
    std::string expr_as_string = visitor.MakeKey(expr.get(), "projector");
    expressions_as_strings_.push_back(expr_as_string);
    arrow::internal::hash_combine(result, expr_as_string);
    UpdateUniquifier(visitor.has_like());
  }
  arrow::internal::hash_combine(result, static_cast<size_t>(mode));
  arrow::internal::hash_combine(result, configuration->Hash());
  arrow::internal::hash_combine(result, schema_->ToString());
  arrow::internal::hash_combine(result, uniquifier_);
  hash_code_ = result;
}

ExpressionCacheKey::ExpressionCacheKey(SchemaPtr schema,
                                       std::shared_ptr<Configuration> configuration,
                                       Expression& expression)
    : schema_(schema),
      mode_(SelectionVector::MODE_NONE),
      uniquifier_(0),
      configuration_(configuration) {
  static const int kSeedValue = 4;
  size_t result = kSeedValue;
  ExpressionKeyVisitor visitor;
  expressions_as_strings_.push_back(visitor.MakeKey(&expression, "filter"));
  UpdateUniquifier(visitor.has_like());

  arrow::internal::hash_combine(result, expressions_as_strings_.back());
  arrow::internal::hash_combine(result, configuration->Hash());
  arrow::internal::hash_combine(result, schema_->ToString());
  arrow::internal::hash_combine(result, uniquifier_);
  hash_code_ = result;
}

void ExpressionCacheKey::UpdateUniquifier(bool has_like) {
  if (uniquifier_ == 0) {
    // caching of expressions with re2 patterns causes lock contention. So, use
    // multiple instances to reduce contention.
    if (has_like) {
      uniquifier_ = std::hash<std::thread::id>()(std::this_thread::get_id()) % 16;
    }
  }
}

}  // namespace gandiva
