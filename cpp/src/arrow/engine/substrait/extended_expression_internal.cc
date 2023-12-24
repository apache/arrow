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

// This API is EXPERIMENTAL.

#include "arrow/engine/substrait/extended_expression_internal.h"

#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "arrow/engine/substrait/type_internal.h"
#include "arrow/engine/substrait/util.h"
#include "arrow/engine/substrait/util_internal.h"
#include "arrow/status.h"
#include "arrow/util/iterator.h"
#include "arrow/util/string.h"

namespace arrow {
namespace engine {

namespace {
Result<ExtensionSet> GetExtensionSetFromExtendedExpression(
    const substrait::ExtendedExpression& expr,
    const ConversionOptions& conversion_options, const ExtensionIdRegistry* registry) {
  return GetExtensionSetFromMessage(expr, conversion_options, registry);
}

Status AddExtensionSetToExtendedExpression(const ExtensionSet& ext_set,
                                           substrait::ExtendedExpression* expr) {
  return AddExtensionSetToMessage(ext_set, expr);
}

Status VisitNestedFields(const DataType& type,
                         std::function<Status(const Field&)> visitor) {
  if (!is_nested(type.id())) {
    return Status::OK();
  }
  for (const auto& field : type.fields()) {
    ARROW_RETURN_NOT_OK(VisitNestedFields(*field->type(), visitor));
    ARROW_RETURN_NOT_OK(visitor(*field));
  }
  return Status::OK();
}

Result<NamedExpression> ExpressionFromProto(
    const substrait::ExpressionReference& expression, const Schema& input_schema,
    const ExtensionSet& ext_set, const ConversionOptions& conversion_options,
    const ExtensionIdRegistry* registry) {
  NamedExpression named_expr;
  switch (expression.expr_type_case()) {
    case substrait::ExpressionReference::ExprTypeCase::kExpression: {
      ARROW_ASSIGN_OR_RAISE(
          named_expr.expression,
          FromProto(expression.expression(), ext_set, conversion_options));
      break;
    }
    case substrait::ExpressionReference::ExprTypeCase::kMeasure: {
      return Status::NotImplemented("ExtendedExpression containing aggregate functions");
    }
    default: {
      return Status::Invalid(
          "Unrecognized substrait::ExpressionReference::ExprTypeCase: ",
          expression.expr_type_case());
    }
  }

  ARROW_ASSIGN_OR_RAISE(named_expr.expression, named_expr.expression.Bind(input_schema));
  const DataType& output_type = *named_expr.expression.type();

  // An expression reference has the entire DFS tree of field names for the output type
  // which is usually redundant.  Then it has one extra name for the name of the
  // expression which is not redundant.
  //
  // For example, if the base schema is [struct<foo:i32>, i32] and the expression is
  // field(0) the extended expression output names might be ["foo", "my_expression"].
  // The "foo" is redundant but we can verify it matches and reject if it does not.
  //
  // The one exception is struct literals which have no field names.  For example, if
  // the base schema is [i32, i64] and the expression is {7, 3}_struct<i8,i8> then the
  // output type is struct<?:i8, ?:i8> and we do not know the names of the output type.
  //
  // TODO(weston) we could patch the names back in at this point using the output
  // names field but this is rather complex and it might be easier to give names to
  // struct literals in Substrait.
  int output_name_idx = 0;
  ARROW_RETURN_NOT_OK(VisitNestedFields(output_type, [&](const Field& field) {
    if (output_name_idx >= expression.output_names_size()) {
      return Status::Invalid("Ambiguous plan.  Expression had ",
                             expression.output_names_size(),
                             " output names but the field in base_schema had type ",
                             output_type.ToString(), " which needs more output names");
    }
    if (!field.name().empty() &&
        field.name() != expression.output_names(output_name_idx)) {
      return Status::Invalid("Ambiguous plan.  Expression had output type ",
                             output_type.ToString(),
                             " which contains a nested field named ", field.name(),
                             " but the output_names in the Substrait message contained ",
                             expression.output_names(output_name_idx));
    }
    output_name_idx++;
    return Status::OK();
  }));
  // The last name is the actual field name that we can't verify but there should only
  // be one extra name.
  if (output_name_idx < expression.output_names_size() - 1) {
    return Status::Invalid("Ambiguous plan.  Expression had ",
                           expression.output_names_size(),
                           " output names but the field in base_schema had type ",
                           output_type.ToString(), " which doesn't have enough fields");
  }
  if (expression.output_names_size() == 0) {
    // This is potentially invalid substrait but we can handle it
    named_expr.name = "";
  } else {
    named_expr.name = expression.output_names(expression.output_names_size() - 1);
  }
  return named_expr;
}

Result<std::unique_ptr<substrait::ExpressionReference>> CreateExpressionReference(
    const std::string& name, const Expression& expr, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto expr_ref = std::make_unique<substrait::ExpressionReference>();
  ARROW_RETURN_NOT_OK(VisitNestedFields(*expr.type(), [&](const Field& field) {
    expr_ref->add_output_names(field.name());
    return Status::OK();
  }));
  expr_ref->add_output_names(name);
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::Expression> expression,
                        ToProto(expr, ext_set, conversion_options));
  expr_ref->set_allocated_expression(expression.release());
  return std::move(expr_ref);
}

}  // namespace

Result<BoundExpressions> FromProto(const substrait::ExtendedExpression& expression,
                                   ExtensionSet* ext_set_out,
                                   const ConversionOptions& conversion_options,
                                   const ExtensionIdRegistry* registry) {
  BoundExpressions bound_expressions;
  ARROW_RETURN_NOT_OK(CheckVersion(expression.version().major_number(),
                                   expression.version().minor_number()));
  if (expression.has_advanced_extensions()) {
    return Status::NotImplemented("Advanced extensions in ExtendedExpression");
  }
  ARROW_ASSIGN_OR_RAISE(
      ExtensionSet ext_set,
      GetExtensionSetFromExtendedExpression(expression, conversion_options, registry));

  ARROW_ASSIGN_OR_RAISE(bound_expressions.schema,
                        FromProto(expression.base_schema(), ext_set, conversion_options));

  bound_expressions.named_expressions.reserve(expression.referred_expr_size());

  for (const auto& referred_expr : expression.referred_expr()) {
    ARROW_ASSIGN_OR_RAISE(NamedExpression named_expr,
                          ExpressionFromProto(referred_expr, *bound_expressions.schema,
                                              ext_set, conversion_options, registry));
    bound_expressions.named_expressions.push_back(std::move(named_expr));
  }

  if (ext_set_out) {
    *ext_set_out = std::move(ext_set);
  }

  return std::move(bound_expressions);
}

Result<std::unique_ptr<substrait::ExtendedExpression>> ToProto(
    const BoundExpressions& bound_expressions, ExtensionSet* ext_set,
    const ConversionOptions& conversion_options) {
  auto expression = std::make_unique<substrait::ExtendedExpression>();
  expression->set_allocated_version(CreateVersion().release());
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::NamedStruct> base_schema,
                        ToProto(*bound_expressions.schema, ext_set, conversion_options));
  expression->set_allocated_base_schema(base_schema.release());
  for (const auto& named_expression : bound_expressions.named_expressions) {
    Expression bound_expr = named_expression.expression;
    if (!bound_expr.IsBound()) {
      // This will use the default function registry.  Most of the time that will be fine.
      // In the cases where this is not what the user wants then the user should make sure
      // to pass in bound expressions.
      ARROW_ASSIGN_OR_RAISE(bound_expr, bound_expr.Bind(*bound_expressions.schema));
    }
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<substrait::ExpressionReference> expr_ref,
                          CreateExpressionReference(named_expression.name, bound_expr,
                                                    ext_set, conversion_options));
    expression->mutable_referred_expr()->AddAllocated(expr_ref.release());
  }
  RETURN_NOT_OK(AddExtensionSetToExtendedExpression(*ext_set, expression.get()));
  return std::move(expression);
}

}  // namespace engine
}  // namespace arrow
