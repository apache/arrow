/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <arrow-glib/compute.hpp>
#include <arrow-glib/datum.hpp>
#include <arrow-glib/expression.hpp>
#include <arrow-glib/error.hpp>

G_BEGIN_DECLS

/**
 * SECTION: expression
 * @section_id: expression
 * @title: Expression
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowExpression is a base class for all expression classes such
 * as #GArrowLiteralExpression.
 *
 * #GArrowLiteralExpression is a class for literal value.
 *
 * #GArrowFieldExpression is a class for field reference.
 *
 * #GArrowCallExpression is a class for function call.
 *
 * Since: 6.0.0
 */

typedef struct GArrowExpressionPrivate_ {
  arrow::compute::Expression expression;
} GArrowExpressionPrivate;

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowExpression,
                                    garrow_expression,
                                    G_TYPE_OBJECT)

#define GARROW_EXPRESSION_GET_PRIVATE(object)  \
  static_cast<GArrowExpressionPrivate *>(      \
    garrow_expression_get_instance_private(    \
      GARROW_EXPRESSION(object)))

static void
garrow_expression_finalize(GObject *object)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(object);
  priv->expression.~Expression();
  G_OBJECT_CLASS(garrow_expression_parent_class)->finalize(object);
}

static void
garrow_expression_init(GArrowExpression *object)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(object);
  new(&priv->expression) arrow::compute::Expression();
}

static void
garrow_expression_class_init(GArrowExpressionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_expression_finalize;
}

/**
 * garrow_expression_to_string:
 * @expression: A #GArrowExpression.
 *
 * Returns: The formatted expression.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gchar *
garrow_expression_to_string(GArrowExpression *expression)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  auto string = priv->expression.ToString();
  return g_strndup(string.data(), string.size());
}

/**
 * garrow_expression_equal:
 * @expression: A #GArrowExpression.
 * @other_expression: A #GArrowExpression.
 *
 * Returns: %TRUE if both of them have the same content, %FALSE
 *   otherwise.
 *
 * Since: 6.0.0
 */
gboolean
garrow_expression_equal(GArrowExpression *expression,
                        GArrowExpression *other_expression)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  auto other_priv = GARROW_EXPRESSION_GET_PRIVATE(other_expression);
  return priv->expression.Equals(other_priv->expression);
}


G_DEFINE_TYPE(GArrowLiteralExpression,
              garrow_literal_expression,
              GARROW_TYPE_EXPRESSION)

static void
garrow_literal_expression_init(GArrowLiteralExpression *object)
{
}

static void
garrow_literal_expression_class_init(GArrowLiteralExpressionClass *klass)
{
}

/**
 * garrow_literal_expression_new:
 * @datum: A #GArrowDatum.
 *
 * Returns: A newly created #GArrowLiteralExpression.
 *
 * Since: 6.0.0
 */
GArrowLiteralExpression *
garrow_literal_expression_new(GArrowDatum *datum)
{
  auto arrow_datum = garrow_datum_get_raw(datum);
  auto arrow_expression = arrow::compute::literal(arrow_datum);
  return GARROW_LITERAL_EXPRESSION(garrow_expression_new_raw(arrow_expression));
}


G_DEFINE_TYPE(GArrowFieldExpression,
              garrow_field_expression,
              GARROW_TYPE_EXPRESSION)

static void
garrow_field_expression_init(GArrowFieldExpression *object)
{
}

static void
garrow_field_expression_class_init(GArrowFieldExpressionClass *klass)
{
}

/**
 * garrow_field_expression_new:
 * @reference: A field name or dot path.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GArrowFieldExpression on sucess, %NULL on
 *   error.
 *
 * Since: 6.0.0
 */
GArrowFieldExpression *
garrow_field_expression_new(const gchar *reference,
                            GError **error)
{
  if (reference && reference[0] == '.') {
    auto arrow_reference_result = arrow::FieldRef::FromDotPath(reference);
    if (!garrow::check(error,
                       arrow_reference_result,
                       "[field-expression][new]")) {
      return NULL;
    }
    auto arrow_expression = arrow::compute::field_ref(*arrow_reference_result);
    return GARROW_FIELD_EXPRESSION(garrow_expression_new_raw(arrow_expression));
  } else {
    arrow::FieldRef arrow_reference(reference);
    auto arrow_expression = arrow::compute::field_ref(arrow_reference);
    return GARROW_FIELD_EXPRESSION(garrow_expression_new_raw(arrow_expression));
  }
}


G_DEFINE_TYPE(GArrowCallExpression,
              garrow_call_expression,
              GARROW_TYPE_EXPRESSION)

static void
garrow_call_expression_init(GArrowCallExpression *object)
{
}

static void
garrow_call_expression_class_init(GArrowCallExpressionClass *klass)
{
}

/**
 * garrow_call_expression_new:
 * @function: A name of function to be called.
 * @arguments: (element-type GArrowExpression): Arguments of this call.
 * @options: (nullable): A #GArrowFunctionOptions for the called function.
 *
 * Returns: A newly created #GArrowCallExpression.
 *
 * Since: 6.0.0
 */
GArrowCallExpression *
garrow_call_expression_new(const gchar *function,
                           GList *arguments,
                           GArrowFunctionOptions *options)
{
  std::vector<arrow::compute::Expression> arrow_arguments;
  for (GList *node = arguments; node; node = node->next) {
    auto argument = GARROW_EXPRESSION(node->data);
    auto arrow_argument = garrow_expression_get_raw(argument);
    arrow_arguments.push_back(*arrow_argument);
  }
  std::shared_ptr<arrow::compute::FunctionOptions> arrow_options;
  if (options) {
    arrow_options.reset(garrow_function_options_get_raw(options));
  }
  auto arrow_expression = arrow::compute::call(function,
                                               arrow_arguments,
                                               arrow_options);
  return GARROW_CALL_EXPRESSION(garrow_expression_new_raw(arrow_expression));
}


G_END_DECLS

GArrowExpression *
garrow_expression_new_raw(const arrow::compute::Expression &arrow_expression)
{
  GType gtype = GARROW_TYPE_EXPRESSION;
  if (arrow_expression.literal()) {
    gtype = GARROW_TYPE_LITERAL_EXPRESSION;
  } else if (arrow_expression.parameter()) {
    gtype = GARROW_TYPE_FIELD_EXPRESSION;
  } else if (arrow_expression.call()) {
    gtype = GARROW_TYPE_CALL_EXPRESSION;
  }
  auto expression = GARROW_EXPRESSION(g_object_new(gtype, NULL));
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  priv->expression = arrow_expression;
  return expression;
}

arrow::compute::Expression *
garrow_expression_get_raw(GArrowExpression *expression)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  return &(priv->expression);
}
