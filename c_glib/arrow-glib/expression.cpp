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

enum {
  PROP_EXPRESSION = 1,
};

struct GArrowExpressionPrivate
{
  arrow::compute::Expression expression;
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowExpression, garrow_expression, G_TYPE_OBJECT)

#define GARROW_EXPRESSION_GET_PRIVATE(object)                                            \
  static_cast<GArrowExpressionPrivate *>(                                                \
    garrow_expression_get_instance_private(GARROW_EXPRESSION(object)))

static void
garrow_expression_finalize(GObject *object)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(object);
  priv->expression.~Expression();
  G_OBJECT_CLASS(garrow_expression_parent_class)->finalize(object);
}

static void
garrow_expression_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXPRESSION:
    priv->expression =
      *static_cast<arrow::compute::Expression *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_expression_init(GArrowExpression *object)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(object);
  new (&priv->expression) arrow::compute::Expression();
}

static void
garrow_expression_class_init(GArrowExpressionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_expression_finalize;
  gobject_class->set_property = garrow_expression_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "expression",
    "Expression",
    "The raw arrow::compute::Expression *",
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXPRESSION, spec);
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
garrow_expression_equal(GArrowExpression *expression, GArrowExpression *other_expression)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  auto other_priv = GARROW_EXPRESSION_GET_PRIVATE(other_expression);
  return priv->expression.Equals(other_priv->expression);
}

enum {
  PROP_DATUM = 1,
};

struct GArrowLiteralExpressionPrivate
{
  GArrowDatum *datum;
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowLiteralExpression,
                           garrow_literal_expression,
                           GARROW_TYPE_EXPRESSION)

#define GARROW_LITERAL_EXPRESSION_GET_PRIVATE(object)                                    \
  static_cast<GArrowLiteralExpressionPrivate *>(                                         \
    garrow_literal_expression_get_instance_private(GARROW_LITERAL_EXPRESSION(object)))

static void
garrow_literal_expression_dispose(GObject *object)
{
  auto priv = GARROW_LITERAL_EXPRESSION_GET_PRIVATE(object);

  if (priv->datum) {
    g_object_unref(priv->datum);
    priv->datum = nullptr;
  }

  G_OBJECT_CLASS(garrow_literal_expression_parent_class)->dispose(object);
}

static void
garrow_literal_expression_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_LITERAL_EXPRESSION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATUM:
    priv->datum = GARROW_DATUM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_literal_expression_get_property(GObject *object,
                                       guint prop_id,
                                       GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_LITERAL_EXPRESSION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATUM:
    g_value_set_object(value, priv->datum);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_literal_expression_init(GArrowLiteralExpression *object)
{
}

static void
garrow_literal_expression_class_init(GArrowLiteralExpressionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_literal_expression_dispose;
  gobject_class->set_property = garrow_literal_expression_set_property;
  gobject_class->get_property = garrow_literal_expression_get_property;

  GParamSpec *spec;

  /**
   * GArrowLiteralExpression:datum:
   *
   * The datum of this literal.
   *
   * Since: 24.0.0
   */
  spec = g_param_spec_object(
    "datum",
    "Datum",
    "The datum of this literal",
    GARROW_TYPE_DATUM,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATUM, spec);
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
  return GARROW_LITERAL_EXPRESSION(garrow_expression_new_raw(arrow_expression,
                                                             "expression",
                                                             &arrow_expression,
                                                             "datum",
                                                             datum,
                                                             nullptr));
}

G_DEFINE_TYPE(GArrowFieldExpression, garrow_field_expression, GARROW_TYPE_EXPRESSION)

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
 * Returns: A newly created #GArrowFieldExpression on success, %NULL on
 *   error.
 *
 * Since: 6.0.0
 */
GArrowFieldExpression *
garrow_field_expression_new(const gchar *reference, GError **error)
{
  auto arrow_reference_result = garrow_field_reference_resolve_raw(reference);
  if (!garrow::check(error, arrow_reference_result, "[field-expression][new]")) {
    return NULL;
  }
  auto arrow_expression = arrow::compute::field_ref(*arrow_reference_result);
  return GARROW_FIELD_EXPRESSION(garrow_expression_new_raw(arrow_expression));
}

struct GArrowCallExpressionPrivate
{
  GList *arguments;
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCallExpression,
                           garrow_call_expression,
                           GARROW_TYPE_EXPRESSION)

#define GARROW_CALL_EXPRESSION_GET_PRIVATE(object)                                       \
  static_cast<GArrowCallExpressionPrivate *>(                                            \
    garrow_call_expression_get_instance_private(GARROW_CALL_EXPRESSION(object)))

static void
garrow_call_expression_dispose(GObject *object)
{
  auto priv = GARROW_CALL_EXPRESSION_GET_PRIVATE(object);

  g_list_free_full(priv->arguments, g_object_unref);
  priv->arguments = nullptr;

  G_OBJECT_CLASS(garrow_call_expression_parent_class)->dispose(object);
}

static void
garrow_call_expression_init(GArrowCallExpression *object)
{
}

static void
garrow_call_expression_class_init(GArrowCallExpressionClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_call_expression_dispose;
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
    arrow_options.reset(garrow_function_options_get_raw(options)->Copy().release());
  }
  auto arrow_expression = arrow::compute::call(function, arrow_arguments, arrow_options);
  auto expression = GARROW_CALL_EXPRESSION(garrow_expression_new_raw(arrow_expression));
  auto priv = GARROW_CALL_EXPRESSION_GET_PRIVATE(expression);
  priv->arguments =
    g_list_copy_deep(arguments, reinterpret_cast<GCopyFunc>(g_object_ref), nullptr);
  return expression;
}

/**
 * garrow_call_expression_get_arguments:
 * @expression: A #GArrowCallExpression.
 *
 * Returns: (transfer none) (element-type GArrowExpression): Arguments
 *   of this expression.
 *
 * Since: 24.0.0
 */
GList *
garrow_call_expression_get_arguments(GArrowCallExpression *expression)
{
  auto priv = GARROW_CALL_EXPRESSION_GET_PRIVATE(expression);
  return priv->arguments;
}

G_END_DECLS

GArrowExpression *
garrow_expression_new_raw(const arrow::compute::Expression &arrow_expression)
{
  return garrow_expression_new_raw(arrow_expression,
                                   "expression",
                                   &arrow_expression,
                                   nullptr);
}

GArrowExpression *
garrow_expression_new_raw(const arrow::compute::Expression &arrow_expression,
                          const gchar *first_property_name,
                          ...)
{
  va_list args;
  va_start(args, first_property_name);
  auto array =
    garrow_expression_new_raw_valist(arrow_expression, first_property_name, args);
  va_end(args);
  return array;
}

GArrowExpression *
garrow_expression_new_raw_valist(const arrow::compute::Expression &arrow_expression,
                                 const gchar *first_property_name,
                                 va_list args)
{
  GType gtype = GARROW_TYPE_EXPRESSION;
  if (arrow_expression.literal()) {
    gtype = GARROW_TYPE_LITERAL_EXPRESSION;
  } else if (arrow_expression.parameter()) {
    gtype = GARROW_TYPE_FIELD_EXPRESSION;
  } else if (arrow_expression.call()) {
    gtype = GARROW_TYPE_CALL_EXPRESSION;
  }
  return GARROW_EXPRESSION(g_object_new_valist(gtype, first_property_name, args));
}

arrow::compute::Expression *
garrow_expression_get_raw(GArrowExpression *expression)
{
  auto priv = GARROW_EXPRESSION_GET_PRIVATE(expression);
  return &(priv->expression);
}
