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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/field.hpp>

#include <gandiva-glib/expression.hpp>

G_BEGIN_DECLS

/**
 * SECTION: expression
 * @title: Expression classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaExpression is a class for an expression tree with a root node,
 * and a result field.
 *
 * Since: 0.12.0
 */

typedef struct GGandivaExpressionPrivate_ {
  std::shared_ptr<gandiva::Expression> expression;
} GGandivaExpressionPrivate;

enum {
  PROP_0,
  PROP_EXPRESSION
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaExpression,
                           ggandiva_expression,
                           G_TYPE_OBJECT)

#define GGANDIVA_EXPRESSION_GET_PRIVATE(obj)                 \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                        \
                               GGANDIVA_TYPE_EXPRESSION,     \
                               GGandivaExpressionPrivate))

static void
ggandiva_expression_finalize(GObject *object)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(object);

  priv->expression = nullptr;

  G_OBJECT_CLASS(ggandiva_expression_parent_class)->finalize(object);
}

static void
ggandiva_expression_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_EXPRESSION:
    priv->expression =
      *static_cast<std::shared_ptr<gandiva::Expression> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_expression_init(GGandivaExpression *object)
{
}

static void
ggandiva_expression_class_init(GGandivaExpressionClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_expression_finalize;
  gobject_class->set_property = ggandiva_expression_set_property;

  spec = g_param_spec_pointer("expression",
                              "Expression",
                              "The raw std::shared<gandiva::Expression> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXPRESSION, spec);
}

/**
 * ggandiva_expression_new:
 * @function: The function name in the expression.
 * @input_fields: (element-type GArrowField): The input fields.
 * @output_field: A #GArrowField to be output.
 *
 * Returns: (transfer full): The expression tree with a root node,
 *   and a result field.
 *
 * Since: 0.12.0
 */
GGandivaExpression *
ggandiva_expression_new(const gchar *function,
                        GList *input_fields,
                        GArrowField *output_field)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_input_fields;
  for (GList *node = input_fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_input_field = garrow_field_get_raw(field);
    arrow_input_fields.push_back(arrow_input_field);
  }
  auto arrow_output_field = garrow_field_get_raw(output_field);
  auto gandiva_expression =
    gandiva::TreeExprBuilder::MakeExpression(function,
                                             arrow_input_fields,
                                             arrow_output_field);
  return ggandiva_expression_new_raw(&gandiva_expression);
}

/**
 * ggandiva_expression_to_string:
 * @expression: A #GGandivaExpression.
 *
 * Returns: The string representation of the node in the expression tree.
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.12.0
 */
gchar *
ggandiva_expression_to_string(GGandivaExpression *expression)
{
  auto gandiva_expression = ggandiva_expression_get_raw(expression);
  auto string = gandiva_expression->ToString();
  return g_strndup(string.data(), string.size());
}

G_END_DECLS

GGandivaExpression *
ggandiva_expression_new_raw(std::shared_ptr<gandiva::Expression> *gandiva_expression)
{
  auto gandiva = g_object_new(GGANDIVA_TYPE_EXPRESSION,
                              "expression", gandiva_expression,
                              NULL);
  return GGANDIVA_EXPRESSION(gandiva);
}

std::shared_ptr<gandiva::Expression>
ggandiva_expression_get_raw(GGandivaExpression *gandiva)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(gandiva);
  return priv->expression;
}
