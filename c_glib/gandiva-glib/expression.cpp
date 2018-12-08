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
#include <gandiva-glib/node.hpp>

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
  GGandivaNode *root_node;
  GArrowField *result_field;
} GGandivaExpressionPrivate;

enum {
  PROP_EXPRESSION = 1,
  PROP_ROOT_NODE,
  PROP_RESULT_FIELD
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaExpression,
                           ggandiva_expression,
                           G_TYPE_OBJECT)

#define GGANDIVA_EXPRESSION_GET_PRIVATE(object)                 \
  static_cast<GGandivaExpressionPrivate *>(                     \
    ggandiva_expression_get_instance_private(                   \
      GGANDIVA_EXPRESSION(object)))

static void
ggandiva_expression_dispose(GObject *object)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(object);

  if (priv->root_node) {
    g_object_unref(priv->root_node);
    priv->root_node = nullptr;
  }

  if (priv->result_field) {
    g_object_unref(priv->result_field);
    priv->result_field = nullptr;
  }

  G_OBJECT_CLASS(ggandiva_expression_parent_class)->dispose(object);
}

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
  case PROP_ROOT_NODE:
    priv->root_node = GGANDIVA_NODE(g_value_dup_object(value));
    break;
  case PROP_RESULT_FIELD:
    priv->result_field = GARROW_FIELD(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_expression_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ROOT_NODE:
    g_value_set_object(value, priv->root_node);
    break;
  case PROP_RESULT_FIELD:
    g_value_set_object(value, priv->result_field);
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
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = ggandiva_expression_dispose;
  gobject_class->finalize     = ggandiva_expression_finalize;
  gobject_class->set_property = ggandiva_expression_set_property;
  gobject_class->get_property = ggandiva_expression_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("expression",
                              "Expression",
                              "The raw std::shared<gandiva::Expression> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_EXPRESSION, spec);

  spec = g_param_spec_object("root-node",
                             "Root Node",
                             "The root node for the expression",
                             GGANDIVA_TYPE_NODE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ROOT_NODE, spec);

  spec = g_param_spec_object("result-field",
                             "Result Field",
                             "The name and type of returned value as #GArrowField",
                             GARROW_TYPE_FIELD,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RESULT_FIELD, spec);
}

/**
 * ggandiva_expression_new:
 * @root_node: The root node for the expression.
 * @result_field: The name and type of returned value as #GArrowField.
 *
 * Returns: A newly created #GGandivaExpression.
 *
 * Since: 0.12.0
 */
GGandivaExpression *
ggandiva_expression_new(GGandivaNode *root_node,
                        GArrowField *result_field)
{
  auto gandiva_root_node = ggandiva_node_get_raw(root_node);
  auto arrow_result_field = garrow_field_get_raw(result_field);
  auto gandiva_expression =
    gandiva::TreeExprBuilder::MakeExpression(gandiva_root_node,
                                             arrow_result_field);
  return ggandiva_expression_new_raw(&gandiva_expression,
                                     root_node,
                                     result_field);
}

/**
 * ggandiva_expression_to_string:
 * @expression: A #GGandivaExpression.
 *
 * Returns: (transfer full): The string representation of the node in the expression tree.
 *
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
ggandiva_expression_new_raw(std::shared_ptr<gandiva::Expression> *gandiva_expression,
                            GGandivaNode *root_node,
                            GArrowField *result_field)
{
  auto expression = g_object_new(GGANDIVA_TYPE_EXPRESSION,
                                 "expression", gandiva_expression,
                                 "root-node", root_node,
                                 "result-field", result_field,
                                 NULL);
  return GGANDIVA_EXPRESSION(expression);
}

std::shared_ptr<gandiva::Expression>
ggandiva_expression_get_raw(GGandivaExpression *expression)
{
  auto priv = GGANDIVA_EXPRESSION_GET_PRIVATE(expression);
  return priv->expression;
}
