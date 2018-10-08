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
#include <gandiva-glib/tree-expression-builder.hpp>

G_BEGIN_DECLS

/**
 * SECTION: tree-expression-builder
 * @title: TreeExpressionBuilder classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaTreeExpressionBuilder is a class for building tree-based expression.
 *
 * Since: 0.12.0
 */

typedef struct GGandivaTreeExpressionBuilderPrivate_ {
  std::shared_ptr<gandiva::TreeExprBuilder> tree_expression_builder;
} GGandivaTreeExpressionBuilderPrivate;

enum {
  PROP_0,
  PROP_TREE_EXPRESSION_BUILDER
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaTreeExpressionBuilder,
                           ggandiva_tree_expression_builder,
                           G_TYPE_OBJECT)

#define GGANDIVA_TREE_EXPRESSION_BUILDER_GET_PRIVATE(obj)                 \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                     \
                               GGANDIVA_TYPE_TREE_EXPRESSION_BUILDER,     \
                               GGandivaTreeExpressionBuilderPrivate))

static void
ggandiva_tree_expression_builder_finalize(GObject *object)
{
  auto priv = GGANDIVA_TREE_EXPRESSION_BUILDER_GET_PRIVATE(object);

  priv->tree_expression_builder = nullptr;

  G_OBJECT_CLASS(ggandiva_tree_expression_builder_parent_class)->finalize(object);
}

static void
ggandiva_tree_expression_builder_set_property(GObject *object,
                                              guint prop_id,
                                              const GValue *value,
                                              GParamSpec *pspec)
{
  auto priv = GGANDIVA_TREE_EXPRESSION_BUILDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TREE_EXPRESSION_BUILDER:
    priv->tree_expression_builder =
      *static_cast<std::shared_ptr<gandiva::TreeExprBuilder> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_tree_expression_builder_init(GGandivaTreeExpressionBuilder *object)
{
}

static void
ggandiva_tree_expression_builder_class_init(GGandivaTreeExpressionBuilderClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_tree_expression_builder_finalize;
  gobject_class->set_property = ggandiva_tree_expression_builder_set_property;

  spec = g_param_spec_pointer("tree_expression_builder",
                              "TreeExpressionBuilder",
                              "The raw std::shared<gandiva::TreeExprBuilder> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_TREE_EXPRESSION_BUILDER, spec);
}

/**
 * ggandiva_tree_expression_builder_new:
 *
 * Returns: A newly created #GGandivaTreeExpressionBuilder.
 *
 * Since: 0.12.0
 */
GGandivaTreeExpressionBuilder *
ggandiva_tree_expression_builder_new(void)
{
  auto gandiva_gandiva = std::make_shared<gandiva::TreeExprBuilder>();
  return ggandiva_tree_expression_builder_new_raw(&gandiva_gandiva);
}

/**
 * ggandiva_tree_expression_builder_make_expression:
 * @tree_expression_builder: A #GGandivaTreeExpressionBuilder.
 * @function: The simple function expression.
 * @input_fields: (element-type GArrowField): The input fields.
 * @output_field: A #GArrowField.
 *
 * Returns: (transfer full): An expression tree with a root node,
 *   and a result field.
 *
 * Since: 0.12.0
 */
GGandivaExpression *
ggandiva_tree_expression_builder_make_expression(GGandivaTreeExpressionBuilder *tree_expression_builder,
                                                 gchar *function,
                                                 GList *input_fields,
                                                 GArrowField *output_field)
{
  auto gandiva_tree_expression_builder =
    ggandiva_tree_expression_builder_get_raw(tree_expression_builder);
  std::vector<std::shared_ptr<arrow::Field>> arrow_input_fields;
  for (GList *node = input_fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_input_field = garrow_field_get_raw(field);
    arrow_input_fields.push_back(arrow_input_field);
  }
  auto arrow_output_field = garrow_field_get_raw(output_field);
  auto gandiva_expression =
    gandiva_tree_expression_builder->MakeExpression(function,
                                                    arrow_input_fields,
                                                    arrow_output_field);
  return ggandiva_expression_new_raw(&gandiva_expression);
}

G_END_DECLS

GGandivaTreeExpressionBuilder *
ggandiva_tree_expression_builder_new_raw(std::shared_ptr<gandiva::TreeExprBuilder> *gandiva_tree_expression_builder)
{
  auto gandiva = g_object_new(GGANDIVA_TYPE_TREE_EXPRESSION_BUILDER,
                              "tree_expression_builder", gandiva_tree_expression_builder,
                              NULL);
  return GGANDIVA_TREE_EXPRESSION_BUILDER(gandiva);
}

std::shared_ptr<gandiva::TreeExprBuilder>
ggandiva_tree_expression_builder_get_raw(GGandivaTreeExpressionBuilder *gandiva)
{
  auto priv = GGANDIVA_TREE_EXPRESSION_BUILDER_GET_PRIVATE(gandiva);
  return priv->tree_expression_builder;
}
