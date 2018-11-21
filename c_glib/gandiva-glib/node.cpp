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

#include <arrow-glib/data-type.hpp>
#include <arrow-glib/field.hpp>

#include <gandiva-glib/node.hpp>

G_BEGIN_DECLS

/**
 * SECTION: node
 * @section_id: node-classes
 * @title: Node classes
 * @include: gandiva-glib/gandiva-glib.h
 *
 * #GGandivaNode is a base class for a node in the expression tree.
 *
 * #GGandivaFieldNode is a class for a node in the expression tree, representing an Arrow field.
 *
 * #GGandivaFunctionNode is a class for a node in the expression tree, representing a function.
 *
 * Since: 0.12.0
 */

typedef struct GGandivaNodePrivate_ {
  std::shared_ptr<gandiva::Node> node;
} GGandivaNodePrivate;

enum {
  PROP_0,
  PROP_NODE
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaNode,
                           ggandiva_node,
                           G_TYPE_OBJECT)

#define GGANDIVA_NODE_GET_PRIVATE(object)                       \
  static_cast<GGandivaNodePrivate *>(                           \
    ggandiva_node_get_instance_private(                         \
      GGANDIVA_NODE(object)))

static void
ggandiva_node_finalize(GObject *object)
{
  auto priv = GGANDIVA_NODE_GET_PRIVATE(object);

  priv->node = nullptr;

  G_OBJECT_CLASS(ggandiva_node_parent_class)->finalize(object);
}

static void
ggandiva_node_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GGANDIVA_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NODE:
    priv->node =
      *static_cast<std::shared_ptr<gandiva::Node> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_node_init(GGandivaNode *object)
{
}

static void
ggandiva_node_class_init(GGandivaNodeClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_node_finalize;
  gobject_class->set_property = ggandiva_node_set_property;

  spec = g_param_spec_pointer("node",
                              "Node",
                              "The raw std::shared<gandiva::Node> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NODE, spec);
}


G_DEFINE_TYPE(GGandivaFieldNode,
              ggandiva_field_node,
              GGANDIVA_TYPE_NODE)

static void
ggandiva_field_node_init(GGandivaFieldNode *field_node)
{
}

static void
ggandiva_field_node_class_init(GGandivaFieldNodeClass *klass)
{
}

/**
 * ggandiva_field_node_new:
 * @field: A #GArrowField.
 *
 * Returns: (transfer full): The node in the expression tree,
 *   representing an Arrow Field.
 *
 * Since: 0.12.0
 */
GGandivaFieldNode *
ggandiva_field_node_new(GArrowField *field)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto gandiva_node =
    gandiva::TreeExprBuilder::MakeField(arrow_field);
  return ggandiva_field_node_new_raw(&gandiva_node);
}


G_DEFINE_TYPE(GGandivaFunctionNode,
              ggandiva_function_node,
              GGANDIVA_TYPE_NODE)

static void
ggandiva_function_node_init(GGandivaFunctionNode *function_node)
{
}

static void
ggandiva_function_node_class_init(GGandivaFunctionNodeClass *klass)
{
}

/**
 * ggandiva_function_node_new:
 * @function: The function name in the node.
 * @nodes: (element-type GGandivaNode): The child nodes for the function node.
 * @data_type: A #GArrowDataType.
 *
 * Returns: (transfer full): The node in the expression tree,
 *   representing a function.
 *
 * Since: 0.12.0
 */
GGandivaFunctionNode *
ggandiva_function_node_new(const gchar *function,
                           GList *nodes,
                           GArrowDataType *data_type)
{
  std::vector<std::shared_ptr<gandiva::Node>> gandiva_nodes;
  for (auto node = nodes; node; node = g_list_next(node)) {
    auto gandiva_node = ggandiva_node_get_raw(GGANDIVA_NODE(node->data));
    gandiva_nodes.push_back(gandiva_node);
  }
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto gandiva_node =
    gandiva::TreeExprBuilder::MakeFunction(function,
                                           gandiva_nodes,
                                           arrow_data_type);
  return ggandiva_function_node_new_raw(&gandiva_node);
}

G_END_DECLS

std::shared_ptr<gandiva::Node>
ggandiva_node_get_raw(GGandivaNode *gandiva)
{
  auto priv = GGANDIVA_NODE_GET_PRIVATE(gandiva);
  return priv->node;
}

GGandivaFieldNode *
ggandiva_field_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node)
{
  auto gandiva = g_object_new(GGANDIVA_TYPE_FIELD_NODE,
                              "node", gandiva_node,
                              NULL);
  return GGANDIVA_FIELD_NODE(gandiva);
}

GGandivaFunctionNode *
ggandiva_function_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node)
{
  auto gandiva = g_object_new(GGANDIVA_TYPE_FUNCTION_NODE,
                              "node", gandiva_node,
                              NULL);
  return GGANDIVA_FUNCTION_NODE(gandiva);
}
