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
  PROP_NODE = 1
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


typedef struct GGandivaFieldNodePrivate_ {
  GArrowField *field;
} GGandivaFieldNodePrivate;

enum {
  PROP_FIELD = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFieldNode,
                           ggandiva_field_node,
                           GGANDIVA_TYPE_NODE)

#define GGANDIVA_FIELD_NODE_GET_PRIVATE(object)                 \
  static_cast<GGandivaFieldNodePrivate *>(                      \
    ggandiva_field_node_get_instance_private(                   \
      GGANDIVA_FIELD_NODE(object)))

static void
ggandiva_field_node_dispose(GObject *object)
{
  auto priv = GGANDIVA_FIELD_NODE_GET_PRIVATE(object);

  if (priv->field) {
    g_object_unref(priv->field);
    priv->field = nullptr;
  }

  G_OBJECT_CLASS(ggandiva_field_node_parent_class)->dispose(object);
}

static void
ggandiva_field_node_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GGANDIVA_FIELD_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FIELD:
    priv->field = GARROW_FIELD(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_field_node_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GGANDIVA_FIELD_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FIELD:
    g_value_set_object(value, priv->field);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_field_node_init(GGandivaFieldNode *field_node)
{
}

static void
ggandiva_field_node_class_init(GGandivaFieldNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = ggandiva_field_node_dispose;
  gobject_class->set_property = ggandiva_field_node_set_property;
  gobject_class->get_property = ggandiva_field_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("field",
                             "Field",
                             "The field",
                             GARROW_TYPE_FIELD,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FIELD, spec);
}

/**
 * ggandiva_field_node_new:
 * @field: A #GArrowField.
 *
 * Returns: A newly created #GGandivaFieldNode for the given field.
 *
 * Since: 0.12.0
 */
GGandivaFieldNode *
ggandiva_field_node_new(GArrowField *field)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto gandiva_node = gandiva::TreeExprBuilder::MakeField(arrow_field);
  return ggandiva_field_node_new_raw(&gandiva_node, field);
}


typedef struct GGandivaFunctionNodePrivate_ {
  gchar *name;
  GList *parameters;
  GArrowDataType *return_type;
} GGandivaFunctionNodePrivate;

enum {
  PROP_NAME = 1,
  PROP_RETURN_TYPE
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFunctionNode,
                           ggandiva_function_node,
                           GGANDIVA_TYPE_NODE)

#define GGANDIVA_FUNCTION_NODE_GET_PRIVATE(object)      \
  static_cast<GGandivaFunctionNodePrivate *>(           \
    ggandiva_function_node_get_instance_private(        \
      GGANDIVA_FUNCTION_NODE(object)))                  \

static void
ggandiva_function_node_dispose(GObject *object)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(object);

  if (priv->parameters) {
    for (auto node = priv->parameters; node; node = g_list_next(node)) {
      auto parameter = GGANDIVA_NODE(node->data);
      g_object_unref(parameter);
    }
    g_list_free(priv->parameters);
    priv->parameters = nullptr;
  }

  if (priv->return_type) {
    g_object_unref(priv->return_type);
    priv->return_type = nullptr;
  }

  G_OBJECT_CLASS(ggandiva_function_node_parent_class)->dispose(object);
}

static void
ggandiva_function_node_finalize(GObject *object)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(object);

  g_free(priv->name);

  G_OBJECT_CLASS(ggandiva_function_node_parent_class)->finalize(object);
}

static void
ggandiva_function_node_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NAME:
    priv->name = g_value_dup_string(value);
    break;
  case PROP_RETURN_TYPE:
    priv->return_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_function_node_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_NAME:
    g_value_set_string(value, priv->name);
    break;
  case PROP_RETURN_TYPE:
    g_value_set_object(value, priv->return_type);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_function_node_init(GGandivaFunctionNode *function_node)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(function_node);
  priv->parameters = nullptr;
}

static void
ggandiva_function_node_class_init(GGandivaFunctionNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = ggandiva_function_node_dispose;
  gobject_class->finalize     = ggandiva_function_node_finalize;
  gobject_class->set_property = ggandiva_function_node_set_property;
  gobject_class->get_property = ggandiva_function_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_string("name",
                             "Name",
                             "The name of the function",
                             nullptr,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NAME, spec);

  spec = g_param_spec_object("return-type",
                             "Return type",
                             "The return type of the function",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RETURN_TYPE, spec);
}

/**
 * ggandiva_function_node_new:
 * @name: The name of the function to be called.
 * @parameters: (element-type GGandivaNode): The parameters of the function call.
 * @return_type: The return type of the function call.
 *
 * Returns: A newly created #GGandivaFunctionNode for the function call.
 *
 * Since: 0.12.0
 */
GGandivaFunctionNode *
ggandiva_function_node_new(const gchar *name,
                           GList *parameters,
                           GArrowDataType *return_type)
{
  std::vector<std::shared_ptr<gandiva::Node>> gandiva_nodes;
  for (auto node = parameters; node; node = g_list_next(node)) {
    auto gandiva_node = ggandiva_node_get_raw(GGANDIVA_NODE(node->data));
    gandiva_nodes.push_back(gandiva_node);
  }
  auto arrow_return_type = garrow_data_type_get_raw(return_type);
  auto gandiva_node = gandiva::TreeExprBuilder::MakeFunction(name,
                                                             gandiva_nodes,
                                                             arrow_return_type);
  return ggandiva_function_node_new_raw(&gandiva_node,
                                        name,
                                        parameters,
                                        return_type);
}

/**
 * ggandiva_function_node_get_parameters:
 * @node: A #GGandivaFunctionNode.
 *
 * Returns: (transfer none) (element-type GGandivaNode):
 *   The parameters of the function node.
 *
 * Since: 0.12.0
 */
GList *
ggandiva_function_node_get_parameters(GGandivaFunctionNode *node)
{
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(node);
  return priv->parameters;
}

G_END_DECLS

std::shared_ptr<gandiva::Node>
ggandiva_node_get_raw(GGandivaNode *node)
{
  auto priv = GGANDIVA_NODE_GET_PRIVATE(node);
  return priv->node;
}

GGandivaFieldNode *
ggandiva_field_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                            GArrowField *field)
{
  auto field_node = g_object_new(GGANDIVA_TYPE_FIELD_NODE,
                                 "node", gandiva_node,
                                 "field", field,
                                 NULL);
  return GGANDIVA_FIELD_NODE(field_node);
}

GGandivaFunctionNode *
ggandiva_function_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                               const gchar *name,
                               GList *parameters,
                               GArrowDataType *return_type)
{
  auto function_node = g_object_new(GGANDIVA_TYPE_FUNCTION_NODE,
                                    "node", gandiva_node,
                                    "name", name,
                                    "return-type", return_type,
                                    NULL);
  auto priv = GGANDIVA_FUNCTION_NODE_GET_PRIVATE(function_node);
  for (auto node = parameters; node; node = g_list_next(node)) {
    auto parameter = GGANDIVA_NODE(node->data);
    priv->parameters = g_list_prepend(priv->parameters, g_object_ref(parameter));
  }
  priv->parameters = g_list_reverse(priv->parameters);
  return GGANDIVA_FUNCTION_NODE(function_node);
}
