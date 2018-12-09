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
 * #GGandivaLiteralNode is a base class for a node in the expression tree,
 * representing a literal.
 *
 * #GGandivaUint8LiteralNode is a class for a node in the expression tree,
 * representing a 8-bit unsigned integer literal.
 *
 * #GGandivaUint16LiteralNode is a class for a node in the expression tree,
 * representing a 16-bit unsigned integer literal.
 *
 * #GGandivaUint32LiteralNode is a class for a node in the expression tree,
 * representing a 32-bit unsigned integer literal.
 *
 * #GGandivaUint64LiteralNode is a class for a node in the expression tree,
 * representing a 64-bit unsigned integer literal.
 *
 * #GGandivaInt8LiteralNode is a class for a node in the expression tree,
 * representing a 8-bit integer literal.
 *
 * #GGandivaInt16LiteralNode is a class for a node in the expression tree,
 * representing a 16-bit integer literal.
 *
 * #GGandivaInt32LiteralNode is a class for a node in the expression tree,
 * representing a 32-bit integer literal.
 *
 * #GGandivaInt64LiteralNode is a class for a node in the expression tree,
 * representing a 64-bit integer literal.
 *
 * #GGandivaFloatLiteralNode is a class for a node in the expression tree,
 * representing a 32-bit floating point literal.
 *
 * #GGandivaDoubleLiteralNode is a class for a node in the expression tree,
 * representing a 64-bit floating point literal.
 *
 * #GGandivaStringLiteralNode is a class for a node in the expression tree,
 * representing an UTF-8 encoded string literal.
 *
 * #GGandivaBinaryLiteralNode is a class for a node in the expression tree,
 * representing a binary literal.
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


G_DEFINE_TYPE(GGandivaLiteralNode,
              ggandiva_literal_node,
              GGANDIVA_TYPE_NODE)

static void
ggandiva_literal_node_init(GGandivaLiteralNode *literal_node)
{
}

static void
ggandiva_literal_node_class_init(GGandivaLiteralNodeClass *klass)
{
}


typedef struct GGandivaBooleanLiteralNodePrivate_ {
  gboolean value;
} GGandivaBooleanLiteralNodePrivate;

enum {
  PROP_IS_TRUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaBooleanLiteralNode,
                           ggandiva_boolean_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_BOOLEAN_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaBooleanLiteralNodePrivate *>(                     \
    ggandiva_boolean_literal_node_get_instance_private(                 \
      GGANDIVA_BOOLEAN_LITERAL_NODE(object)))

static void
ggandiva_boolean_literal_node_set_property(GObject *object,
                                           guint prop_id,
                                           const GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GGANDIVA_BOOLEAN_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_IS_TRUE:
    priv->value = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_boolean_literal_node_get_property(GObject *object,
                                           guint prop_id,
                                           GValue *value,
                                           GParamSpec *pspec)
{
  auto priv = GGANDIVA_BOOLEAN_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_IS_TRUE:
    g_value_set_boolean(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_boolean_literal_node_init(GGandivaBooleanLiteralNode *boolean_literal_node)
{
}

static void
ggandiva_boolean_literal_node_class_init(GGandivaBooleanLiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_boolean_literal_node_set_property;
  gobject_class->get_property = ggandiva_boolean_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_boolean("value",
                              "Value",
                              "The value of the boolean literal",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_IS_TRUE, spec);
}

/**
 * ggandiva_boolean_literal_node_new:
 * @value: The value of the boolean literal.
 *
 * Returns: A newly created #GGandivaBooleanLiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaBooleanLiteralNode *
ggandiva_boolean_literal_node_new(gboolean value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_boolean_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaUint8LiteralNodePrivate_ {
  guint8 value;
} GGandivaUint8LiteralNodePrivate;

enum {
  PROP_UINT8_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaUint8LiteralNode,
                           ggandiva_uint8_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_UINT8_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaUint8LiteralNodePrivate *>(                     \
    ggandiva_uint8_literal_node_get_instance_private(                 \
      GGANDIVA_UINT8_LITERAL_NODE(object)))

static void
ggandiva_uint8_literal_node_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT8_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT8_VALUE:
    priv->value = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint8_literal_node_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT8_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT8_VALUE:
    g_value_set_uint(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint8_literal_node_init(GGandivaUint8LiteralNode *uint8_literal_node)
{
}

static void
ggandiva_uint8_literal_node_class_init(GGandivaUint8LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_uint8_literal_node_set_property;
  gobject_class->get_property = ggandiva_uint8_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_uint("value",
                           "Value",
                           "The value of the uint8 literal",
                           0,
                           G_MAXUINT,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                    G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_UINT8_VALUE, spec);
}

/**
 * ggandiva_uint8_literal_node_new:
 * @value: The value of the uint8 literal.
 *
 * Returns: A newly created #GGandivaUint8LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaUint8LiteralNode *
ggandiva_uint8_literal_node_new(guint8 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_uint8_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaUint16LiteralNodePrivate_ {
  guint16 value;
} GGandivaUint16LiteralNodePrivate;

enum {
  PROP_UINT16_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaUint16LiteralNode,
                           ggandiva_uint16_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_UINT16_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaUint16LiteralNodePrivate *>(                     \
    ggandiva_uint16_literal_node_get_instance_private(                 \
      GGANDIVA_UINT16_LITERAL_NODE(object)))

static void
ggandiva_uint16_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT16_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT16_VALUE:
    priv->value = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint16_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT16_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT16_VALUE:
    g_value_set_uint(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint16_literal_node_init(GGandivaUint16LiteralNode *uint16_literal_node)
{
}

static void
ggandiva_uint16_literal_node_class_init(GGandivaUint16LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_uint16_literal_node_set_property;
  gobject_class->get_property = ggandiva_uint16_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_uint("value",
                           "Value",
                           "The value of the uint16 literal",
                           0,
                           G_MAXUINT16,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                    G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_UINT16_VALUE, spec);
}

/**
 * ggandiva_uint16_literal_node_new:
 * @value: The value of the uint16 literal.
 *
 * Returns: A newly created #GGandivaUint16LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaUint16LiteralNode *
ggandiva_uint16_literal_node_new(guint16 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_uint16_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaUint32LiteralNodePrivate_ {
  guint32 value;
} GGandivaUint32LiteralNodePrivate;

enum {
  PROP_UINT32_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaUint32LiteralNode,
                           ggandiva_uint32_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_UINT32_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaUint32LiteralNodePrivate *>(                     \
    ggandiva_uint32_literal_node_get_instance_private(                 \
      GGANDIVA_UINT32_LITERAL_NODE(object)))

static void
ggandiva_uint32_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT32_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT32_VALUE:
    priv->value = g_value_get_uint(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint32_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT32_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT32_VALUE:
    g_value_set_uint(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint32_literal_node_init(GGandivaUint32LiteralNode *uint32_literal_node)
{
}

static void
ggandiva_uint32_literal_node_class_init(GGandivaUint32LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_uint32_literal_node_set_property;
  gobject_class->get_property = ggandiva_uint32_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_uint("value",
                           "Value",
                           "The value of the uint32 literal",
                           0,
                           G_MAXUINT32,
                           0,
                           static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                    G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_UINT32_VALUE, spec);
}

/**
 * ggandiva_uint32_literal_node_new:
 * @value: The value of the uint32 literal.
 *
 * Returns: A newly created #GGandivaUint32LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaUint32LiteralNode *
ggandiva_uint32_literal_node_new(guint32 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_uint32_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaUint64LiteralNodePrivate_ {
  guint64 value;
} GGandivaUint64LiteralNodePrivate;

enum {
  PROP_UINT64_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaUint64LiteralNode,
                           ggandiva_uint64_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_UINT64_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaUint64LiteralNodePrivate *>(                     \
    ggandiva_uint64_literal_node_get_instance_private(                 \
      GGANDIVA_UINT64_LITERAL_NODE(object)))

static void
ggandiva_uint64_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT64_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT64_VALUE:
    priv->value = g_value_get_uint64(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint64_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_UINT64_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_UINT64_VALUE:
    g_value_set_uint64(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_uint64_literal_node_init(GGandivaUint64LiteralNode *uint64_literal_node)
{
}

static void
ggandiva_uint64_literal_node_class_init(GGandivaUint64LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_uint64_literal_node_set_property;
  gobject_class->get_property = ggandiva_uint64_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_uint64("value",
                             "Value",
                             "The value of the uint64 literal",
                             0,
                             G_MAXUINT64,
                             0,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_UINT64_VALUE, spec);
}

/**
 * ggandiva_uint64_literal_node_new:
 * @value: The value of the uint64 literal.
 *
 * Returns: A newly created #GGandivaUint64LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaUint64LiteralNode *
ggandiva_uint64_literal_node_new(guint64 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_uint64_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaInt8LiteralNodePrivate_ {
  gint8 value;
} GGandivaInt8LiteralNodePrivate;

enum {
  PROP_INT8_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaInt8LiteralNode,
                           ggandiva_int8_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_INT8_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaInt8LiteralNodePrivate *>(                     \
    ggandiva_int8_literal_node_get_instance_private(                 \
      GGANDIVA_INT8_LITERAL_NODE(object)))

static void
ggandiva_int8_literal_node_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT8_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT8_VALUE:
    priv->value = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int8_literal_node_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT8_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT8_VALUE:
    g_value_set_int(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int8_literal_node_init(GGandivaInt8LiteralNode *int8_literal_node)
{
}

static void
ggandiva_int8_literal_node_class_init(GGandivaInt8LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_int8_literal_node_set_property;
  gobject_class->get_property = ggandiva_int8_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int("value",
                          "Value",
                          "The value of the int8 literal",
                          G_MININT,
                          G_MAXINT,
                          0,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INT8_VALUE, spec);
}

/**
 * ggandiva_int8_literal_node_new:
 * @value: The value of the int8 literal.
 *
 * Returns: A newly created #GGandivaInt8LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaInt8LiteralNode *
ggandiva_int8_literal_node_new(gint8 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_int8_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaInt16LiteralNodePrivate_ {
  gint16 value;
} GGandivaInt16LiteralNodePrivate;

enum {
  PROP_INT16_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaInt16LiteralNode,
                           ggandiva_int16_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_INT16_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaInt16LiteralNodePrivate *>(                     \
    ggandiva_int16_literal_node_get_instance_private(                 \
      GGANDIVA_INT16_LITERAL_NODE(object)))

static void
ggandiva_int16_literal_node_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT16_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT16_VALUE:
    priv->value = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int16_literal_node_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT16_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT16_VALUE:
    g_value_set_int(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int16_literal_node_init(GGandivaInt16LiteralNode *int16_literal_node)
{
}

static void
ggandiva_int16_literal_node_class_init(GGandivaInt16LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_int16_literal_node_set_property;
  gobject_class->get_property = ggandiva_int16_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int("value",
                          "Value",
                          "The value of the int16 literal",
                          G_MININT16,
                          G_MAXINT16,
                          0,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INT16_VALUE, spec);
}

/**
 * ggandiva_int16_literal_node_new:
 * @value: The value of the int16 literal.
 *
 * Returns: A newly created #GGandivaInt16LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaInt16LiteralNode *
ggandiva_int16_literal_node_new(gint16 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_int16_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaInt32LiteralNodePrivate_ {
  gint32 value;
} GGandivaInt32LiteralNodePrivate;

enum {
  PROP_INT32_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaInt32LiteralNode,
                           ggandiva_int32_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_INT32_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaInt32LiteralNodePrivate *>(                     \
    ggandiva_int32_literal_node_get_instance_private(                 \
      GGANDIVA_INT32_LITERAL_NODE(object)))

static void
ggandiva_int32_literal_node_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT32_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT32_VALUE:
    priv->value = g_value_get_int(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int32_literal_node_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT32_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT32_VALUE:
    g_value_set_int(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int32_literal_node_init(GGandivaInt32LiteralNode *int32_literal_node)
{
}

static void
ggandiva_int32_literal_node_class_init(GGandivaInt32LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_int32_literal_node_set_property;
  gobject_class->get_property = ggandiva_int32_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int("value",
                          "Value",
                          "The value of the int32 literal",
                          G_MININT32,
                          G_MAXINT32,
                          0,
                          static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                   G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INT32_VALUE, spec);
}

/**
 * ggandiva_int32_literal_node_new:
 * @value: The value of the int32 literal.
 *
 * Returns: A newly created #GGandivaInt32LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaInt32LiteralNode *
ggandiva_int32_literal_node_new(gint32 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_int32_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaInt64LiteralNodePrivate_ {
  gint64 value;
} GGandivaInt64LiteralNodePrivate;

enum {
  PROP_INT64_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaInt64LiteralNode,
                           ggandiva_int64_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_INT64_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaInt64LiteralNodePrivate *>(                     \
    ggandiva_int64_literal_node_get_instance_private(                 \
      GGANDIVA_INT64_LITERAL_NODE(object)))

static void
ggandiva_int64_literal_node_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT64_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT64_VALUE:
    priv->value = g_value_get_int64(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int64_literal_node_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_INT64_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INT64_VALUE:
    g_value_set_int64(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_int64_literal_node_init(GGandivaInt64LiteralNode *int64_literal_node)
{
}

static void
ggandiva_int64_literal_node_class_init(GGandivaInt64LiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_int64_literal_node_set_property;
  gobject_class->get_property = ggandiva_int64_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_int64("value",
                            "Value",
                            "The value of the int64 literal",
                            G_MININT64,
                            G_MAXINT64,
                            0,
                            static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                     G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INT64_VALUE, spec);
}

/**
 * ggandiva_int64_literal_node_new:
 * @value: The value of the int64 literal.
 *
 * Returns: A newly created #GGandivaInt64LiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaInt64LiteralNode *
ggandiva_int64_literal_node_new(gint64 value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_int64_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaFloatLiteralNodePrivate_ {
  gfloat value;
} GGandivaFloatLiteralNodePrivate;

enum {
  PROP_FLOAT_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaFloatLiteralNode,
                           ggandiva_float_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_FLOAT_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaFloatLiteralNodePrivate *>(                     \
    ggandiva_float_literal_node_get_instance_private(                 \
      GGANDIVA_FLOAT_LITERAL_NODE(object)))

static void
ggandiva_float_literal_node_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_FLOAT_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FLOAT_VALUE:
    priv->value = g_value_get_float(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_float_literal_node_get_property(GObject *object,
                                         guint prop_id,
                                         GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GGANDIVA_FLOAT_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FLOAT_VALUE:
    g_value_set_float(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_float_literal_node_init(GGandivaFloatLiteralNode *float_literal_node)
{
}

static void
ggandiva_float_literal_node_class_init(GGandivaFloatLiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_float_literal_node_set_property;
  gobject_class->get_property = ggandiva_float_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_float("value",
                            "Value",
                            "The value of the float literal",
                            -G_MAXFLOAT,
                            G_MAXFLOAT,
                            0.0,
                            static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                     G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FLOAT_VALUE, spec);
}

/**
 * ggandiva_float_literal_node_new:
 * @value: The value of the float literal.
 *
 * Returns: A newly created #GGandivaFloatLiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaFloatLiteralNode *
ggandiva_float_literal_node_new(gfloat value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_float_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaDoubleLiteralNodePrivate_ {
  gdouble value;
} GGandivaDoubleLiteralNodePrivate;

enum {
  PROP_DOUBLE_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaDoubleLiteralNode,
                           ggandiva_double_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_DOUBLE_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaDoubleLiteralNodePrivate *>(                     \
    ggandiva_double_literal_node_get_instance_private(                 \
      GGANDIVA_DOUBLE_LITERAL_NODE(object)))

static void
ggandiva_double_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_DOUBLE_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DOUBLE_VALUE:
    priv->value = g_value_get_double(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_double_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_DOUBLE_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DOUBLE_VALUE:
    g_value_set_double(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_double_literal_node_init(GGandivaDoubleLiteralNode *double_literal_node)
{
}

static void
ggandiva_double_literal_node_class_init(GGandivaDoubleLiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = ggandiva_double_literal_node_set_property;
  gobject_class->get_property = ggandiva_double_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_double("value",
                             "Value",
                             "The value of the double literal",
                             -G_MAXDOUBLE,
                             G_MAXDOUBLE,
                             0.0,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DOUBLE_VALUE, spec);
}

/**
 * ggandiva_double_literal_node_new:
 * @value: The value of the double literal.
 *
 * Returns: A newly created #GGandivaDoubleLiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaDoubleLiteralNode *
ggandiva_double_literal_node_new(gdouble value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeLiteral(value);
  return ggandiva_double_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaStringLiteralNodePrivate_ {
  gchar *value;
} GGandivaStringLiteralNodePrivate;

enum {
  PROP_STRING_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaStringLiteralNode,
                           ggandiva_string_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_STRING_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaStringLiteralNodePrivate *>(                     \
    ggandiva_string_literal_node_get_instance_private(                 \
      GGANDIVA_STRING_LITERAL_NODE(object)))

static void
ggandiva_string_literal_node_finalize(GObject *object)
{
  auto priv = GGANDIVA_STRING_LITERAL_NODE_GET_PRIVATE(object);

  g_free(priv->value);

  G_OBJECT_CLASS(ggandiva_string_literal_node_parent_class)->finalize(object);
}

static void
ggandiva_string_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_STRING_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STRING_VALUE:
    priv->value = g_value_dup_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_string_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_STRING_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STRING_VALUE:
    g_value_set_string(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_string_literal_node_init(GGandivaStringLiteralNode *string_literal_node)
{
}

static void
ggandiva_string_literal_node_class_init(GGandivaStringLiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_string_literal_node_finalize;
  gobject_class->set_property = ggandiva_string_literal_node_set_property;
  gobject_class->get_property = ggandiva_string_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_string("value",
                             "Value",
                             "The value of the string literal",
                             nullptr,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STRING_VALUE, spec);
}

/**
 * ggandiva_string_literal_node_new:
 * @value: The value of the string literal.
 *
 * Returns: A newly created #GGandivaStringLiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaStringLiteralNode *
ggandiva_string_literal_node_new(const gchar *value)
{
  auto gandiva_node = gandiva::TreeExprBuilder::MakeStringLiteral(value);
  return ggandiva_string_literal_node_new_raw(&gandiva_node, value);
}


typedef struct GGandivaBinaryLiteralNodePrivate_ {
  gchar *value;
} GGandivaBinaryLiteralNodePrivate;

enum {
  PROP_BINARY_VALUE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GGandivaBinaryLiteralNode,
                           ggandiva_binary_literal_node,
                           GGANDIVA_TYPE_LITERAL_NODE)

#define GGANDIVA_BINARY_LITERAL_NODE_GET_PRIVATE(object)               \
  static_cast<GGandivaBinaryLiteralNodePrivate *>(                     \
    ggandiva_binary_literal_node_get_instance_private(                 \
      GGANDIVA_BINARY_LITERAL_NODE(object)))

static void
ggandiva_binary_literal_node_finalize(GObject *object)
{
  auto priv = GGANDIVA_BINARY_LITERAL_NODE_GET_PRIVATE(object);

  g_free(priv->value);

  G_OBJECT_CLASS(ggandiva_binary_literal_node_parent_class)->finalize(object);
}

static void
ggandiva_binary_literal_node_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_BINARY_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BINARY_VALUE:
    priv->value = g_value_dup_string(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_binary_literal_node_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GGANDIVA_BINARY_LITERAL_NODE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_BINARY_VALUE:
    g_value_set_string(value, priv->value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
ggandiva_binary_literal_node_init(GGandivaBinaryLiteralNode *binary_literal_node)
{
}

static void
ggandiva_binary_literal_node_class_init(GGandivaBinaryLiteralNodeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = ggandiva_binary_literal_node_finalize;
  gobject_class->set_property = ggandiva_binary_literal_node_set_property;
  gobject_class->get_property = ggandiva_binary_literal_node_get_property;

  GParamSpec *spec;
  spec = g_param_spec_string("value",
                             "Value",
                             "The value of the binary literal",
                             nullptr,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BINARY_VALUE, spec);
}

/**
 * ggandiva_binary_literal_node_new:
 * @value: (array length=size): The value of the binary literal.
 * @size: The number of bytes of the value.
 *
 * Returns: A newly created #GGandivaBinaryLiteralNode.
 *
 * Since: 0.12.0
 */
GGandivaBinaryLiteralNode *
ggandiva_binary_literal_node_new(const guint8 *value,
                                 gsize size)
{
  auto gandiva_node =
    gandiva::TreeExprBuilder::MakeBinaryLiteral(std::string(reinterpret_cast<const char*>(value),
                                                            static_cast<size_t>(size)));
  return ggandiva_binary_literal_node_new_raw(&gandiva_node);
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

GGandivaBooleanLiteralNode *
ggandiva_boolean_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                      gboolean value)
{
  auto boolean_literal_node = g_object_new(GGANDIVA_TYPE_BOOLEAN_LITERAL_NODE,
                                           "node", gandiva_node,
                                           "value", value,
                                           NULL);
  return GGANDIVA_BOOLEAN_LITERAL_NODE(boolean_literal_node);
}

GGandivaUint8LiteralNode *
ggandiva_uint8_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                    guint8 value)
{
  auto uint8_literal_node = g_object_new(GGANDIVA_TYPE_UINT8_LITERAL_NODE,
                                         "node", gandiva_node,
                                         "value", value,
                                         NULL);
  return GGANDIVA_UINT8_LITERAL_NODE(uint8_literal_node);
}

GGandivaUint16LiteralNode *
ggandiva_uint16_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                     guint16 value)
{
  auto uint16_literal_node = g_object_new(GGANDIVA_TYPE_UINT16_LITERAL_NODE,
                                          "node", gandiva_node,
                                          "value", value,
                                          NULL);
  return GGANDIVA_UINT16_LITERAL_NODE(uint16_literal_node);
}

GGandivaUint32LiteralNode *
ggandiva_uint32_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                     guint32 value)
{
  auto uint32_literal_node = g_object_new(GGANDIVA_TYPE_UINT32_LITERAL_NODE,
                                          "node", gandiva_node,
                                          "value", value,
                                          NULL);
  return GGANDIVA_UINT32_LITERAL_NODE(uint32_literal_node);
}

GGandivaUint64LiteralNode *
ggandiva_uint64_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                     guint64 value)
{
  auto uint64_literal_node = g_object_new(GGANDIVA_TYPE_UINT64_LITERAL_NODE,
                                          "node", gandiva_node,
                                          "value", value,
                                          NULL);
  return GGANDIVA_UINT64_LITERAL_NODE(uint64_literal_node);
}

GGandivaInt8LiteralNode *
ggandiva_int8_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                   gint8 value)
{
  auto int8_literal_node = g_object_new(GGANDIVA_TYPE_INT8_LITERAL_NODE,
                                        "node", gandiva_node,
                                        "value", value,
                                        NULL);
  return GGANDIVA_INT8_LITERAL_NODE(int8_literal_node);
}

GGandivaInt16LiteralNode *
ggandiva_int16_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                    gint16 value)
{
  auto int16_literal_node = g_object_new(GGANDIVA_TYPE_INT16_LITERAL_NODE,
                                         "node", gandiva_node,
                                         "value", value,
                                         NULL);
  return GGANDIVA_INT16_LITERAL_NODE(int16_literal_node);
}

GGandivaInt32LiteralNode *
ggandiva_int32_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                    gint32 value)
{
  auto int32_literal_node = g_object_new(GGANDIVA_TYPE_INT32_LITERAL_NODE,
                                         "node", gandiva_node,
                                         "value", value,
                                         NULL);
  return GGANDIVA_INT32_LITERAL_NODE(int32_literal_node);
}

GGandivaInt64LiteralNode *
ggandiva_int64_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                    gint64 value)
{
  auto int64_literal_node = g_object_new(GGANDIVA_TYPE_INT64_LITERAL_NODE,
                                         "node", gandiva_node,
                                         "value", value,
                                         NULL);
  return GGANDIVA_INT64_LITERAL_NODE(int64_literal_node);
}

GGandivaFloatLiteralNode *
ggandiva_float_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                    gfloat value)
{
  auto float_literal_node = g_object_new(GGANDIVA_TYPE_FLOAT_LITERAL_NODE,
                                         "node", gandiva_node,
                                         "value", value,
                                         NULL);
  return GGANDIVA_FLOAT_LITERAL_NODE(float_literal_node);
}

GGandivaDoubleLiteralNode *
ggandiva_double_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                     gdouble value)
{
  auto double_literal_node = g_object_new(GGANDIVA_TYPE_DOUBLE_LITERAL_NODE,
                                          "node", gandiva_node,
                                          "value", value,
                                          NULL);
  return GGANDIVA_DOUBLE_LITERAL_NODE(double_literal_node);
}

GGandivaStringLiteralNode *
ggandiva_string_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                                     const gchar *value)
{
  auto string_literal_node = g_object_new(GGANDIVA_TYPE_STRING_LITERAL_NODE,
                                          "node", gandiva_node,
                                          "value", value,
                                          NULL);
  return GGANDIVA_STRING_LITERAL_NODE(string_literal_node);
}

GGandivaBinaryLiteralNode *
ggandiva_binary_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node)
{
  auto binary_literal_node = g_object_new(GGANDIVA_TYPE_BINARY_LITERAL_NODE,
                                          "node", gandiva_node,
                                          NULL);
  return GGANDIVA_BINARY_LITERAL_NODE(binary_literal_node);
}
