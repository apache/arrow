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

#pragma once

#include <arrow-glib/arrow-glib.h>

#include <gandiva-glib/version.h>

G_BEGIN_DECLS

#define GGANDIVA_TYPE_NODE (ggandiva_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaNode, ggandiva_node, GGANDIVA, NODE, GObject)

struct _GGandivaNodeClass
{
  GObjectClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_16;
gchar *
ggandiva_node_to_string(GGandivaNode *node);

#define GGANDIVA_TYPE_FIELD_NODE (ggandiva_field_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(
  GGandivaFieldNode, ggandiva_field_node, GGANDIVA, FIELD_NODE, GGandivaNode)
struct _GGandivaFieldNodeClass
{
  GGandivaNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaFieldNode *
ggandiva_field_node_new(GArrowField *field);

#define GGANDIVA_TYPE_FUNCTION_NODE (ggandiva_function_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(
  GGandivaFunctionNode, ggandiva_function_node, GGANDIVA, FUNCTION_NODE, GGandivaNode)
struct _GGandivaFunctionNodeClass
{
  GGandivaNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaFunctionNode *
ggandiva_function_node_new(const gchar *name,
                           GList *parameters,
                           GArrowDataType *return_type);

GGANDIVA_AVAILABLE_IN_0_12;
GList *
ggandiva_function_node_get_parameters(GGandivaFunctionNode *node);

#define GGANDIVA_TYPE_LITERAL_NODE (ggandiva_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(
  GGandivaLiteralNode, ggandiva_literal_node, GGANDIVA, LITERAL_NODE, GGandivaNode)
struct _GGandivaLiteralNodeClass
{
  GGandivaNodeClass parent_class;
};

#define GGANDIVA_TYPE_NULL_LITERAL_NODE (ggandiva_null_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaNullLiteralNode,
                         ggandiva_null_literal_node,
                         GGANDIVA,
                         NULL_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaNullLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaNullLiteralNode *
ggandiva_null_literal_node_new(GArrowDataType *return_type, GError **error);

#define GGANDIVA_TYPE_BOOLEAN_LITERAL_NODE (ggandiva_boolean_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaBooleanLiteralNode,
                         ggandiva_boolean_literal_node,
                         GGANDIVA,
                         BOOLEAN_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaBooleanLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaBooleanLiteralNode *
ggandiva_boolean_literal_node_new(gboolean value);

GGANDIVA_AVAILABLE_IN_0_12;
gboolean
ggandiva_boolean_literal_node_get_value(GGandivaBooleanLiteralNode *node);

#define GGANDIVA_TYPE_INT8_LITERAL_NODE (ggandiva_int8_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaInt8LiteralNode,
                         ggandiva_int8_literal_node,
                         GGANDIVA,
                         INT8_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaInt8LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaInt8LiteralNode *
ggandiva_int8_literal_node_new(gint8 value);

GGANDIVA_AVAILABLE_IN_0_12;
gint8
ggandiva_int8_literal_node_get_value(GGandivaInt8LiteralNode *node);

#define GGANDIVA_TYPE_UINT8_LITERAL_NODE (ggandiva_uint8_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt8LiteralNode,
                         ggandiva_uint8_literal_node,
                         GGANDIVA,
                         UINT8_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaUInt8LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaUInt8LiteralNode *
ggandiva_uint8_literal_node_new(guint8 value);

GGANDIVA_AVAILABLE_IN_0_12;
guint8
ggandiva_uint8_literal_node_get_value(GGandivaUInt8LiteralNode *node);

#define GGANDIVA_TYPE_INT16_LITERAL_NODE (ggandiva_int16_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaInt16LiteralNode,
                         ggandiva_int16_literal_node,
                         GGANDIVA,
                         INT16_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaInt16LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaInt16LiteralNode *
ggandiva_int16_literal_node_new(gint16 value);

GGANDIVA_AVAILABLE_IN_0_12;
gint16
ggandiva_int16_literal_node_get_value(GGandivaInt16LiteralNode *node);

#define GGANDIVA_TYPE_UINT16_LITERAL_NODE (ggandiva_uint16_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt16LiteralNode,
                         ggandiva_uint16_literal_node,
                         GGANDIVA,
                         UINT16_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaUInt16LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaUInt16LiteralNode *
ggandiva_uint16_literal_node_new(guint16 value);

GGANDIVA_AVAILABLE_IN_0_12;
guint16
ggandiva_uint16_literal_node_get_value(GGandivaUInt16LiteralNode *node);

#define GGANDIVA_TYPE_INT32_LITERAL_NODE (ggandiva_int32_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaInt32LiteralNode,
                         ggandiva_int32_literal_node,
                         GGANDIVA,
                         INT32_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaInt32LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaInt32LiteralNode *
ggandiva_int32_literal_node_new(gint32 value);

GGANDIVA_AVAILABLE_IN_0_12;
gint32
ggandiva_int32_literal_node_get_value(GGandivaInt32LiteralNode *node);

#define GGANDIVA_TYPE_UINT32_LITERAL_NODE (ggandiva_uint32_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt32LiteralNode,
                         ggandiva_uint32_literal_node,
                         GGANDIVA,
                         UINT32_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaUInt32LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaUInt32LiteralNode *
ggandiva_uint32_literal_node_new(guint32 value);

GGANDIVA_AVAILABLE_IN_0_12;
guint32
ggandiva_uint32_literal_node_get_value(GGandivaUInt32LiteralNode *node);

#define GGANDIVA_TYPE_INT64_LITERAL_NODE (ggandiva_int64_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaInt64LiteralNode,
                         ggandiva_int64_literal_node,
                         GGANDIVA,
                         INT64_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaInt64LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaInt64LiteralNode *
ggandiva_int64_literal_node_new(gint64 value);

GGANDIVA_AVAILABLE_IN_0_12;
gint64
ggandiva_int64_literal_node_get_value(GGandivaInt64LiteralNode *node);

#define GGANDIVA_TYPE_UINT64_LITERAL_NODE (ggandiva_uint64_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaUInt64LiteralNode,
                         ggandiva_uint64_literal_node,
                         GGANDIVA,
                         UINT64_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaUInt64LiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaUInt64LiteralNode *
ggandiva_uint64_literal_node_new(guint64 value);

GGANDIVA_AVAILABLE_IN_0_12;
guint64
ggandiva_uint64_literal_node_get_value(GGandivaUInt64LiteralNode *node);

#define GGANDIVA_TYPE_FLOAT_LITERAL_NODE (ggandiva_float_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaFloatLiteralNode,
                         ggandiva_float_literal_node,
                         GGANDIVA,
                         FLOAT_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaFloatLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaFloatLiteralNode *
ggandiva_float_literal_node_new(gfloat value);

GGANDIVA_AVAILABLE_IN_0_12;
gfloat
ggandiva_float_literal_node_get_value(GGandivaFloatLiteralNode *node);

#define GGANDIVA_TYPE_DOUBLE_LITERAL_NODE (ggandiva_double_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaDoubleLiteralNode,
                         ggandiva_double_literal_node,
                         GGANDIVA,
                         DOUBLE_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaDoubleLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaDoubleLiteralNode *
ggandiva_double_literal_node_new(gdouble value);

GGANDIVA_AVAILABLE_IN_0_12;
gdouble
ggandiva_double_literal_node_get_value(GGandivaDoubleLiteralNode *node);

#define GGANDIVA_TYPE_BINARY_LITERAL_NODE (ggandiva_binary_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaBinaryLiteralNode,
                         ggandiva_binary_literal_node,
                         GGANDIVA,
                         BINARY_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaBinaryLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaBinaryLiteralNode *
ggandiva_binary_literal_node_new(const guint8 *value, gsize size);

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaBinaryLiteralNode *
ggandiva_binary_literal_node_new_bytes(GBytes *value);

GGANDIVA_AVAILABLE_IN_0_12;
GBytes *
ggandiva_binary_literal_node_get_value(GGandivaBinaryLiteralNode *node);

#define GGANDIVA_TYPE_STRING_LITERAL_NODE (ggandiva_string_literal_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(GGandivaStringLiteralNode,
                         ggandiva_string_literal_node,
                         GGANDIVA,
                         STRING_LITERAL_NODE,
                         GGandivaLiteralNode)
struct _GGandivaStringLiteralNodeClass
{
  GGandivaLiteralNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaStringLiteralNode *
ggandiva_string_literal_node_new(const gchar *value);

GGANDIVA_AVAILABLE_IN_0_12;
const gchar *
ggandiva_string_literal_node_get_value(GGandivaStringLiteralNode *node);

#define GGANDIVA_TYPE_IF_NODE (ggandiva_if_node_get_type())
GGANDIVA_AVAILABLE_IN_0_12;
G_DECLARE_DERIVABLE_TYPE(
  GGandivaIfNode, ggandiva_if_node, GGANDIVA, IF_NODE, GGandivaNode)
struct _GGandivaIfNodeClass
{
  GGandivaNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12;
GGandivaIfNode *
ggandiva_if_node_new(GGandivaNode *condition_node,
                     GGandivaNode *then_node,
                     GGandivaNode *else_node,
                     GArrowDataType *return_type,
                     GError **error);

#define GGANDIVA_TYPE_BOOLEAN_NODE (ggandiva_boolean_node_get_type())
GGANDIVA_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(
  GGandivaBooleanNode, ggandiva_boolean_node, GGANDIVA, BOOLEAN_NODE, GGandivaNode)

struct _GGandivaBooleanNodeClass
{
  GGandivaNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_17
GList *
ggandiva_boolean_node_get_children(GGandivaBooleanNode *node);

#define GGANDIVA_TYPE_AND_NODE (ggandiva_and_node_get_type())
GGANDIVA_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(
  GGandivaAndNode, ggandiva_and_node, GGANDIVA, AND_NODE, GGandivaBooleanNode)
struct _GGandivaAndNodeClass
{
  GGandivaBooleanNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_17
GGandivaAndNode *
ggandiva_and_node_new(GList *children);

#define GGANDIVA_TYPE_OR_NODE (ggandiva_or_node_get_type())
GGANDIVA_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(
  GGandivaOrNode, ggandiva_or_node, GGANDIVA, OR_NODE, GGandivaBooleanNode)
struct _GGandivaOrNodeClass
{
  GGandivaBooleanNodeClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_17
GGandivaOrNode *
ggandiva_or_node_new(GList *children);

G_END_DECLS
