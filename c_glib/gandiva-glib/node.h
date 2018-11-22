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

G_BEGIN_DECLS

#define GGANDIVA_TYPE_NODE (ggandiva_node_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaNode,
                         ggandiva_node,
                         GGANDIVA,
                         NODE,
                         GObject)

struct _GGandivaNodeClass
{
  GObjectClass parent_class;
};

#define GGANDIVA_TYPE_FIELD_NODE (ggandiva_field_node_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaFieldNode,
                         ggandiva_field_node,
                         GGANDIVA,
                         FIELD_NODE,
                         GGandivaNode)
struct _GGandivaFieldNodeClass
{
  GGandivaNodeClass parent_class;
};

GGandivaFieldNode *ggandiva_field_node_new(GArrowField *field);


#define GGANDIVA_TYPE_FUNCTION_NODE (ggandiva_function_node_get_type())
G_DECLARE_DERIVABLE_TYPE(GGandivaFunctionNode,
                         ggandiva_function_node,
                         GGANDIVA,
                         FUNCTION_NODE,
                         GGandivaNode)
struct _GGandivaFunctionNodeClass
{
  GGandivaNodeClass parent_class;
};

GGandivaFunctionNode *
ggandiva_function_node_new(const gchar *name,
                           GList *parameters,
                           GArrowDataType *return_type);
GList *
ggandiva_function_node_get_parameters(GGandivaFunctionNode *node);

G_END_DECLS
