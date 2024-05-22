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

#include <gandiva-glib/node.h>

G_BEGIN_DECLS

#define GGANDIVA_TYPE_EXPRESSION (ggandiva_expression_get_type())
GGANDIVA_AVAILABLE_IN_0_12
G_DECLARE_DERIVABLE_TYPE(
  GGandivaExpression, ggandiva_expression, GGANDIVA, EXPRESSION, GObject)

struct _GGandivaExpressionClass
{
  GObjectClass parent_class;
};

GGANDIVA_AVAILABLE_IN_0_12
GGandivaExpression *
ggandiva_expression_new(GGandivaNode *root_node, GArrowField *result_field);

GGANDIVA_AVAILABLE_IN_0_12
gchar *
ggandiva_expression_to_string(GGandivaExpression *expression);

#define GGANDIVA_TYPE_CONDITION (ggandiva_condition_get_type())
GGANDIVA_AVAILABLE_IN_4_0
G_DECLARE_DERIVABLE_TYPE(
  GGandivaCondition, ggandiva_condition, GGANDIVA, CONDITION, GGandivaExpression)

struct _GGandivaConditionClass
{
  GGandivaExpressionClass parent_class;
};

GGANDIVA_AVAILABLE_IN_4_0
GGandivaCondition *
ggandiva_condition_new(GGandivaNode *root_node);

G_END_DECLS
