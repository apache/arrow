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

#include <memory>

#include <gandiva/node.h>
#include <gandiva/tree_expr_builder.h>

#include <gandiva-glib/node.h>

std::shared_ptr<gandiva::Node>
ggandiva_node_get_raw(GGandivaNode *node);
GGandivaFieldNode *
ggandiva_field_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                            GArrowField *field);
GGandivaFunctionNode *
ggandiva_function_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                               const gchar *name,
                               GList *parameters,
                               GArrowDataType *return_type);
GGandivaLiteralNode *
ggandiva_literal_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                              GArrowDataType *return_type);
GGandivaIfNode *
ggandiva_if_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                         GGandivaNode *condition_node,
                         GGandivaNode *then_node,
                         GGandivaNode *else_node,
                         GArrowDataType *return_type);
GGandivaBooleanNode *
ggandiva_boolean_node_new_raw(std::shared_ptr<gandiva::Node> *gandiva_node,
                              GList *children);
