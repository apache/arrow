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

#include <gandiva/expression.h>
#include <gandiva/tree_expr_builder.h>

#include <gandiva-glib/expression.h>

GGandivaExpression *
ggandiva_expression_new_raw(std::shared_ptr<gandiva::Expression> *gandiva_expression,
                            GGandivaNode *root_node,
                            GArrowField *result_field);
std::shared_ptr<gandiva::Expression>
ggandiva_expression_get_raw(GGandivaExpression *expression);

GGandivaCondition *
ggandiva_condition_new_raw(std::shared_ptr<gandiva::Condition> *gandiva_expression,
                           GGandivaNode *root_node);
std::shared_ptr<gandiva::Condition>
ggandiva_condition_get_raw(GGandivaCondition *condition);
