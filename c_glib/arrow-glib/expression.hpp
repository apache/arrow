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

#include <arrow/compute/expression.h>

#include <arrow-glib/expression.h>
#include <arrow-glib/visibility.h>

GARROW_EXPORT
GArrowExpression *
garrow_expression_new_raw(const arrow::compute::Expression &arrow_expression);

GARROW_EXPORT
arrow::compute::Expression *
garrow_expression_get_raw(GArrowExpression *expression);
