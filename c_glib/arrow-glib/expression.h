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

#include <arrow-glib/compute.h>

G_BEGIN_DECLS

GARROW_AVAILABLE_IN_6_0
gchar *
garrow_expression_to_string(GArrowExpression *expression);
GARROW_AVAILABLE_IN_6_0
gboolean
garrow_expression_equal(GArrowExpression *expression,
                        GArrowExpression *other_expression);


#define GARROW_TYPE_LITERAL_EXPRESSION (garrow_literal_expression_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLiteralExpression,
                         garrow_literal_expression,
                         GARROW,
                         LITERAL_EXPRESSION,
                         GArrowExpression)
struct _GArrowLiteralExpressionClass
{
  GArrowExpressionClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowLiteralExpression *
garrow_literal_expression_new(GArrowDatum *datum);


#define GARROW_TYPE_FIELD_EXPRESSION (garrow_field_expression_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFieldExpression,
                         garrow_field_expression,
                         GARROW,
                         FIELD_EXPRESSION,
                         GArrowExpression)
struct _GArrowFieldExpressionClass
{
  GArrowExpressionClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowFieldExpression *
garrow_field_expression_new(const gchar *reference,
                            GError **error);


#define GARROW_TYPE_CALL_EXPRESSION (garrow_call_expression_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCallExpression,
                         garrow_call_expression,
                         GARROW,
                         CALL_EXPRESSION,
                         GArrowExpression)
struct _GArrowCallExpressionClass
{
  GArrowExpressionClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowCallExpression *
garrow_call_expression_new(const gchar *function,
                           GList *arguments,
                           GArrowFunctionOptions *options);

G_END_DECLS
