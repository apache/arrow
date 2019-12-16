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

#include <arrow-glib/gobject-type.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_DECIMAL128 (garrow_decimal128_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128,
                         garrow_decimal128,
                         GARROW,
                         DECIMAL128,
                         GObject)

struct _GArrowDecimal128Class
{
  GObjectClass parent_class;
};

GArrowDecimal128 *garrow_decimal128_new_string(const gchar *data);
GArrowDecimal128 *garrow_decimal128_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_equal(GArrowDecimal128 *decimal,
                                 GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_not_equal(GArrowDecimal128 *decimal,
                                     GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_less_than(GArrowDecimal128 *decimal,
                                     GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_less_than_or_equal(GArrowDecimal128 *decimal,
                                              GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_greater_than(GArrowDecimal128 *decimal,
                                        GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean garrow_decimal128_greater_than_or_equal(GArrowDecimal128 *decimal,
                                                 GArrowDecimal128 *other_decimal);
gchar *garrow_decimal128_to_string_scale(GArrowDecimal128 *decimal,
                                         gint32 scale);
gchar *garrow_decimal128_to_string(GArrowDecimal128 *decimal);
void garrow_decimal128_abs(GArrowDecimal128 *decimal);
void garrow_decimal128_negate(GArrowDecimal128 *decimal);
gint64 garrow_decimal128_to_integer(GArrowDecimal128 *decimal);
GArrowDecimal128 *garrow_decimal128_plus(GArrowDecimal128 *left,
                                         GArrowDecimal128 *right);
GArrowDecimal128 *garrow_decimal128_minus(GArrowDecimal128 *left,
                                          GArrowDecimal128 *right);
GArrowDecimal128 *garrow_decimal128_multiply(GArrowDecimal128 *left,
                                             GArrowDecimal128 *right);
GArrowDecimal128 *garrow_decimal128_divide(GArrowDecimal128 *left,
                                           GArrowDecimal128 *right,
                                           GArrowDecimal128 **remainder,
                                           GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowDecimal128 *
garrow_decimal128_rescale(GArrowDecimal128 *decimal,
                          gint32 original_scale,
                          gint32 new_scale,
                          GError **error);

G_END_DECLS
