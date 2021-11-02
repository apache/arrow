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

/* Disabled because it conflicts with GARROW_TYPE_DECIMAL128 in GArrowType. */
/* #define GARROW_TYPE_DECIMAL128 (garrow_decimal128_get_type()) */
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128,
                         garrow_decimal128,
                         GARROW,
                         DECIMAL128,
                         GObject)

struct _GArrowDecimal128Class
{
  GObjectClass parent_class;
};

GArrowDecimal128 *
garrow_decimal128_new_string(const gchar *data,
                             GError **error);
GArrowDecimal128 *garrow_decimal128_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal128 *garrow_decimal128_copy(GArrowDecimal128 *decimal);
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
GARROW_AVAILABLE_IN_3_0
GBytes *garrow_decimal128_to_bytes(GArrowDecimal128 *decimal);
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


/* Disabled because it conflicts with GARROW_TYPE_DECIMAL256 in GArrowType. */
/* #define GARROW_TYPE_DECIMAL256 (garrow_decimal256_get_type()) */
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256,
                         garrow_decimal256,
                         GARROW,
                         DECIMAL256,
                         GObject)

struct _GArrowDecimal256Class
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_new_string(const gchar *data,
                             GError **error);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *garrow_decimal256_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *garrow_decimal256_copy(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_equal(GArrowDecimal256 *decimal,
                                 GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_not_equal(GArrowDecimal256 *decimal,
                                     GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_less_than(GArrowDecimal256 *decimal,
                                     GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_less_than_or_equal(GArrowDecimal256 *decimal,
                                              GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_greater_than(GArrowDecimal256 *decimal,
                                        GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean garrow_decimal256_greater_than_or_equal(GArrowDecimal256 *decimal,
                                                 GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gchar *garrow_decimal256_to_string_scale(GArrowDecimal256 *decimal,
                                         gint32 scale);
GARROW_AVAILABLE_IN_3_0
gchar *garrow_decimal256_to_string(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
GBytes *garrow_decimal256_to_bytes(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
void garrow_decimal256_abs(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
void garrow_decimal256_negate(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *garrow_decimal256_plus(GArrowDecimal256 *left,
                                         GArrowDecimal256 *right);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *garrow_decimal256_multiply(GArrowDecimal256 *left,
                                             GArrowDecimal256 *right);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *garrow_decimal256_divide(GArrowDecimal256 *left,
                                           GArrowDecimal256 *right,
                                           GArrowDecimal256 **remainder,
                                           GError **error);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_rescale(GArrowDecimal256 *decimal,
                          gint32 original_scale,
                          gint32 new_scale,
                          GError **error);

G_END_DECLS
