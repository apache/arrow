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

#include <glib-object.h>

#include <arrow-glib/version.h>

G_BEGIN_DECLS

/* Disabled because it conflicts with GARROW_TYPE_DECIMAL32 in GArrowType. */
/* #define GARROW_TYPE_DECIMAL32 (garrow_decimal32_get_type()) */
GARROW_AVAILABLE_IN_19_0
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal32, garrow_decimal32, GARROW, DECIMAL32, GObject)

struct _GArrowDecimal32Class
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_new_string(const gchar *data, GError **error);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_copy(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_equal(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_not_equal(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_less_than(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_less_than_or_equal(GArrowDecimal32 *decimal,
                                    GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_greater_than(GArrowDecimal32 *decimal, GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal32_greater_than_or_equal(GArrowDecimal32 *decimal,
                                       GArrowDecimal32 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal32_to_string_scale(GArrowDecimal32 *decimal, gint32 scale);
GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal32_to_string(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
GBytes *
garrow_decimal32_to_bytes(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
void
garrow_decimal32_abs(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
void
garrow_decimal32_negate(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
gint64
garrow_decimal32_to_integer(GArrowDecimal32 *decimal);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_plus(GArrowDecimal32 *left, GArrowDecimal32 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_minus(GArrowDecimal32 *left, GArrowDecimal32 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_multiply(GArrowDecimal32 *left, GArrowDecimal32 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_divide(GArrowDecimal32 *left,
                        GArrowDecimal32 *right,
                        GArrowDecimal32 **remainder,
                        GError **error);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_rescale(GArrowDecimal32 *decimal,
                         gint32 original_scale,
                         gint32 new_scale,
                         GError **error);

/* Disabled because it conflicts with GARROW_TYPE_DECIMAL64 in GArrowType. */
/* #define GARROW_TYPE_DECIMAL64 (garrow_decimal64_get_type()) */
GARROW_AVAILABLE_IN_19_0
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal64, garrow_decimal64, GARROW, DECIMAL64, GObject)

struct _GArrowDecimal64Class
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_new_string(const gchar *data, GError **error);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_copy(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_equal(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_not_equal(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_less_than(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_less_than_or_equal(GArrowDecimal64 *decimal,
                                    GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_greater_than(GArrowDecimal64 *decimal, GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gboolean
garrow_decimal64_greater_than_or_equal(GArrowDecimal64 *decimal,
                                       GArrowDecimal64 *other_decimal);
GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal64_to_string_scale(GArrowDecimal64 *decimal, gint32 scale);
GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal64_to_string(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
GBytes *
garrow_decimal64_to_bytes(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
void
garrow_decimal64_abs(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
void
garrow_decimal64_negate(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
gint64
garrow_decimal64_to_integer(GArrowDecimal64 *decimal);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_plus(GArrowDecimal64 *left, GArrowDecimal64 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_minus(GArrowDecimal64 *left, GArrowDecimal64 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_multiply(GArrowDecimal64 *left, GArrowDecimal64 *right);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_divide(GArrowDecimal64 *left,
                        GArrowDecimal64 *right,
                        GArrowDecimal64 **remainder,
                        GError **error);
GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_rescale(GArrowDecimal64 *decimal,
                         gint32 original_scale,
                         gint32 new_scale,
                         GError **error);

/* Disabled because it conflicts with GARROW_TYPE_DECIMAL128 in GArrowType. */
/* #define GARROW_TYPE_DECIMAL128 (garrow_decimal128_get_type()) */
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128, garrow_decimal128, GARROW, DECIMAL128, GObject)

struct _GArrowDecimal128Class
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_new_string(const gchar *data, GError **error);
GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal128 *
garrow_decimal128_copy(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_equal(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_not_equal(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_less_than(GArrowDecimal128 *decimal, GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_less_than_or_equal(GArrowDecimal128 *decimal,
                                     GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_greater_than(GArrowDecimal128 *decimal,
                               GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_0_12
gboolean
garrow_decimal128_greater_than_or_equal(GArrowDecimal128 *decimal,
                                        GArrowDecimal128 *other_decimal);
GARROW_AVAILABLE_IN_ALL
gchar *
garrow_decimal128_to_string_scale(GArrowDecimal128 *decimal, gint32 scale);
GARROW_AVAILABLE_IN_ALL
gchar *
garrow_decimal128_to_string(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_3_0
GBytes *
garrow_decimal128_to_bytes(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_ALL
void
garrow_decimal128_abs(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_ALL
void
garrow_decimal128_negate(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_ALL
gint64
garrow_decimal128_to_integer(GArrowDecimal128 *decimal);
GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_plus(GArrowDecimal128 *left, GArrowDecimal128 *right);
GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_minus(GArrowDecimal128 *left, GArrowDecimal128 *right);
GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_multiply(GArrowDecimal128 *left, GArrowDecimal128 *right);
GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_divide(GArrowDecimal128 *left,
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
GARROW_AVAILABLE_IN_3_0
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256, garrow_decimal256, GARROW, DECIMAL256, GObject)

struct _GArrowDecimal256Class
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_new_string(const gchar *data, GError **error);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_new_integer(const gint64 data);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_copy(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_equal(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_not_equal(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_less_than(GArrowDecimal256 *decimal, GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_less_than_or_equal(GArrowDecimal256 *decimal,
                                     GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_greater_than(GArrowDecimal256 *decimal,
                               GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_decimal256_greater_than_or_equal(GArrowDecimal256 *decimal,
                                        GArrowDecimal256 *other_decimal);
GARROW_AVAILABLE_IN_3_0
gchar *
garrow_decimal256_to_string_scale(GArrowDecimal256 *decimal, gint32 scale);
GARROW_AVAILABLE_IN_3_0
gchar *
garrow_decimal256_to_string(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
GBytes *
garrow_decimal256_to_bytes(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
void
garrow_decimal256_abs(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
void
garrow_decimal256_negate(GArrowDecimal256 *decimal);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_plus(GArrowDecimal256 *left, GArrowDecimal256 *right);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_multiply(GArrowDecimal256 *left, GArrowDecimal256 *right);
GARROW_AVAILABLE_IN_3_0
GArrowDecimal256 *
garrow_decimal256_divide(GArrowDecimal256 *left,
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
