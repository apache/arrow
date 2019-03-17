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

#include <arrow-glib/array.h>

G_BEGIN_DECLS

#define GARROW_TYPE_CAST_OPTIONS (garrow_cast_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCastOptions,
                         garrow_cast_options,
                         GARROW,
                         CAST_OPTIONS,
                         GObject)
struct _GArrowCastOptionsClass
{
  GObjectClass parent_class;
};

GArrowCastOptions *garrow_cast_options_new(void);


/**
 * GArrowCountMode:
 * @GARROW_COUNT_ALL: Count all non-null values.
 * @GARROW_COUNT_NULL: Count all null values.
 *
 * They are corresponding to `arrow::compute::CountOptions::mode` values.
 */
typedef enum {
  GARROW_COUNT_ALL,
  GARROW_COUNT_NULL,
} GArrowCountMode;

#define GARROW_TYPE_COUNT_OPTIONS (garrow_count_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCountOptions,
                         garrow_count_options,
                         GARROW,
                         COUNT_OPTIONS,
                         GObject)
struct _GArrowCountOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_13
GArrowCountOptions *
garrow_count_options_new(void);


GArrowArray *garrow_array_cast(GArrowArray *array,
                               GArrowDataType *target_data_type,
                               GArrowCastOptions *options,
                               GError **error);
GArrowArray *garrow_array_unique(GArrowArray *array,
                                GError **error);
GArrowDictionaryArray *garrow_array_dictionary_encode(GArrowArray *array,
                                                      GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_array_count(GArrowArray *array,
                          GArrowCountOptions *options,
                          GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowStructArray *garrow_array_count_values(GArrowArray *array,
                                             GError **error);

GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_invert(GArrowBooleanArray *array,
                                                GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_and(GArrowBooleanArray *left,
                                             GArrowBooleanArray *right,
                                             GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_or(GArrowBooleanArray *left,
                                            GArrowBooleanArray *right,
                                            GError **error);
GARROW_AVAILABLE_IN_0_13
GArrowBooleanArray *garrow_boolean_array_xor(GArrowBooleanArray *left,
                                             GArrowBooleanArray *right,
                                             GError **error);

GARROW_AVAILABLE_IN_0_13
gdouble garrow_numeric_array_mean(GArrowNumericArray *array,
                                  GError **error);

GARROW_AVAILABLE_IN_0_13
gint64 garrow_int8_array_sum(GArrowInt8Array *array,
                             GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint8_array_sum(GArrowUInt8Array *array,
                               GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int16_array_sum(GArrowInt16Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint16_array_sum(GArrowUInt16Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int32_array_sum(GArrowInt32Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint32_array_sum(GArrowUInt32Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gint64 garrow_int64_array_sum(GArrowInt64Array *array,
                              GError **error);
GARROW_AVAILABLE_IN_0_13
guint64 garrow_uint64_array_sum(GArrowUInt64Array *array,
                                GError **error);
GARROW_AVAILABLE_IN_0_13
gdouble garrow_float_array_sum(GArrowFloatArray *array,
                               GError **error);
GARROW_AVAILABLE_IN_0_13
gdouble garrow_double_array_sum(GArrowDoubleArray *array,
                                GError **error);

G_END_DECLS
