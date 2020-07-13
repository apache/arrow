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

#include <arrow-glib/datum.h>

G_BEGIN_DECLS

#define GARROW_TYPE_EXECUTE_CONTEXT (garrow_execute_context_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExecuteContext,
                         garrow_execute_context,
                         GARROW,
                         EXECUTE_CONTEXT,
                         GObject)
struct _GArrowExecuteContextClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_1_0
GArrowExecuteContext *garrow_execute_context_new(void);


#define GARROW_TYPE_FUNCTION_OPTIONS (garrow_function_options_get_type())
G_DECLARE_INTERFACE(GArrowFunctionOptions,
                    garrow_function_options,
                    GARROW,
                    FUNCTION_OPTIONS,
                    GObject)


#define GARROW_TYPE_FUNCTION (garrow_function_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFunction,
                         garrow_function,
                         GARROW,
                         FUNCTION,
                         GObject)
struct _GArrowFunctionClass
{
  GObjectClass parent_class;
};


GARROW_AVAILABLE_IN_1_0
GArrowFunction *garrow_function_find(const gchar *name);

GARROW_AVAILABLE_IN_1_0
GArrowDatum *garrow_function_execute(GArrowFunction *function,
                                     GList *args,
                                     GArrowFunctionOptions *options,
                                     GArrowExecuteContext *context,
                                     GError **error);


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


/**
 * GArrowFilterNullSelectionBehavior:
 * @GARROW_FILTER_NULL_SELECTION_DROP:
 *   Filtered value will be removed in the output.
 * @GARROW_FILTER_NULL_SELECTION_EMIT_NULL:
 *   Filtered value will be null in the output.
 *
 * They are corresponding to
 * `arrow::compute::FilterOptions::NullSelectionBehavior` values.
 */
typedef enum {
  GARROW_FILTER_NULL_SELECTION_DROP,
  GARROW_FILTER_NULL_SELECTION_EMIT_NULL,
} GArrowFilterNullSelectionBehavior;

#define GARROW_TYPE_FILTER_OPTIONS (garrow_filter_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFilterOptions,
                         garrow_filter_options,
                         GARROW,
                         FILTER_OPTIONS,
                         GObject)
struct _GArrowFilterOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowFilterOptions *
garrow_filter_options_new(void);


#define GARROW_TYPE_TAKE_OPTIONS (garrow_take_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTakeOptions,
                         garrow_take_options,
                         GARROW,
                         TAKE_OPTIONS,
                         GObject)
struct _GArrowTakeOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_14
GArrowTakeOptions *
garrow_take_options_new(void);


/**
 * GArrowCompareOperator:
 * @GARROW_COMPARE_EQUAL: Equal operator.
 * @GARROW_COMPARE_NOT_EQUAL: Not equal operator.
 * @GARROW_COMPARE_GREATER: Greater operator.
 * @GARROW_COMPARE_GREATER_EQUAL: Greater equal operator.
 * @GARROW_COMPARE_LESS: Less operator.
 * @GARROW_COMPARE_LESS_EQUAL: Less equal operator.
 *
 * They are corresponding to `arrow::compute::CompareOperator` values.
 */
typedef enum {
  GARROW_COMPARE_EQUAL,
  GARROW_COMPARE_NOT_EQUAL,
  GARROW_COMPARE_GREATER,
  GARROW_COMPARE_GREATER_EQUAL,
  GARROW_COMPARE_LESS,
  GARROW_COMPARE_LESS_EQUAL
} GArrowCompareOperator;

#define GARROW_TYPE_COMPARE_OPTIONS (garrow_compare_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowCompareOptions,
                         garrow_compare_options,
                         GARROW,
                         COMPARE_OPTIONS,
                         GObject)
struct _GArrowCompareOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_14
GArrowCompareOptions *
garrow_compare_options_new(void);


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
GARROW_AVAILABLE_IN_0_14
GArrowArray *garrow_array_take(GArrowArray *array,
                               GArrowArray *indices,
                               GArrowTakeOptions *options,
                               GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_array_take_chunked_array(GArrowArray *array,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_take(GArrowTable *table,
                  GArrowArray *indices,
                  GArrowTakeOptions *options,
                  GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_take_chunked_array(GArrowTable *table,
                                GArrowChunkedArray *indices,
                                GArrowTakeOptions *options,
                                GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_take(GArrowChunkedArray *chunked_array,
                          GArrowArray *indices,
                          GArrowTakeOptions *options,
                          GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_take_chunked_array(GArrowChunkedArray *chunked_array,
                                        GArrowChunkedArray *indices,
                                        GArrowTakeOptions *options,
                                        GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowRecordBatch *
garrow_record_batch_take(GArrowRecordBatch *record_batch,
                         GArrowArray *indices,
                         GArrowTakeOptions *options,
                         GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_int8_array_compare(GArrowInt8Array *array,
                          gint8 value,
                          GArrowCompareOptions *options,
                          GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_uint8_array_compare(GArrowUInt8Array *array,
                           guint8 value,
                           GArrowCompareOptions *options,
                           GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_int16_array_compare(GArrowInt16Array *array,
                           gint16 value,
                           GArrowCompareOptions *options,
                           GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_uint16_array_compare(GArrowUInt16Array *array,
                            guint16 value,
                            GArrowCompareOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_int32_array_compare(GArrowInt32Array *array,
                           gint32 value,
                           GArrowCompareOptions *options,
                           GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_uint32_array_compare(GArrowUInt32Array *array,
                            guint32 value,
                            GArrowCompareOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_int64_array_compare(GArrowInt64Array *array,
                           gint64 value,
                           GArrowCompareOptions *options,
                           GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_uint64_array_compare(GArrowUInt64Array *array,
                            guint64 value,
                            GArrowCompareOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_float_array_compare(GArrowFloatArray *array,
                           gfloat value,
                           GArrowCompareOptions *options,
                           GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowBooleanArray *
garrow_double_array_compare(GArrowDoubleArray *array,
                            gdouble value,
                            GArrowCompareOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowArray *
garrow_array_filter(GArrowArray *array,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowBooleanArray *
garrow_array_is_in(GArrowArray *left,
                   GArrowArray *right,
                   GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowBooleanArray *
garrow_array_is_in_chunked_array(GArrowArray *left,
                                 GArrowChunkedArray *right,
                                 GError **error);
GARROW_AVAILABLE_IN_0_15
GArrowUInt64Array *
garrow_array_sort_to_indices(GArrowArray *array,
                             GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_filter(GArrowTable *table,
                    GArrowBooleanArray *filter,
                    GArrowFilterOptions *options,
                    GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowTable *
garrow_table_filter_chunked_array(GArrowTable *table,
                                  GArrowChunkedArray *filter,
                                  GArrowFilterOptions *options,
                                  GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_filter(GArrowChunkedArray *chunked_array,
                            GArrowBooleanArray *filter,
                            GArrowFilterOptions *options,
                            GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowChunkedArray *
garrow_chunked_array_filter_chunked_array(GArrowChunkedArray *chunked_array,
                                          GArrowChunkedArray *filter,
                                          GArrowFilterOptions *options,
                                          GError **error);
GARROW_AVAILABLE_IN_0_16
GArrowRecordBatch *
garrow_record_batch_filter(GArrowRecordBatch *record_batch,
                           GArrowBooleanArray *filter,
                           GArrowFilterOptions *options,
                           GError **error);

G_END_DECLS
