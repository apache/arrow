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

#include <arrow-glib/basic-array-definition.h>
#include <arrow-glib/basic-data-type.h>
#include <arrow-glib/buffer.h>
#include <arrow-glib/interval.h>

G_BEGIN_DECLS

#define GARROW_TYPE_EQUAL_OPTIONS (garrow_equal_options_get_type())
GARROW_AVAILABLE_IN_5_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowEqualOptions, garrow_equal_options, GARROW, EQUAL_OPTIONS, GObject)
struct _GArrowEqualOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowEqualOptions *
garrow_equal_options_new(void);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_equal_options_is_approx(GArrowEqualOptions *options);

#define GARROW_TYPE_ARRAY_STATISTICS (garrow_array_statistics_get_type())
GARROW_AVAILABLE_IN_20_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowArrayStatistics, garrow_array_statistics, GARROW, ARRAY_STATISTICS, GObject)
struct _GArrowArrayStatisticsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_20_0
gboolean
garrow_array_statistics_has_null_count(GArrowArrayStatistics *statistics);
GARROW_AVAILABLE_IN_20_0
gint64
garrow_array_statistics_get_null_count(GArrowArrayStatistics *statistics);

GARROW_AVAILABLE_IN_21_0
gboolean
garrow_array_statistics_has_distinct_count(GArrowArrayStatistics *statistics);
GARROW_AVAILABLE_IN_21_0
gint64
garrow_array_statistics_get_distinct_count(GArrowArrayStatistics *statistics);

GARROW_AVAILABLE_IN_6_0
GArrowArray *
garrow_array_import(gpointer c_abi_array, GArrowDataType *data_type, GError **error);

GARROW_AVAILABLE_IN_6_0
gboolean
garrow_array_export(GArrowArray *array,
                    gpointer *c_abi_array,
                    gpointer *c_abi_schema,
                    GError **error);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_array_equal(GArrowArray *array, GArrowArray *other_array);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_array_equal_options(GArrowArray *array,
                           GArrowArray *other_array,
                           GArrowEqualOptions *options);
GARROW_AVAILABLE_IN_ALL
gboolean
garrow_array_equal_approx(GArrowArray *array, GArrowArray *other_array);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_array_equal_range(GArrowArray *array,
                         gint64 start_index,
                         GArrowArray *other_array,
                         gint64 other_start_index,
                         gint64 end_index,
                         GArrowEqualOptions *options);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_array_is_null(GArrowArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_array_is_valid(GArrowArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_array_get_length(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_array_get_offset(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_array_get_n_nulls(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowBuffer *
garrow_array_get_null_bitmap(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowDataType *
garrow_array_get_value_data_type(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowType
garrow_array_get_value_type(GArrowArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_array_slice(GArrowArray *array, gint64 offset, gint64 length);

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_array_to_string(GArrowArray *array, GError **error);

GARROW_AVAILABLE_IN_0_15
GArrowArray *
garrow_array_view(GArrowArray *array, GArrowDataType *return_type, GError **error);

GARROW_AVAILABLE_IN_0_15
gchar *
garrow_array_diff_unified(GArrowArray *array, GArrowArray *other_array);

GARROW_AVAILABLE_IN_4_0
GArrowArray *
garrow_array_concatenate(GArrowArray *array, GList *other_arrays, GError **error);

GARROW_AVAILABLE_IN_20_0
gboolean
garrow_array_validate(GArrowArray *array, GError **error);

GARROW_AVAILABLE_IN_20_0
gboolean
garrow_array_validate_full(GArrowArray *array, GError **error);

GARROW_AVAILABLE_IN_20_0
GArrowArrayStatistics *
garrow_array_get_statistics(GArrowArray *array);

#define GARROW_TYPE_NULL_ARRAY (garrow_null_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowNullArray, garrow_null_array, GARROW, NULL_ARRAY, GArrowArray)
struct _GArrowNullArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowNullArray *
garrow_null_array_new(gint64 length);

#define GARROW_TYPE_PRIMITIVE_ARRAY (garrow_primitive_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowPrimitiveArray, garrow_primitive_array, GARROW, PRIMITIVE_ARRAY, GArrowArray)
struct _GArrowPrimitiveArrayClass
{
  GArrowArrayClass parent_class;
};

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_1_0_FOR(garrow_primitive_array_get_data_buffer)
GArrowBuffer *
garrow_primitive_array_get_buffer(GArrowPrimitiveArray *array);
#endif
GARROW_AVAILABLE_IN_1_0
GArrowBuffer *
garrow_primitive_array_get_data_buffer(GArrowPrimitiveArray *array);

#define GARROW_TYPE_BOOLEAN_ARRAY (garrow_boolean_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowBooleanArray, garrow_boolean_array, GARROW, BOOLEAN_ARRAY, GArrowPrimitiveArray)
struct _GArrowBooleanArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowBooleanArray *
garrow_boolean_array_new(gint64 length,
                         GArrowBuffer *data,
                         GArrowBuffer *null_bitmap,
                         gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_boolean_array_get_value(GArrowBooleanArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
gboolean *
garrow_boolean_array_get_values(GArrowBooleanArray *array, gint64 *length);

#define GARROW_TYPE_NUMERIC_ARRAY (garrow_numeric_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowNumericArray, garrow_numeric_array, GARROW, NUMERIC_ARRAY, GArrowPrimitiveArray)
struct _GArrowNumericArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

#define GARROW_TYPE_INT8_ARRAY (garrow_int8_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowInt8Array, garrow_int8_array, GARROW, INT8_ARRAY, GArrowNumericArray)
struct _GArrowInt8ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowInt8Array *
garrow_int8_array_new(gint64 length,
                      GArrowBuffer *data,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint8
garrow_int8_array_get_value(GArrowInt8Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint8 *
garrow_int8_array_get_values(GArrowInt8Array *array, gint64 *length);

#define GARROW_TYPE_UINT8_ARRAY (garrow_uint8_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUInt8Array, garrow_uint8_array, GARROW, UINT8_ARRAY, GArrowNumericArray)
struct _GArrowUInt8ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowUInt8Array *
garrow_uint8_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
guint8
garrow_uint8_array_get_value(GArrowUInt8Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const guint8 *
garrow_uint8_array_get_values(GArrowUInt8Array *array, gint64 *length);

#define GARROW_TYPE_INT16_ARRAY (garrow_int16_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowInt16Array, garrow_int16_array, GARROW, INT16_ARRAY, GArrowNumericArray)
struct _GArrowInt16ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowInt16Array *
garrow_int16_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint16
garrow_int16_array_get_value(GArrowInt16Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint16 *
garrow_int16_array_get_values(GArrowInt16Array *array, gint64 *length);

#define GARROW_TYPE_UINT16_ARRAY (garrow_uint16_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUInt16Array, garrow_uint16_array, GARROW, UINT16_ARRAY, GArrowNumericArray)
struct _GArrowUInt16ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowUInt16Array *
garrow_uint16_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
guint16
garrow_uint16_array_get_value(GArrowUInt16Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const guint16 *
garrow_uint16_array_get_values(GArrowUInt16Array *array, gint64 *length);

#define GARROW_TYPE_INT32_ARRAY (garrow_int32_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowInt32Array, garrow_int32_array, GARROW, INT32_ARRAY, GArrowNumericArray)
struct _GArrowInt32ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowInt32Array *
garrow_int32_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint32
garrow_int32_array_get_value(GArrowInt32Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint32 *
garrow_int32_array_get_values(GArrowInt32Array *array, gint64 *length);

#define GARROW_TYPE_UINT32_ARRAY (garrow_uint32_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUInt32Array, garrow_uint32_array, GARROW, UINT32_ARRAY, GArrowNumericArray)
struct _GArrowUInt32ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowUInt32Array *
garrow_uint32_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
guint32
garrow_uint32_array_get_value(GArrowUInt32Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const guint32 *
garrow_uint32_array_get_values(GArrowUInt32Array *array, gint64 *length);

#define GARROW_TYPE_INT64_ARRAY (garrow_int64_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowInt64Array, garrow_int64_array, GARROW, INT64_ARRAY, GArrowNumericArray)
struct _GArrowInt64ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowInt64Array *
garrow_int64_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_int64_array_get_value(GArrowInt64Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint64 *
garrow_int64_array_get_values(GArrowInt64Array *array, gint64 *length);

#define GARROW_TYPE_UINT64_ARRAY (garrow_uint64_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUInt64Array, garrow_uint64_array, GARROW, UINT64_ARRAY, GArrowNumericArray)
struct _GArrowUInt64ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowUInt64Array *
garrow_uint64_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
guint64
garrow_uint64_array_get_value(GArrowUInt64Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const guint64 *
garrow_uint64_array_get_values(GArrowUInt64Array *array, gint64 *length);

#define GARROW_TYPE_HALF_FLOAT_ARRAY (garrow_half_float_array_get_type())
GARROW_AVAILABLE_IN_11_0
G_DECLARE_DERIVABLE_TYPE(GArrowHalfFloatArray,
                         garrow_half_float_array,
                         GARROW,
                         HALF_FLOAT_ARRAY,
                         GArrowNumericArray)
struct _GArrowHalfFloatArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GArrowHalfFloatArray *
garrow_half_float_array_new(gint64 length,
                            GArrowBuffer *data,
                            GArrowBuffer *null_bitmap,
                            gint64 n_nulls);

GARROW_AVAILABLE_IN_11_0
guint16
garrow_half_float_array_get_value(GArrowHalfFloatArray *array, gint64 i);
GARROW_AVAILABLE_IN_11_0
const guint16 *
garrow_half_float_array_get_values(GArrowHalfFloatArray *array, gint64 *length);

#define GARROW_TYPE_FLOAT_ARRAY (garrow_float_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowFloatArray, garrow_float_array, GARROW, FLOAT_ARRAY, GArrowNumericArray)
struct _GArrowFloatArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowFloatArray *
garrow_float_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gfloat
garrow_float_array_get_value(GArrowFloatArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gfloat *
garrow_float_array_get_values(GArrowFloatArray *array, gint64 *length);

#define GARROW_TYPE_DOUBLE_ARRAY (garrow_double_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowDoubleArray, garrow_double_array, GARROW, DOUBLE_ARRAY, GArrowNumericArray)
struct _GArrowDoubleArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDoubleArray *
garrow_double_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gdouble
garrow_double_array_get_value(GArrowDoubleArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gdouble *
garrow_double_array_get_values(GArrowDoubleArray *array, gint64 *length);

#define GARROW_TYPE_BINARY_ARRAY (garrow_binary_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowBinaryArray, garrow_binary_array, GARROW, BINARY_ARRAY, GArrowArray)
struct _GArrowBinaryArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowBinaryArray *
garrow_binary_array_new(gint64 length,
                        GArrowBuffer *value_offsets,
                        GArrowBuffer *value_data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
GBytes *
garrow_binary_array_get_value(GArrowBinaryArray *array, gint64 i);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_1_0_FOR(garrow_binary_array_get_data_buffer)
GArrowBuffer *
garrow_binary_array_get_buffer(GArrowBinaryArray *array);
#endif
GARROW_AVAILABLE_IN_1_0
GArrowBuffer *
garrow_binary_array_get_data_buffer(GArrowBinaryArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowBuffer *
garrow_binary_array_get_offsets_buffer(GArrowBinaryArray *array);

#define GARROW_TYPE_LARGE_BINARY_ARRAY (garrow_large_binary_array_get_type())
GARROW_AVAILABLE_IN_0_16
G_DECLARE_DERIVABLE_TYPE(GArrowLargeBinaryArray,
                         garrow_large_binary_array,
                         GARROW,
                         LARGE_BINARY_ARRAY,
                         GArrowArray)
struct _GArrowLargeBinaryArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeBinaryArray *
garrow_large_binary_array_new(gint64 length,
                              GArrowBuffer *value_offsets,
                              GArrowBuffer *value_data,
                              GArrowBuffer *null_bitmap,
                              gint64 n_nulls);

GARROW_AVAILABLE_IN_0_16
GBytes *
garrow_large_binary_array_get_value(GArrowLargeBinaryArray *array, gint64 i);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_1_0_FOR(garrow_large_binary_array_get_data_buffer)
GARROW_AVAILABLE_IN_0_16
GArrowBuffer *
garrow_large_binary_array_get_buffer(GArrowLargeBinaryArray *array);
#endif
GARROW_AVAILABLE_IN_1_0
GArrowBuffer *
garrow_large_binary_array_get_data_buffer(GArrowLargeBinaryArray *array);

GARROW_AVAILABLE_IN_0_16
GArrowBuffer *
garrow_large_binary_array_get_offsets_buffer(GArrowLargeBinaryArray *array);

#define GARROW_TYPE_STRING_ARRAY (garrow_string_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowStringArray, garrow_string_array, GARROW, STRING_ARRAY, GArrowBinaryArray)
struct _GArrowStringArrayClass
{
  GArrowBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowStringArray *
garrow_string_array_new(gint64 length,
                        GArrowBuffer *value_offsets,
                        GArrowBuffer *value_data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_string_array_get_string(GArrowStringArray *array, gint64 i);

#define GARROW_TYPE_LARGE_STRING_ARRAY (garrow_large_string_array_get_type())
GARROW_AVAILABLE_IN_0_16
G_DECLARE_DERIVABLE_TYPE(GArrowLargeStringArray,
                         garrow_large_string_array,
                         GARROW,
                         LARGE_STRING_ARRAY,
                         GArrowLargeBinaryArray)
struct _GArrowLargeStringArrayClass
{
  GArrowLargeBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeStringArray *
garrow_large_string_array_new(gint64 length,
                              GArrowBuffer *value_offsets,
                              GArrowBuffer *value_data,
                              GArrowBuffer *null_bitmap,
                              gint64 n_nulls);

GARROW_AVAILABLE_IN_0_16
gchar *
garrow_large_string_array_get_string(GArrowLargeStringArray *array, gint64 i);

#define GARROW_TYPE_BINARY_VIEW_ARRAY (garrow_binary_view_array_get_type())
GARROW_AVAILABLE_IN_20_0
G_DECLARE_DERIVABLE_TYPE(
  GArrowBinaryViewArray, garrow_binary_view_array, GARROW, BINARY_VIEW_ARRAY, GArrowArray)
struct _GArrowBinaryViewArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_20_0
GArrowBinaryViewArray *
garrow_binary_view_array_new(gint64 length,
                             GArrowBuffer *views,
                             GList *data_buffers,
                             GArrowBuffer *null_bitmap,
                             gint64 n_nulls,
                             gint64 offset);

GARROW_AVAILABLE_IN_20_0
GBytes *
garrow_binary_view_array_get_value(GArrowBinaryViewArray *array, gint64 i);

#define GARROW_TYPE_STRING_VIEW_ARRAY (garrow_string_view_array_get_type())
GARROW_AVAILABLE_IN_20_0
G_DECLARE_DERIVABLE_TYPE(GArrowStringViewArray,
                         garrow_string_view_array,
                         GARROW,
                         STRING_VIEW_ARRAY,
                         GArrowBinaryViewArray)
struct _GArrowStringViewArrayClass
{
  GArrowBinaryViewArrayClass parent_class;
};

GARROW_AVAILABLE_IN_20_0
GArrowStringViewArray *
garrow_string_view_array_new(gint64 length,
                             GArrowBuffer *views,
                             GList *data_buffers,
                             GArrowBuffer *null_bitmap,
                             gint64 n_nulls,
                             gint64 offset);

GARROW_AVAILABLE_IN_20_0
GBytes *
garrow_string_view_array_get_value(GArrowStringViewArray *array, gint64 i);

#define GARROW_TYPE_DATE32_ARRAY (garrow_date32_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowDate32Array, garrow_date32_array, GARROW, DATE32_ARRAY, GArrowNumericArray)
struct _GArrowDate32ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDate32Array *
garrow_date32_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint32
garrow_date32_array_get_value(GArrowDate32Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint32 *
garrow_date32_array_get_values(GArrowDate32Array *array, gint64 *length);

#define GARROW_TYPE_DATE64_ARRAY (garrow_date64_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowDate64Array, garrow_date64_array, GARROW, DATE64_ARRAY, GArrowNumericArray)
struct _GArrowDate64ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDate64Array *
garrow_date64_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_date64_array_get_value(GArrowDate64Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint64 *
garrow_date64_array_get_values(GArrowDate64Array *array, gint64 *length);

#define GARROW_TYPE_TIMESTAMP_ARRAY (garrow_timestamp_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowTimestampArray,
                         garrow_timestamp_array,
                         GARROW,
                         TIMESTAMP_ARRAY,
                         GArrowNumericArray)
struct _GArrowTimestampArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowTimestampArray *
garrow_timestamp_array_new(GArrowTimestampDataType *data_type,
                           gint64 length,
                           GArrowBuffer *data,
                           GArrowBuffer *null_bitmap,
                           gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_timestamp_array_get_value(GArrowTimestampArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint64 *
garrow_timestamp_array_get_values(GArrowTimestampArray *array, gint64 *length);

#define GARROW_TYPE_TIME32_ARRAY (garrow_time32_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowTime32Array, garrow_time32_array, GARROW, TIME32_ARRAY, GArrowNumericArray)
struct _GArrowTime32ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowTime32Array *
garrow_time32_array_new(GArrowTime32DataType *data_type,
                        gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint32
garrow_time32_array_get_value(GArrowTime32Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint32 *
garrow_time32_array_get_values(GArrowTime32Array *array, gint64 *length);

#define GARROW_TYPE_TIME64_ARRAY (garrow_time64_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowTime64Array, garrow_time64_array, GARROW, TIME64_ARRAY, GArrowNumericArray)
struct _GArrowTime64ArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowTime64Array *
garrow_time64_array_new(GArrowTime64DataType *data_type,
                        gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_time64_array_get_value(GArrowTime64Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
const gint64 *
garrow_time64_array_get_values(GArrowTime64Array *array, gint64 *length);

#define GARROW_TYPE_MONTH_INTERVAL_ARRAY (garrow_month_interval_array_get_type())
GARROW_AVAILABLE_IN_8_0
G_DECLARE_DERIVABLE_TYPE(GArrowMonthIntervalArray,
                         garrow_month_interval_array,
                         GARROW,
                         MONTH_INTERVAL_ARRAY,
                         GArrowNumericArray)
struct _GArrowMonthIntervalArrayClass
{
  GArrowNumericArrayClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthIntervalArray *
garrow_month_interval_array_new(gint64 length,
                                GArrowBuffer *data,
                                GArrowBuffer *null_bitmap,
                                gint64 n_nulls);
GARROW_AVAILABLE_IN_8_0
gint32
garrow_month_interval_array_get_value(GArrowMonthIntervalArray *array, gint64 i);

GARROW_AVAILABLE_IN_8_0
const gint32 *
garrow_month_interval_array_get_values(GArrowMonthIntervalArray *array, gint64 *length);

#define GARROW_TYPE_DAY_TIME_INTERVAL_ARRAY (garrow_day_time_interval_array_get_type())
GARROW_AVAILABLE_IN_8_0
G_DECLARE_DERIVABLE_TYPE(GArrowDayTimeIntervalArray,
                         garrow_day_time_interval_array,
                         GARROW,
                         DAY_TIME_INTERVAL_ARRAY,
                         GArrowPrimitiveArray)
struct _GArrowDayTimeIntervalArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowDayTimeIntervalArray *
garrow_day_time_interval_array_new(gint64 length,
                                   GArrowBuffer *data,
                                   GArrowBuffer *null_bitmap,
                                   gint64 n_nulls);
GARROW_AVAILABLE_IN_8_0
GArrowDayMillisecond *
garrow_day_time_interval_array_get_value(GArrowDayTimeIntervalArray *array, gint64 i);

GARROW_AVAILABLE_IN_8_0
GList *
garrow_day_time_interval_array_get_values(GArrowDayTimeIntervalArray *array);

#define GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_ARRAY                                        \
  (garrow_month_day_nano_interval_array_get_type())
GARROW_AVAILABLE_IN_8_0
G_DECLARE_DERIVABLE_TYPE(GArrowMonthDayNanoIntervalArray,
                         garrow_month_day_nano_interval_array,
                         GARROW,
                         MONTH_DAY_NANO_INTERVAL_ARRAY,
                         GArrowPrimitiveArray)
struct _GArrowMonthDayNanoIntervalArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNanoIntervalArray *
garrow_month_day_nano_interval_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls);
GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNano *
garrow_month_day_nano_interval_array_get_value(GArrowMonthDayNanoIntervalArray *array,
                                               gint64 i);
GARROW_AVAILABLE_IN_8_0
GList *
garrow_month_day_nano_interval_array_get_values(GArrowMonthDayNanoIntervalArray *array);

#define GARROW_TYPE_FIXED_SIZE_BINARY_ARRAY (garrow_fixed_size_binary_array_get_type())
GARROW_AVAILABLE_IN_3_0
G_DECLARE_DERIVABLE_TYPE(GArrowFixedSizeBinaryArray,
                         garrow_fixed_size_binary_array,
                         GARROW,
                         FIXED_SIZE_BINARY_ARRAY,
                         GArrowPrimitiveArray)
struct _GArrowFixedSizeBinaryArrayClass
{
  GArrowPrimitiveArrayClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowFixedSizeBinaryArray *
garrow_fixed_size_binary_array_new(GArrowFixedSizeBinaryDataType *data_type,
                                   gint64 length,
                                   GArrowBuffer *data,
                                   GArrowBuffer *null_bitmap,
                                   gint64 n_nulls);
GARROW_AVAILABLE_IN_3_0
gint32
garrow_fixed_size_binary_array_get_byte_width(GArrowFixedSizeBinaryArray *array);

GARROW_AVAILABLE_IN_3_0
GBytes *
garrow_fixed_size_binary_array_get_value(GArrowFixedSizeBinaryArray *array, gint64 i);

GARROW_AVAILABLE_IN_3_0
GBytes *
garrow_fixed_size_binary_array_get_values_bytes(GArrowFixedSizeBinaryArray *array);

#define GARROW_TYPE_DECIMAL32_ARRAY (garrow_decimal32_array_get_type())
GARROW_AVAILABLE_IN_19_0
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal32Array,
                         garrow_decimal32_array,
                         GARROW,
                         DECIMAL32_ARRAY,
                         GArrowFixedSizeBinaryArray)
struct _GArrowDecimal32ArrayClass
{
  GArrowFixedSizeBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal32_array_format_value(GArrowDecimal32Array *array, gint64 i);

GARROW_AVAILABLE_IN_19_0
GArrowDecimal32 *
garrow_decimal32_array_get_value(GArrowDecimal32Array *array, gint64 i);

#define GARROW_TYPE_DECIMAL64_ARRAY (garrow_decimal64_array_get_type())
GARROW_AVAILABLE_IN_19_0
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal64Array,
                         garrow_decimal64_array,
                         GARROW,
                         DECIMAL64_ARRAY,
                         GArrowFixedSizeBinaryArray)
struct _GArrowDecimal64ArrayClass
{
  GArrowFixedSizeBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_19_0
gchar *
garrow_decimal64_array_format_value(GArrowDecimal64Array *array, gint64 i);

GARROW_AVAILABLE_IN_19_0
GArrowDecimal64 *
garrow_decimal64_array_get_value(GArrowDecimal64Array *array, gint64 i);

#define GARROW_TYPE_DECIMAL128_ARRAY (garrow_decimal128_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128Array,
                         garrow_decimal128_array,
                         GARROW,
                         DECIMAL128_ARRAY,
                         GArrowFixedSizeBinaryArray)
struct _GArrowDecimal128ArrayClass
{
  GArrowFixedSizeBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_decimal128_array_format_value(GArrowDecimal128Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
GArrowDecimal128 *
garrow_decimal128_array_get_value(GArrowDecimal128Array *array, gint64 i);

#define GARROW_TYPE_DECIMAL256_ARRAY (garrow_decimal256_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256Array,
                         garrow_decimal256_array,
                         GARROW,
                         DECIMAL256_ARRAY,
                         GArrowFixedSizeBinaryArray)
struct _GArrowDecimal256ArrayClass
{
  GArrowFixedSizeBinaryArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_decimal256_array_format_value(GArrowDecimal256Array *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
GArrowDecimal256 *
garrow_decimal256_array_get_value(GArrowDecimal256Array *array, gint64 i);

GARROW_AVAILABLE_IN_3_0
GArrowArray *
garrow_extension_array_get_storage(GArrowExtensionArray *array);

G_END_DECLS
