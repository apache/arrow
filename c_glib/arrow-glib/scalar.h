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
#include <arrow-glib/compute-definition.h>

G_BEGIN_DECLS

#define GARROW_TYPE_SCALAR (garrow_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowScalar,
                         garrow_scalar,
                         GARROW,
                         SCALAR,
                         GObject)
struct _GArrowScalarClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowScalar *
garrow_scalar_parse(GArrowDataType *data_type,
                    const guint8 *data,
                    gsize size,
                    GError **error);

GARROW_AVAILABLE_IN_5_0
GArrowDataType *
garrow_scalar_get_data_type(GArrowScalar *scalar);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_scalar_is_valid(GArrowScalar *scalar);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_scalar_equal(GArrowScalar *scalar,
                    GArrowScalar *other_scalar);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_scalar_equal_options(GArrowScalar *scalar,
                            GArrowScalar *other_scalar,
                            GArrowEqualOptions *options);
GARROW_AVAILABLE_IN_5_0
gchar *
garrow_scalar_to_string(GArrowScalar *scalar);

GARROW_AVAILABLE_IN_5_0
GArrowScalar *
garrow_scalar_cast(GArrowScalar *scalar,
                   GArrowDataType *data_type,
                   GArrowCastOptions *options,
                   GError **error);


#define GARROW_TYPE_NULL_SCALAR (garrow_null_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowNullScalar,
                         garrow_null_scalar,
                         GARROW,
                         NULL_SCALAR,
                         GArrowScalar)
struct _GArrowNullScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowNullScalar *
garrow_null_scalar_new(void);


#define GARROW_TYPE_BOOLEAN_SCALAR (garrow_boolean_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBooleanScalar,
                         garrow_boolean_scalar,
                         GARROW,
                         BOOLEAN_SCALAR,
                         GArrowScalar)
struct _GArrowBooleanScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowBooleanScalar *
garrow_boolean_scalar_new(gboolean value);
GARROW_AVAILABLE_IN_5_0
gboolean
garrow_boolean_scalar_get_value(GArrowBooleanScalar *scalar);


#define GARROW_TYPE_INT8_SCALAR (garrow_int8_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt8Scalar,
                         garrow_int8_scalar,
                         GARROW,
                         INT8_SCALAR,
                         GArrowScalar)
struct _GArrowInt8ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowInt8Scalar *
garrow_int8_scalar_new(gint8 value);
GARROW_AVAILABLE_IN_5_0
gint8
garrow_int8_scalar_get_value(GArrowInt8Scalar *scalar);


#define GARROW_TYPE_INT16_SCALAR (garrow_int16_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt16Scalar,
                         garrow_int16_scalar,
                         GARROW,
                         INT16_SCALAR,
                         GArrowScalar)
struct _GArrowInt16ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowInt16Scalar *
garrow_int16_scalar_new(gint16 value);
GARROW_AVAILABLE_IN_5_0
gint16
garrow_int16_scalar_get_value(GArrowInt16Scalar *scalar);


#define GARROW_TYPE_INT32_SCALAR (garrow_int32_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt32Scalar,
                         garrow_int32_scalar,
                         GARROW,
                         INT32_SCALAR,
                         GArrowScalar)
struct _GArrowInt32ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowInt32Scalar *
garrow_int32_scalar_new(gint32 value);
GARROW_AVAILABLE_IN_5_0
gint32
garrow_int32_scalar_get_value(GArrowInt32Scalar *scalar);


#define GARROW_TYPE_INT64_SCALAR (garrow_int64_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt64Scalar,
                         garrow_int64_scalar,
                         GARROW,
                         INT64_SCALAR,
                         GArrowScalar)
struct _GArrowInt64ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowInt64Scalar *
garrow_int64_scalar_new(gint64 value);
GARROW_AVAILABLE_IN_5_0
gint64
garrow_int64_scalar_get_value(GArrowInt64Scalar *scalar);


#define GARROW_TYPE_UINT8_SCALAR (garrow_uint8_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt8Scalar,
                         garrow_uint8_scalar,
                         GARROW,
                         UINT8_SCALAR,
                         GArrowScalar)
struct _GArrowUInt8ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowUInt8Scalar *
garrow_uint8_scalar_new(guint8 value);
GARROW_AVAILABLE_IN_5_0
guint8
garrow_uint8_scalar_get_value(GArrowUInt8Scalar *scalar);


#define GARROW_TYPE_UINT16_SCALAR (garrow_uint16_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt16Scalar,
                         garrow_uint16_scalar,
                         GARROW,
                         UINT16_SCALAR,
                         GArrowScalar)
struct _GArrowUInt16ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowUInt16Scalar *
garrow_uint16_scalar_new(guint16 value);
GARROW_AVAILABLE_IN_5_0
guint16
garrow_uint16_scalar_get_value(GArrowUInt16Scalar *scalar);


#define GARROW_TYPE_UINT32_SCALAR (garrow_uint32_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt32Scalar,
                         garrow_uint32_scalar,
                         GARROW,
                         UINT32_SCALAR,
                         GArrowScalar)
struct _GArrowUInt32ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowUInt32Scalar *
garrow_uint32_scalar_new(guint32 value);
GARROW_AVAILABLE_IN_5_0
guint32
garrow_uint32_scalar_get_value(GArrowUInt32Scalar *scalar);


#define GARROW_TYPE_UINT64_SCALAR (garrow_uint64_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt64Scalar,
                         garrow_uint64_scalar,
                         GARROW,
                         UINT64_SCALAR,
                         GArrowScalar)
struct _GArrowUInt64ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowUInt64Scalar *
garrow_uint64_scalar_new(guint64 value);
GARROW_AVAILABLE_IN_5_0
guint64
garrow_uint64_scalar_get_value(GArrowUInt64Scalar *scalar);


#define GARROW_TYPE_HALF_FLOAT_SCALAR (garrow_half_float_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowHalfFloatScalar,
                         garrow_half_float_scalar,
                         GARROW,
                         HALF_FLOAT_SCALAR,
                         GArrowScalar)
struct _GArrowHalfFloatScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GArrowHalfFloatScalar *
garrow_half_float_scalar_new(guint16 value);
GARROW_AVAILABLE_IN_11_0
guint16
garrow_half_float_scalar_get_value(GArrowHalfFloatScalar *scalar);


#define GARROW_TYPE_FLOAT_SCALAR (garrow_float_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatScalar,
                         garrow_float_scalar,
                         GARROW,
                         FLOAT_SCALAR,
                         GArrowScalar)
struct _GArrowFloatScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowFloatScalar *
garrow_float_scalar_new(gfloat value);
GARROW_AVAILABLE_IN_5_0
gfloat
garrow_float_scalar_get_value(GArrowFloatScalar *scalar);


#define GARROW_TYPE_DOUBLE_SCALAR (garrow_double_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDoubleScalar,
                         garrow_double_scalar,
                         GARROW,
                         DOUBLE_SCALAR,
                         GArrowScalar)
struct _GArrowDoubleScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDoubleScalar *
garrow_double_scalar_new(gdouble value);
GARROW_AVAILABLE_IN_5_0
gdouble
garrow_double_scalar_get_value(GArrowDoubleScalar *scalar);


#define GARROW_TYPE_BASE_BINARY_SCALAR (garrow_base_binary_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBaseBinaryScalar,
                         garrow_base_binary_scalar,
                         GARROW,
                         BASE_BINARY_SCALAR,
                         GArrowScalar)
struct _GArrowBaseBinaryScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowBuffer *
garrow_base_binary_scalar_get_value(GArrowBaseBinaryScalar *scalar);


#define GARROW_TYPE_BINARY_SCALAR (garrow_binary_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBinaryScalar,
                         garrow_binary_scalar,
                         GARROW,
                         BINARY_SCALAR,
                         GArrowBaseBinaryScalar)
struct _GArrowBinaryScalarClass
{
  GArrowBaseBinaryScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowBinaryScalar *
garrow_binary_scalar_new(GArrowBuffer *value);


#define GARROW_TYPE_STRING_SCALAR (garrow_string_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStringScalar,
                         garrow_string_scalar,
                         GARROW,
                         STRING_SCALAR,
                         GArrowBaseBinaryScalar)
struct _GArrowStringScalarClass
{
  GArrowBaseBinaryScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowStringScalar *
garrow_string_scalar_new(GArrowBuffer *value);


#define GARROW_TYPE_LARGE_BINARY_SCALAR (garrow_large_binary_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeBinaryScalar,
                         garrow_large_binary_scalar,
                         GARROW,
                         LARGE_BINARY_SCALAR,
                         GArrowBaseBinaryScalar)
struct _GArrowLargeBinaryScalarClass
{
  GArrowBaseBinaryScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowLargeBinaryScalar *
garrow_large_binary_scalar_new(GArrowBuffer *value);


#define GARROW_TYPE_LARGE_STRING_SCALAR (garrow_large_string_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeStringScalar,
                         garrow_large_string_scalar,
                         GARROW,
                         LARGE_STRING_SCALAR,
                         GArrowBaseBinaryScalar)
struct _GArrowLargeStringScalarClass
{
  GArrowBaseBinaryScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowLargeStringScalar *
garrow_large_string_scalar_new(GArrowBuffer *value);


#define GARROW_TYPE_FIXED_SIZE_BINARY_SCALAR    \
  (garrow_fixed_size_binary_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFixedSizeBinaryScalar,
                         garrow_fixed_size_binary_scalar,
                         GARROW,
                         FIXED_SIZE_BINARY_SCALAR,
                         GArrowBaseBinaryScalar)
struct _GArrowFixedSizeBinaryScalarClass
{
  GArrowBaseBinaryScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowFixedSizeBinaryScalar *
garrow_fixed_size_binary_scalar_new(GArrowFixedSizeBinaryDataType *data_type,
                                    GArrowBuffer *value);


#define GARROW_TYPE_DATE32_SCALAR (garrow_date32_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate32Scalar,
                         garrow_date32_scalar,
                         GARROW,
                         DATE32_SCALAR,
                         GArrowScalar)
struct _GArrowDate32ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDate32Scalar *
garrow_date32_scalar_new(gint32 value);
GARROW_AVAILABLE_IN_5_0
gint32
garrow_date32_scalar_get_value(GArrowDate32Scalar *scalar);


#define GARROW_TYPE_DATE64_SCALAR (garrow_date64_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate64Scalar,
                         garrow_date64_scalar,
                         GARROW,
                         DATE64_SCALAR,
                         GArrowScalar)
struct _GArrowDate64ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDate64Scalar *
garrow_date64_scalar_new(gint64 value);
GARROW_AVAILABLE_IN_5_0
gint64
garrow_date64_scalar_get_value(GArrowDate64Scalar *scalar);


#define GARROW_TYPE_TIME32_SCALAR (garrow_time32_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime32Scalar,
                         garrow_time32_scalar,
                         GARROW,
                         TIME32_SCALAR,
                         GArrowScalar)
struct _GArrowTime32ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowTime32Scalar *
garrow_time32_scalar_new(GArrowTime32DataType *data_type,
                         gint32 value);
GARROW_AVAILABLE_IN_5_0
gint32
garrow_time32_scalar_get_value(GArrowTime32Scalar *scalar);


#define GARROW_TYPE_TIME64_SCALAR (garrow_time64_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime64Scalar,
                         garrow_time64_scalar,
                         GARROW,
                         TIME64_SCALAR,
                         GArrowScalar)
struct _GArrowTime64ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowTime64Scalar *
garrow_time64_scalar_new(GArrowTime64DataType *data_type,
                         gint64 value);
GARROW_AVAILABLE_IN_5_0
gint64
garrow_time64_scalar_get_value(GArrowTime64Scalar *scalar);


#define GARROW_TYPE_TIMESTAMP_SCALAR (garrow_timestamp_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTimestampScalar,
                         garrow_timestamp_scalar,
                         GARROW,
                         TIMESTAMP_SCALAR,
                         GArrowScalar)
struct _GArrowTimestampScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowTimestampScalar *
garrow_timestamp_scalar_new(GArrowTimestampDataType *data_type,
                            gint64 value);
GARROW_AVAILABLE_IN_5_0
gint64
garrow_timestamp_scalar_get_value(GArrowTimestampScalar *scalar);


#define GARROW_TYPE_MONTH_INTERVAL_SCALAR       \
  (garrow_month_interval_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthIntervalScalar,
                         garrow_month_interval_scalar,
                         GARROW,
                         MONTH_INTERVAL_SCALAR,
                         GArrowScalar)
struct _GArrowMonthIntervalScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthIntervalScalar *
garrow_month_interval_scalar_new(gint32 value);
GARROW_AVAILABLE_IN_8_0
gint32
garrow_month_interval_scalar_get_value(GArrowMonthIntervalScalar *scalar);


#define GARROW_TYPE_DAY_TIME_INTERVAL_SCALAR    \
  (garrow_day_time_interval_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDayTimeIntervalScalar,
                         garrow_day_time_interval_scalar,
                         GARROW,
                         DAY_TIME_INTERVAL_SCALAR,
                         GArrowScalar)
struct _GArrowDayTimeIntervalScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowDayTimeIntervalScalar *
garrow_day_time_interval_scalar_new(GArrowDayMillisecond *value);
GARROW_AVAILABLE_IN_8_0
GArrowDayMillisecond *
garrow_day_time_interval_scalar_get_value(GArrowDayTimeIntervalScalar *scalar);


#define GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_SCALAR \
  (garrow_month_day_nano_interval_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthDayNanoIntervalScalar,
                         garrow_month_day_nano_interval_scalar,
                         GARROW,
                         MONTH_DAY_NANO_INTERVAL_SCALAR,
                         GArrowScalar)
struct _GArrowMonthDayNanoIntervalScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNanoIntervalScalar *
garrow_month_day_nano_interval_scalar_new(GArrowMonthDayNano *value);
GARROW_AVAILABLE_IN_8_0
GArrowMonthDayNano *
garrow_month_day_nano_interval_scalar_get_value(
  GArrowMonthDayNanoIntervalScalar *scalar);


#define GARROW_TYPE_DECIMAL128_SCALAR (garrow_decimal128_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128Scalar,
                         garrow_decimal128_scalar,
                         GARROW,
                         DECIMAL128_SCALAR,
                         GArrowScalar)
struct _GArrowDecimal128ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDecimal128Scalar *
garrow_decimal128_scalar_new(GArrowDecimal128DataType *data_type,
                             GArrowDecimal128 *value);
GARROW_AVAILABLE_IN_5_0
GArrowDecimal128 *
garrow_decimal128_scalar_get_value(GArrowDecimal128Scalar *scalar);


#define GARROW_TYPE_DECIMAL256_SCALAR (garrow_decimal256_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256Scalar,
                         garrow_decimal256_scalar,
                         GARROW,
                         DECIMAL256_SCALAR,
                         GArrowScalar)
struct _GArrowDecimal256ScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDecimal256Scalar *
garrow_decimal256_scalar_new(GArrowDecimal256DataType *data_type,
                             GArrowDecimal256 *value);
GARROW_AVAILABLE_IN_5_0
GArrowDecimal256 *
garrow_decimal256_scalar_get_value(GArrowDecimal256Scalar *scalar);


#define GARROW_TYPE_BASE_LIST_SCALAR (garrow_base_list_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBaseListScalar,
                         garrow_base_list_scalar,
                         GARROW,
                         BASE_LIST_SCALAR,
                         GArrowScalar)
struct _GArrowBaseListScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowArray *
garrow_base_list_scalar_get_value(GArrowBaseListScalar *scalar);

#define GARROW_TYPE_LIST_SCALAR (garrow_list_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowListScalar,
                         garrow_list_scalar,
                         GARROW,
                         LIST_SCALAR,
                         GArrowBaseListScalar)
struct _GArrowListScalarClass
{
  GArrowBaseListScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowListScalar *
garrow_list_scalar_new(GArrowListArray *value);


#define GARROW_TYPE_LARGE_LIST_SCALAR (garrow_large_list_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeListScalar,
                         garrow_large_list_scalar,
                         GARROW,
                         LARGE_LIST_SCALAR,
                         GArrowBaseListScalar)
struct _GArrowLargeListScalarClass
{
  GArrowBaseListScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowLargeListScalar *
garrow_large_list_scalar_new(GArrowLargeListArray *value);


#define GARROW_TYPE_MAP_SCALAR (garrow_map_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMapScalar,
                         garrow_map_scalar,
                         GARROW,
                         MAP_SCALAR,
                         GArrowBaseListScalar)
struct _GArrowMapScalarClass
{
  GArrowBaseListScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowMapScalar *
garrow_map_scalar_new(GArrowStructArray *value);


#define GARROW_TYPE_STRUCT_SCALAR (garrow_struct_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStructScalar,
                         garrow_struct_scalar,
                         GARROW,
                         STRUCT_SCALAR,
                         GArrowScalar)
struct _GArrowStructScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowStructScalar *
garrow_struct_scalar_new(GArrowStructDataType *data_type,
                         GList *value);
GARROW_AVAILABLE_IN_5_0
GList *
garrow_struct_scalar_get_value(GArrowStructScalar *scalar);


#define GARROW_TYPE_UNION_SCALAR (garrow_union_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUnionScalar,
                         garrow_union_scalar,
                         GARROW,
                         UNION_SCALAR,
                         GArrowScalar)
struct _GArrowUnionScalarClass
{
  GArrowScalarClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
gint8
garrow_union_scalar_get_type_code(GArrowUnionScalar *scalar);
GARROW_AVAILABLE_IN_5_0
GArrowScalar *
garrow_union_scalar_get_value(GArrowUnionScalar *scalar);


#define GARROW_TYPE_SPARSE_UNION_SCALAR (garrow_sparse_union_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSparseUnionScalar,
                         garrow_sparse_union_scalar,
                         GARROW,
                         SPARSE_UNION_SCALAR,
                         GArrowUnionScalar)
struct _GArrowSparseUnionScalarClass
{
  GArrowUnionScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowSparseUnionScalar *
garrow_sparse_union_scalar_new(GArrowSparseUnionDataType *data_type,
                               gint8 type_code,
                               GArrowScalar *value);


#define GARROW_TYPE_DENSE_UNION_SCALAR (garrow_dense_union_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDenseUnionScalar,
                         garrow_dense_union_scalar,
                         GARROW,
                         DENSE_UNION_SCALAR,
                         GArrowUnionScalar)
struct _GArrowDenseUnionScalarClass
{
  GArrowUnionScalarClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GArrowDenseUnionScalar *
garrow_dense_union_scalar_new(GArrowDenseUnionDataType *data_type,
                              gint8 type_code,
                              GArrowScalar *value);


#define GARROW_TYPE_EXTENSION_SCALAR (garrow_extension_scalar_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExtensionScalar,
                         garrow_extension_scalar,
                         GARROW,
                         EXTENSION_SCALAR,
                         GArrowScalar)
struct _GArrowExtensionScalarClass
{
  GArrowScalarClass parent_class;
};

G_END_DECLS
