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
#include <arrow-glib/chunked-array-definition.h>
#include <arrow-glib/decimal.h>
#include <arrow-glib/type.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_DATA_TYPE (garrow_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDataType,
                         garrow_data_type,
                         GARROW,
                         DATA_TYPE,
                         GObject)
struct _GArrowDataTypeClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowDataType *
garrow_data_type_import(gpointer c_abi_schema,
                        GError **error);

GARROW_AVAILABLE_IN_6_0
gpointer
garrow_data_type_export(GArrowDataType *data_type,
                        GError **error);

gboolean   garrow_data_type_equal     (GArrowDataType *data_type,
                                       GArrowDataType *other_data_type);
gchar     *garrow_data_type_to_string (GArrowDataType *data_type);
GArrowType garrow_data_type_get_id    (GArrowDataType *data_type);
GARROW_AVAILABLE_IN_3_0
gchar *
garrow_data_type_get_name(GArrowDataType *data_type);


#define GARROW_TYPE_FIXED_WIDTH_DATA_TYPE (garrow_fixed_width_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFixedWidthDataType,
                         garrow_fixed_width_data_type,
                         GARROW,
                         FIXED_WIDTH_DATA_TYPE,
                         GArrowDataType)
struct _GArrowFixedWidthDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

gint garrow_fixed_width_data_type_get_bit_width(GArrowFixedWidthDataType *data_type);
/* TODO:
GList *garrow_fixed_width_data_type_get_buffer_layout(GArrowFixedWidthDataType *data_type);
*/


#define GARROW_TYPE_NULL_DATA_TYPE              \
  (garrow_null_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowNullDataType,
                         garrow_null_data_type,
                         GARROW,
                         NULL_DATA_TYPE,
                         GArrowDataType)
struct _GArrowNullDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GArrowNullDataType *garrow_null_data_type_new      (void);


#define GARROW_TYPE_BOOLEAN_DATA_TYPE (garrow_boolean_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBooleanDataType,
                         garrow_boolean_data_type,
                         GARROW,
                         BOOLEAN_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowBooleanDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GArrowBooleanDataType *garrow_boolean_data_type_new      (void);


#define GARROW_TYPE_NUMERIC_DATA_TYPE (garrow_numeric_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowNumericDataType,
                         garrow_numeric_data_type,
                         GARROW,
                         NUMERIC_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowNumericDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};


#define GARROW_TYPE_INTEGER_DATA_TYPE (garrow_integer_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowIntegerDataType,
                         garrow_integer_data_type,
                         GARROW,
                         INTEGER_DATA_TYPE,
                         GArrowNumericDataType)
struct _GArrowIntegerDataTypeClass
{
  GArrowNumericDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
gboolean garrow_integer_data_type_is_signed(GArrowIntegerDataType *data_type);

#define GARROW_TYPE_INT8_DATA_TYPE (garrow_int8_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt8DataType,
                         garrow_int8_data_type,
                         GARROW,
                         INT8_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt8DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt8DataType   *garrow_int8_data_type_new      (void);


#define GARROW_TYPE_UINT8_DATA_TYPE (garrow_uint8_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt8DataType,
                         garrow_uint8_data_type,
                         GARROW,
                         UINT8_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt8DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt8DataType  *garrow_uint8_data_type_new      (void);


#define GARROW_TYPE_INT16_DATA_TYPE (garrow_int16_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt16DataType,
                         garrow_int16_data_type,
                         GARROW,
                         INT16_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt16DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt16DataType  *garrow_int16_data_type_new      (void);


#define GARROW_TYPE_UINT16_DATA_TYPE (garrow_uint16_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt16DataType,
                         garrow_uint16_data_type,
                         GARROW,
                         UINT16_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt16DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt16DataType *garrow_uint16_data_type_new      (void);


#define GARROW_TYPE_INT32_DATA_TYPE (garrow_int32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt32DataType,
                         garrow_int32_data_type,
                         GARROW,
                         INT32_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt32DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt32DataType  *garrow_int32_data_type_new      (void);


#define GARROW_TYPE_UINT32_DATA_TYPE (garrow_uint32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt32DataType,
                         garrow_uint32_data_type,
                         GARROW,
                         UINT32_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt32DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt32DataType *garrow_uint32_data_type_new      (void);


#define GARROW_TYPE_INT64_DATA_TYPE (garrow_int64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowInt64DataType,
                         garrow_int64_data_type,
                         GARROW,
                         INT64_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowInt64DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowInt64DataType  *garrow_int64_data_type_new      (void);


#define GARROW_TYPE_UINT64_DATA_TYPE (garrow_uint64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUInt64DataType,
                         garrow_uint64_data_type,
                         GARROW,
                         UINT64_DATA_TYPE,
                         GArrowIntegerDataType)
struct _GArrowUInt64DataTypeClass
{
  GArrowIntegerDataTypeClass parent_class;
};

GArrowUInt64DataType *garrow_uint64_data_type_new      (void);


#define GARROW_TYPE_FLOATING_POINT_DATA_TYPE    \
  (garrow_floating_point_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatingPointDataType,
                         garrow_floating_point_data_type,
                         GARROW,
                         FLOATING_POINT_DATA_TYPE,
                         GArrowNumericDataType)
struct _GArrowFloatingPointDataTypeClass
{
  GArrowNumericDataTypeClass parent_class;
};


#define GARROW_TYPE_HALF_FLOAT_DATA_TYPE (garrow_half_float_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowHalfFloatDataType,
                         garrow_half_float_data_type,
                         GARROW,
                         HALF_FLOAT_DATA_TYPE,
                         GArrowFloatingPointDataType)
struct _GArrowHalfFloatDataTypeClass
{
  GArrowFloatingPointDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_11_0
GArrowHalfFloatDataType *garrow_half_float_data_type_new(void);


#define GARROW_TYPE_FLOAT_DATA_TYPE (garrow_float_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFloatDataType,
                         garrow_float_data_type,
                         GARROW,
                         FLOAT_DATA_TYPE,
                         GArrowFloatingPointDataType)
struct _GArrowFloatDataTypeClass
{
  GArrowFloatingPointDataTypeClass parent_class;
};

GArrowFloatDataType *garrow_float_data_type_new      (void);


#define GARROW_TYPE_DOUBLE_DATA_TYPE (garrow_double_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDoubleDataType,
                         garrow_double_data_type,
                         GARROW,
                         DOUBLE_DATA_TYPE,
                         GArrowFloatingPointDataType)
struct _GArrowDoubleDataTypeClass
{
  GArrowFloatingPointDataTypeClass parent_class;
};

GArrowDoubleDataType *garrow_double_data_type_new      (void);


#define GARROW_TYPE_BINARY_DATA_TYPE (garrow_binary_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowBinaryDataType,
                         garrow_binary_data_type,
                         GARROW,
                         BINARY_DATA_TYPE,
                         GArrowDataType)
struct _GArrowBinaryDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GArrowBinaryDataType *garrow_binary_data_type_new      (void);


#define GARROW_TYPE_FIXED_SIZE_BINARY_DATA_TYPE (garrow_fixed_size_binary_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFixedSizeBinaryDataType,
                         garrow_fixed_size_binary_data_type,
                         GARROW,
                         FIXED_SIZE_BINARY_DATA_TYPE,
                         GArrowDataType)
struct _GArrowFixedSizeBinaryDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_12
GArrowFixedSizeBinaryDataType *
garrow_fixed_size_binary_data_type_new(gint32 byte_width);
GARROW_AVAILABLE_IN_0_12
gint32
garrow_fixed_size_binary_data_type_get_byte_width(GArrowFixedSizeBinaryDataType *data_type);


#define GARROW_TYPE_LARGE_BINARY_DATA_TYPE (garrow_large_binary_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeBinaryDataType,
                         garrow_large_binary_data_type,
                         GARROW,
                         LARGE_BINARY_DATA_TYPE,
                         GArrowDataType)
struct _GArrowLargeBinaryDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowLargeBinaryDataType *garrow_large_binary_data_type_new(void);


#define GARROW_TYPE_STRING_DATA_TYPE (garrow_string_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStringDataType,
                         garrow_string_data_type,
                         GARROW,
                         STRING_DATA_TYPE,
                         GArrowBinaryDataType)
struct _GArrowStringDataTypeClass
{
  GArrowBinaryDataTypeClass parent_class;
};

GArrowStringDataType *garrow_string_data_type_new      (void);


#define GARROW_TYPE_LARGE_STRING_DATA_TYPE (garrow_large_string_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowLargeStringDataType,
                         garrow_large_string_data_type,
                         GARROW,
                         LARGE_STRING_DATA_TYPE,
                         GArrowLargeBinaryDataType)
struct _GArrowLargeStringDataTypeClass
{
  GArrowLargeBinaryDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowLargeStringDataType *garrow_large_string_data_type_new(void);


#define GARROW_TYPE_TEMPORAL_DATA_TYPE (garrow_temporal_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTemporalDataType,
                         garrow_temporal_data_type,
                         GARROW,
                         TEMPORAL_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowTemporalDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};


#define GARROW_TYPE_DATE32_DATA_TYPE (garrow_date32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate32DataType,
                         garrow_date32_data_type,
                         GARROW,
                         DATE32_DATA_TYPE,
                         GArrowTemporalDataType)
struct _GArrowDate32DataTypeClass
{
  GArrowTemporalDataTypeClass parent_class;
};

GArrowDate32DataType *garrow_date32_data_type_new      (void);


#define GARROW_TYPE_DATE64_DATA_TYPE (garrow_date64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDate64DataType,
                         garrow_date64_data_type,
                         GARROW,
                         DATE64_DATA_TYPE,
                         GArrowTemporalDataType)
struct _GArrowDate64DataTypeClass
{
  GArrowTemporalDataTypeClass parent_class;
};

GArrowDate64DataType *garrow_date64_data_type_new      (void);


#define GARROW_TYPE_TIMESTAMP_DATA_TYPE (garrow_timestamp_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTimestampDataType,
                         garrow_timestamp_data_type,
                         GARROW,
                         TIMESTAMP_DATA_TYPE,
                         GArrowTemporalDataType)
struct _GArrowTimestampDataTypeClass
{
  GArrowTemporalDataTypeClass parent_class;
};

GArrowTimestampDataType *garrow_timestamp_data_type_new   (GArrowTimeUnit unit);
GArrowTimeUnit
garrow_timestamp_data_type_get_unit (GArrowTimestampDataType *timestamp_data_type);


#define GARROW_TYPE_TIME_DATA_TYPE (garrow_time_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTimeDataType,
                         garrow_time_data_type,
                         GARROW,
                         TIME_DATA_TYPE,
                         GArrowTemporalDataType)
struct _GArrowTimeDataTypeClass
{
  GArrowTemporalDataTypeClass parent_class;
};

GArrowTimeUnit garrow_time_data_type_get_unit (GArrowTimeDataType *time_data_type);


#define GARROW_TYPE_TIME32_DATA_TYPE (garrow_time32_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime32DataType,
                         garrow_time32_data_type,
                         GARROW,
                         TIME32_DATA_TYPE,
                         GArrowTimeDataType)
struct _GArrowTime32DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GArrowTime32DataType *garrow_time32_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_TIME64_DATA_TYPE (garrow_time64_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTime64DataType,
                         garrow_time64_data_type,
                         GARROW,
                         TIME64_DATA_TYPE,
                         GArrowTimeDataType)
struct _GArrowTime64DataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GArrowTime64DataType *garrow_time64_data_type_new      (GArrowTimeUnit unit,
                                                        GError **error);


#define GARROW_TYPE_INTERVAL_DATA_TYPE (garrow_interval_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowIntervalDataType,
                         garrow_interval_data_type,
                         GARROW,
                         INTERVAL_DATA_TYPE,
                         GArrowTimeDataType)
struct _GArrowIntervalDataTypeClass
{
  GArrowTimeDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowIntervalType
garrow_interval_data_type_get_interval_type(GArrowIntervalDataType *type);


#define GARROW_TYPE_MONTH_INTERVAL_DATA_TYPE   \
  (garrow_month_interval_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthIntervalDataType,
                         garrow_month_interval_data_type,
                         GARROW,
                         MONTH_INTERVAL_DATA_TYPE,
                         GArrowIntervalDataType)
struct _GArrowMonthIntervalDataTypeClass
{
  GArrowIntervalDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowMonthIntervalDataType *
garrow_month_interval_data_type_new(void);


#define GARROW_TYPE_DAY_TIME_INTERVAL_DATA_TYPE         \
  (garrow_day_time_interval_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDayTimeIntervalDataType,
                         garrow_day_time_interval_data_type,
                         GARROW,
                         DAY_TIME_INTERVAL_DATA_TYPE,
                         GArrowIntervalDataType)
struct _GArrowDayTimeIntervalDataTypeClass
{
  GArrowIntervalDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowDayTimeIntervalDataType *
garrow_day_time_interval_data_type_new(void);


#define GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_DATA_TYPE \
  (garrow_month_day_nano_interval_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowMonthDayNanoIntervalDataType,
                         garrow_month_day_nano_interval_data_type,
                         GARROW,
                         MONTH_DAY_NANO_INTERVAL_DATA_TYPE,
                         GArrowIntervalDataType)
struct _GArrowMonthDayNanoIntervalDataTypeClass
{
  GArrowIntervalDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_7_0
GArrowMonthDayNanoIntervalDataType *
garrow_month_day_nano_interval_data_type_new(void);


#define GARROW_TYPE_DECIMAL_DATA_TYPE (garrow_decimal_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimalDataType,
                         garrow_decimal_data_type,
                         GARROW,
                         DECIMAL_DATA_TYPE,
                         GArrowFixedSizeBinaryDataType)
struct _GArrowDecimalDataTypeClass
{
  GArrowFixedSizeBinaryDataTypeClass parent_class;
};

GArrowDecimalDataType *
garrow_decimal_data_type_new(gint32 precision, gint32 scale, GError **error);
gint32 garrow_decimal_data_type_get_precision(GArrowDecimalDataType *decimal_data_type);
gint32 garrow_decimal_data_type_get_scale(GArrowDecimalDataType *decimal_data_type);


#define GARROW_TYPE_DECIMAL128_DATA_TYPE (garrow_decimal128_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal128DataType,
                         garrow_decimal128_data_type,
                         GARROW,
                         DECIMAL128_DATA_TYPE,
                         GArrowDecimalDataType)
struct _GArrowDecimal128DataTypeClass
{
  GArrowDecimalDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
gint32
garrow_decimal128_data_type_max_precision();

GARROW_AVAILABLE_IN_0_12
GArrowDecimal128DataType *
garrow_decimal128_data_type_new(gint32 precision, gint32 scale, GError **error);


#define GARROW_TYPE_DECIMAL256_DATA_TYPE (garrow_decimal256_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDecimal256DataType,
                         garrow_decimal256_data_type,
                         GARROW,
                         DECIMAL256_DATA_TYPE,
                         GArrowDecimalDataType)
struct _GArrowDecimal256DataTypeClass
{
  GArrowDecimalDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
gint32
garrow_decimal256_data_type_max_precision();

GARROW_AVAILABLE_IN_3_0
GArrowDecimal256DataType *
garrow_decimal256_data_type_new(gint32 precision, gint32 scale, GError **error);

#define GARROW_TYPE_EXTENSION_DATA_TYPE (garrow_extension_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExtensionDataType,
                         garrow_extension_data_type,
                         GARROW,
                         EXTENSION_DATA_TYPE,
                         GArrowDataType)
/**
 * GArrowExtensionDataTypeClass:
 * @get_extension_name: It must returns the name of this extension data type.
 * @equal: It must returns %TRUE only when the both data types equal, %FALSE
 *   otherwise.
 * @deserialize: It must returns a serialized #GArrowDataType from the given
 *   `serialized_data`.
 * @serialize: It must returns a serialized data of this extension data type
 *   to deserialize later.
 * @get_array_gtype: It must returns #GType for corresponding extension array
 *   class.
 *
 * Since: 3.0.0
 */
struct _GArrowExtensionDataTypeClass
{
  GArrowDataTypeClass parent_class;

  gchar *(*get_extension_name)(GArrowExtensionDataType *data_type);
  gboolean (*equal)(GArrowExtensionDataType *data_type,
                    GArrowExtensionDataType *other_data_type);
  GArrowDataType *(*deserialize)(GArrowExtensionDataType *data_type,
                                 GArrowDataType *storage_data_type,
                                 GBytes *serialized_data,
                                 GError **error);
  GBytes *(*serialize)(GArrowExtensionDataType *data_type);
  GType (*get_array_gtype)(GArrowExtensionDataType *data_type);
};

GARROW_AVAILABLE_IN_3_0
gchar *
garrow_extension_data_type_get_extension_name(GArrowExtensionDataType *data_type);

GARROW_AVAILABLE_IN_3_0
GArrowExtensionArray *
garrow_extension_data_type_wrap_array(GArrowExtensionDataType *data_type,
                                      GArrowArray *storage);

GARROW_AVAILABLE_IN_3_0
GArrowChunkedArray *
garrow_extension_data_type_wrap_chunked_array(GArrowExtensionDataType *data_type,
                                              GArrowChunkedArray *storage);


#define GARROW_TYPE_EXTENSION_DATA_TYPE_REGISTRY        \
  (garrow_extension_data_type_registry_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowExtensionDataTypeRegistry,
                         garrow_extension_data_type_registry,
                         GARROW,
                         EXTENSION_DATA_TYPE_REGISTRY,
                         GObject)
struct _GArrowExtensionDataTypeRegistryClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_3_0
GArrowExtensionDataTypeRegistry *
garrow_extension_data_type_registry_default(void);

GARROW_AVAILABLE_IN_3_0
gboolean
garrow_extension_data_type_registry_register(
  GArrowExtensionDataTypeRegistry *registry,
  GArrowExtensionDataType *data_type,
  GError **error);
GARROW_AVAILABLE_IN_3_0
gboolean
garrow_extension_data_type_registry_unregister(
  GArrowExtensionDataTypeRegistry *registry,
  const gchar *name,
  GError **error);
GARROW_AVAILABLE_IN_3_0
GArrowExtensionDataType *
garrow_extension_data_type_registry_lookup(
  GArrowExtensionDataTypeRegistry *registry,
  const gchar *name);

G_END_DECLS
