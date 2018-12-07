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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/data-type.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: basic-data-type
 * @section_id: basic-data-type-classes
 * @title: Basic data type classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowDataType is a base class for all data type classes such as
 * #GArrowBooleanDataType.
 *
 * #GArrowNullDataType is a class for null data type.
 *
 * #GArrowBooleanDataType is a class for boolean data type.
 *
 * #GArrowInt8DataType is a class for 8-bit integer data type.
 *
 * #GArrowUInt8DataType is a class for 8-bit unsigned integer data type.
 *
 * #GArrowInt16DataType is a class for 16-bit integer data type.
 *
 * #GArrowUInt16DataType is a class for 16-bit unsigned integer data type.
 *
 * #GArrowInt32DataType is a class for 32-bit integer data type.
 *
 * #GArrowUInt32DataType is a class for 32-bit unsigned integer data type.
 *
 * #GArrowInt64DataType is a class for 64-bit integer data type.
 *
 * #GArrowUInt64DataType is a class for 64-bit unsigned integer data type.
 *
 * #GArrowFloatDataType is a class for 32-bit floating point data
 * type.
 *
 * #GArrowDoubleDataType is a class for 64-bit floating point data
 * type.
 *
 * #GArrowBinaryDataType is a class for binary data type.
 *
 * #GArrowStringDataType is a class for UTF-8 encoded string data
 * type.
 *
 * #GArrowDate32DataType is a class for the number of days since UNIX
 * epoch in 32-bit signed integer data type.
 *
 * #GArrowDate64DataType is a class for the number of milliseconds
 * since UNIX epoch in 64-bit signed integer data type.
 *
 * #GArrowTimestampDataType is a class for the number of
 * seconds/milliseconds/microseconds/nanoseconds since UNIX epoch in
 * 64-bit signed integer data type.
 *
 * #GArrowTime32DataType is a class for the number of seconds or
 * milliseconds since midnight in 32-bit signed integer data type.
 *
 * #GArrowTime64DataType is a class for the number of microseconds or
 * nanoseconds since midnight in 64-bit signed integer data type.
 *
 * #GArrowDecimalDataType is a class for 128-bit decimal data type.
 */

typedef struct GArrowDataTypePrivate_ {
  std::shared_ptr<arrow::DataType> data_type;
} GArrowDataTypePrivate;

enum {
  PROP_0,
  PROP_DATA_TYPE
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowDataType,
                                    garrow_data_type,
                                    G_TYPE_OBJECT)

#define GARROW_DATA_TYPE_GET_PRIVATE(obj)         \
  static_cast<GArrowDataTypePrivate *>(           \
     garrow_data_type_get_instance_private(       \
       GARROW_DATA_TYPE(obj)))

static void
garrow_data_type_finalize(GObject *object)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  priv->data_type = nullptr;

  G_OBJECT_CLASS(garrow_data_type_parent_class)->finalize(object);
}

static void
garrow_data_type_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA_TYPE:
    priv->data_type =
      *static_cast<std::shared_ptr<arrow::DataType> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_data_type_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_data_type_init(GArrowDataType *object)
{
}

static void
garrow_data_type_class_init(GArrowDataTypeClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_data_type_finalize;
  gobject_class->set_property = garrow_data_type_set_property;
  gobject_class->get_property = garrow_data_type_get_property;

  spec = g_param_spec_pointer("data-type",
                              "DataType",
                              "The raw std::shared<arrow::DataType> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_data_type_equal:
 * @data_type: A #GArrowDataType.
 * @other_data_type: A #GArrowDataType to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 */
gboolean
garrow_data_type_equal(GArrowDataType *data_type,
                       GArrowDataType *other_data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_other_data_type = garrow_data_type_get_raw(other_data_type);
  return arrow_data_type->Equals(arrow_other_data_type);
}

/**
 * garrow_data_type_to_string:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The string representation of the data type. The caller
 *   must free it by g_free() when the caller doesn't need it anymore.
 */
gchar *
garrow_data_type_to_string(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  return g_strdup(arrow_data_type->ToString().c_str());
}

/**
 * garrow_data_type_get_id:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The #GArrowType of the data type.
 */
GArrowType
garrow_data_type_get_id(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  return garrow_type_from_raw(arrow_data_type->id());
}


G_DEFINE_ABSTRACT_TYPE(GArrowFixedWidthDataType,
                       garrow_fixed_width_data_type,
                       GARROW_TYPE_DATA_TYPE)

static void
garrow_fixed_width_data_type_init(GArrowFixedWidthDataType *object)
{
}

static void
garrow_fixed_width_data_type_class_init(GArrowFixedWidthDataTypeClass *klass)
{
}

/**
 * garrow_fixed_width_data_type_get_id:
 * @data_type: A #GArrowFixedWidthDataType.
 *
 * Returns: The number of bits for one data.
 */
gint
garrow_fixed_width_data_type_get_bit_width(GArrowFixedWidthDataType *data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  const auto arrow_fixed_width_type =
    std::static_pointer_cast<arrow::FixedWidthType>(arrow_data_type);
  return arrow_fixed_width_type->bit_width();
}


G_DEFINE_TYPE(GArrowNullDataType,
              garrow_null_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_null_data_type_init(GArrowNullDataType *object)
{
}

static void
garrow_null_data_type_class_init(GArrowNullDataTypeClass *klass)
{
}

/**
 * garrow_null_data_type_new:
 *
 * Returns: The newly created null data type.
 */
GArrowNullDataType *
garrow_null_data_type_new(void)
{
  auto arrow_data_type = arrow::null();

  GArrowNullDataType *data_type =
    GARROW_NULL_DATA_TYPE(g_object_new(GARROW_TYPE_NULL_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowBooleanDataType,
              garrow_boolean_data_type,
              GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_boolean_data_type_init(GArrowBooleanDataType *object)
{
}

static void
garrow_boolean_data_type_class_init(GArrowBooleanDataTypeClass *klass)
{
}

/**
 * garrow_boolean_data_type_new:
 *
 * Returns: The newly created boolean data type.
 */
GArrowBooleanDataType *
garrow_boolean_data_type_new(void)
{
  auto arrow_data_type = arrow::boolean();

  GArrowBooleanDataType *data_type =
    GARROW_BOOLEAN_DATA_TYPE(g_object_new(GARROW_TYPE_BOOLEAN_DATA_TYPE,
                                          "data-type", &arrow_data_type,
                                          NULL));
  return data_type;
}


G_DEFINE_ABSTRACT_TYPE(GArrowNumericDataType,
                       garrow_numeric_data_type,
                       GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_numeric_data_type_init(GArrowNumericDataType *object)
{
}

static void
garrow_numeric_data_type_class_init(GArrowNumericDataTypeClass *klass)
{
}


G_DEFINE_ABSTRACT_TYPE(GArrowIntegerDataType,
                       garrow_integer_data_type,
                       GARROW_TYPE_NUMERIC_DATA_TYPE)

static void
garrow_integer_data_type_init(GArrowIntegerDataType *object)
{
}

static void
garrow_integer_data_type_class_init(GArrowIntegerDataTypeClass *klass)
{
}


G_DEFINE_TYPE(GArrowInt8DataType,
              garrow_int8_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_int8_data_type_init(GArrowInt8DataType *object)
{
}

static void
garrow_int8_data_type_class_init(GArrowInt8DataTypeClass *klass)
{
}

/**
 * garrow_int8_data_type_new:
 *
 * Returns: The newly created 8-bit integer data type.
 */
GArrowInt8DataType *
garrow_int8_data_type_new(void)
{
  auto arrow_data_type = arrow::int8();

  GArrowInt8DataType *data_type =
    GARROW_INT8_DATA_TYPE(g_object_new(GARROW_TYPE_INT8_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt8DataType,
              garrow_uint8_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_uint8_data_type_init(GArrowUInt8DataType *object)
{
}

static void
garrow_uint8_data_type_class_init(GArrowUInt8DataTypeClass *klass)
{
}

/**
 * garrow_uint8_data_type_new:
 *
 * Returns: The newly created 8-bit unsigned integer data type.
 */
GArrowUInt8DataType *
garrow_uint8_data_type_new(void)
{
  auto arrow_data_type = arrow::uint8();

  GArrowUInt8DataType *data_type =
    GARROW_UINT8_DATA_TYPE(g_object_new(GARROW_TYPE_UINT8_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt16DataType,
              garrow_int16_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_int16_data_type_init(GArrowInt16DataType *object)
{
}

static void
garrow_int16_data_type_class_init(GArrowInt16DataTypeClass *klass)
{
}

/**
 * garrow_int16_data_type_new:
 *
 * Returns: The newly created 16-bit integer data type.
 */
GArrowInt16DataType *
garrow_int16_data_type_new(void)
{
  auto arrow_data_type = arrow::int16();

  GArrowInt16DataType *data_type =
    GARROW_INT16_DATA_TYPE(g_object_new(GARROW_TYPE_INT16_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt16DataType,
              garrow_uint16_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_uint16_data_type_init(GArrowUInt16DataType *object)
{
}

static void
garrow_uint16_data_type_class_init(GArrowUInt16DataTypeClass *klass)
{
}

/**
 * garrow_uint16_data_type_new:
 *
 * Returns: The newly created 16-bit unsigned integer data type.
 */
GArrowUInt16DataType *
garrow_uint16_data_type_new(void)
{
  auto arrow_data_type = arrow::uint16();

  GArrowUInt16DataType *data_type =
    GARROW_UINT16_DATA_TYPE(g_object_new(GARROW_TYPE_UINT16_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt32DataType,
              garrow_int32_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_int32_data_type_init(GArrowInt32DataType *object)
{
}

static void
garrow_int32_data_type_class_init(GArrowInt32DataTypeClass *klass)
{
}

/**
 * garrow_int32_data_type_new:
 *
 * Returns: The newly created 32-bit integer data type.
 */
GArrowInt32DataType *
garrow_int32_data_type_new(void)
{
  auto arrow_data_type = arrow::int32();

  GArrowInt32DataType *data_type =
    GARROW_INT32_DATA_TYPE(g_object_new(GARROW_TYPE_INT32_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt32DataType,
              garrow_uint32_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_uint32_data_type_init(GArrowUInt32DataType *object)
{
}

static void
garrow_uint32_data_type_class_init(GArrowUInt32DataTypeClass *klass)
{
}

/**
 * garrow_uint32_data_type_new:
 *
 * Returns: The newly created 32-bit unsigned integer data type.
 */
GArrowUInt32DataType *
garrow_uint32_data_type_new(void)
{
  auto arrow_data_type = arrow::uint32();

  GArrowUInt32DataType *data_type =
    GARROW_UINT32_DATA_TYPE(g_object_new(GARROW_TYPE_UINT32_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowInt64DataType,
              garrow_int64_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_int64_data_type_init(GArrowInt64DataType *object)
{
}

static void
garrow_int64_data_type_class_init(GArrowInt64DataTypeClass *klass)
{
}

/**
 * garrow_int64_data_type_new:
 *
 * Returns: The newly created 64-bit integer data type.
 */
GArrowInt64DataType *
garrow_int64_data_type_new(void)
{
  auto arrow_data_type = arrow::int64();

  GArrowInt64DataType *data_type =
    GARROW_INT64_DATA_TYPE(g_object_new(GARROW_TYPE_INT64_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowUInt64DataType,
              garrow_uint64_data_type,
              GARROW_TYPE_INTEGER_DATA_TYPE)

static void
garrow_uint64_data_type_init(GArrowUInt64DataType *object)
{
}

static void
garrow_uint64_data_type_class_init(GArrowUInt64DataTypeClass *klass)
{
}

/**
 * garrow_uint64_data_type_new:
 *
 * Returns: The newly created 64-bit unsigned integer data type.
 */
GArrowUInt64DataType *
garrow_uint64_data_type_new(void)
{
  auto arrow_data_type = arrow::uint64();

  GArrowUInt64DataType *data_type =
    GARROW_UINT64_DATA_TYPE(g_object_new(GARROW_TYPE_UINT64_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_ABSTRACT_TYPE(GArrowFloatingPointDataType,
                       garrow_floating_point_data_type,
                       GARROW_TYPE_NUMERIC_DATA_TYPE)

static void
garrow_floating_point_data_type_init(GArrowFloatingPointDataType *object)
{
}

static void
garrow_floating_point_data_type_class_init(GArrowFloatingPointDataTypeClass *klass)
{
}


G_DEFINE_TYPE(GArrowFloatDataType,
              garrow_float_data_type,
              GARROW_TYPE_FLOATING_POINT_DATA_TYPE)

static void
garrow_float_data_type_init(GArrowFloatDataType *object)
{
}

static void
garrow_float_data_type_class_init(GArrowFloatDataTypeClass *klass)
{
}

/**
 * garrow_float_data_type_new:
 *
 * Returns: The newly created float data type.
 */
GArrowFloatDataType *
garrow_float_data_type_new(void)
{
  auto arrow_data_type = arrow::float32();

  GArrowFloatDataType *data_type =
    GARROW_FLOAT_DATA_TYPE(g_object_new(GARROW_TYPE_FLOAT_DATA_TYPE,
                                        "data-type", &arrow_data_type,
                                        NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDoubleDataType,
              garrow_double_data_type,
              GARROW_TYPE_FLOATING_POINT_DATA_TYPE)

static void
garrow_double_data_type_init(GArrowDoubleDataType *object)
{
}

static void
garrow_double_data_type_class_init(GArrowDoubleDataTypeClass *klass)
{
}

/**
 * garrow_double_data_type_new:
 *
 * Returns: The newly created 64-bit floating point data type.
 */
GArrowDoubleDataType *
garrow_double_data_type_new(void)
{
  auto arrow_data_type = arrow::float64();

  GArrowDoubleDataType *data_type =
    GARROW_DOUBLE_DATA_TYPE(g_object_new(GARROW_TYPE_DOUBLE_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowBinaryDataType,
              garrow_binary_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_binary_data_type_init(GArrowBinaryDataType *object)
{
}

static void
garrow_binary_data_type_class_init(GArrowBinaryDataTypeClass *klass)
{
}

/**
 * garrow_binary_data_type_new:
 *
 * Returns: The newly created binary data type.
 */
GArrowBinaryDataType *
garrow_binary_data_type_new(void)
{
  auto arrow_data_type = arrow::binary();

  GArrowBinaryDataType *data_type =
    GARROW_BINARY_DATA_TYPE(g_object_new(GARROW_TYPE_BINARY_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowStringDataType,
              garrow_string_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_string_data_type_init(GArrowStringDataType *object)
{
}

static void
garrow_string_data_type_class_init(GArrowStringDataTypeClass *klass)
{
}

/**
 * garrow_string_data_type_new:
 *
 * Returns: The newly created UTF-8 encoded string data type.
 */
GArrowStringDataType *
garrow_string_data_type_new(void)
{
  auto arrow_data_type = arrow::utf8();

  GArrowStringDataType *data_type =
    GARROW_STRING_DATA_TYPE(g_object_new(GARROW_TYPE_STRING_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDate32DataType,
              garrow_date32_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_date32_data_type_init(GArrowDate32DataType *object)
{
}

static void
garrow_date32_data_type_class_init(GArrowDate32DataTypeClass *klass)
{
}

/**
 * garrow_date32_data_type_new:
 *
 * Returns: A newly created the number of milliseconds
 *   since UNIX epoch in 32-bit signed integer data type.
 *
 * Since: 0.7.0
 */
GArrowDate32DataType *
garrow_date32_data_type_new(void)
{
  auto arrow_data_type = arrow::date32();

  GArrowDate32DataType *data_type =
    GARROW_DATE32_DATA_TYPE(g_object_new(GARROW_TYPE_DATE32_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDate64DataType,
              garrow_date64_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_date64_data_type_init(GArrowDate64DataType *object)
{
}

static void
garrow_date64_data_type_class_init(GArrowDate64DataTypeClass *klass)
{
}

/**
 * garrow_date64_data_type_new:
 *
 * Returns: A newly created the number of milliseconds
 *   since UNIX epoch in 64-bit signed integer data type.
 *
 * Since: 0.7.0
 */
GArrowDate64DataType *
garrow_date64_data_type_new(void)
{
  auto arrow_data_type = arrow::date64();

  GArrowDate64DataType *data_type =
    GARROW_DATE64_DATA_TYPE(g_object_new(GARROW_TYPE_DATE64_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowTimestampDataType,
              garrow_timestamp_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_timestamp_data_type_init(GArrowTimestampDataType *object)
{
}

static void
garrow_timestamp_data_type_class_init(GArrowTimestampDataTypeClass *klass)
{
}

/**
 * garrow_timestamp_data_type_new:
 * @unit: The unit of the timestamp data.
 *
 * Returns: A newly created the number of
 *   seconds/milliseconds/microseconds/nanoseconds since UNIX epoch in
 *   64-bit signed integer data type.
 *
 * Since: 0.7.0
 */
GArrowTimestampDataType *
garrow_timestamp_data_type_new(GArrowTimeUnit unit)
{
  auto arrow_unit = garrow_time_unit_to_raw(unit);
  auto arrow_data_type = arrow::timestamp(arrow_unit);
  auto data_type =
    GARROW_TIMESTAMP_DATA_TYPE(g_object_new(GARROW_TYPE_TIMESTAMP_DATA_TYPE,
                                            "data-type", &arrow_data_type,
                                            NULL));
  return data_type;
}

/**
 * garrow_timestamp_data_type_get_unit:
 * @timestamp_data_type: The #GArrowTimestampDataType.
 *
 * Returns: The unit of the timestamp data type.
 *
 * Since: 0.8.0
 */
GArrowTimeUnit
garrow_timestamp_data_type_get_unit(GArrowTimestampDataType *timestamp_data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(timestamp_data_type));
  const auto arrow_timestamp_data_type =
    std::static_pointer_cast<arrow::TimestampType>(arrow_data_type);
  return garrow_time_unit_from_raw(arrow_timestamp_data_type->unit());
}


G_DEFINE_TYPE(GArrowTimeDataType,
              garrow_time_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_time_data_type_init(GArrowTimeDataType *object)
{
}

static void
garrow_time_data_type_class_init(GArrowTimeDataTypeClass *klass)
{
}

/**
 * garrow_time_data_type_get_unit:
 * @time_data_type: The #GArrowTimeDataType.
 *
 * Returns: The unit of the time data type.
 *
 * Since: 0.7.0
 */
GArrowTimeUnit
garrow_time_data_type_get_unit(GArrowTimeDataType *time_data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(time_data_type));
  const auto arrow_time_data_type =
    std::static_pointer_cast<arrow::TimeType>(arrow_data_type);
  return garrow_time_unit_from_raw(arrow_time_data_type->unit());
}


G_DEFINE_TYPE(GArrowTime32DataType,
              garrow_time32_data_type,
              GARROW_TYPE_TIME_DATA_TYPE)

static void
garrow_time32_data_type_init(GArrowTime32DataType *object)
{
}

static void
garrow_time32_data_type_class_init(GArrowTime32DataTypeClass *klass)
{
}

/**
 * garrow_time32_data_type_new:
 * @unit: %GARROW_TIME_UNIT_SECOND or %GARROW_TIME_UNIT_MILLI.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created the number of seconds or milliseconds since
 *   midnight in 32-bit signed integer data type.
 *
 * Since: 0.7.0
 */
GArrowTime32DataType *
garrow_time32_data_type_new(GArrowTimeUnit unit, GError **error)
{
  switch (unit) {
  case GARROW_TIME_UNIT_SECOND:
  case GARROW_TIME_UNIT_MILLI:
    break;
  default:
    {
      auto enum_class = G_ENUM_CLASS(g_type_class_ref(GARROW_TYPE_TIME_UNIT));
      GEnumValue *value = g_enum_get_value(enum_class, unit);
      if (value) {
        g_set_error(error,
                    GARROW_ERROR,
                    GARROW_ERROR_INVALID,
                    "[time32-data-type][new] time unit must be second or milli: "
                    "<%s>",
                    value->value_nick);
      } else {
        g_set_error(error,
                    GARROW_ERROR,
                    GARROW_ERROR_INVALID,
                    "[time32-data-type][new] "
                    "time unit must be second(%d) or milli(%d): <%d>",
                    GARROW_TIME_UNIT_SECOND,
                    GARROW_TIME_UNIT_MILLI,
                    unit);
      }
      g_type_class_unref(enum_class);
    }
    return NULL;
  }

  auto arrow_unit = garrow_time_unit_to_raw(unit);
  auto arrow_data_type = arrow::time32(arrow_unit);
  auto data_type =
    GARROW_TIME32_DATA_TYPE(g_object_new(GARROW_TYPE_TIME32_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowTime64DataType,
              garrow_time64_data_type,
              GARROW_TYPE_TIME_DATA_TYPE)

static void
garrow_time64_data_type_init(GArrowTime64DataType *object)
{
}

static void
garrow_time64_data_type_class_init(GArrowTime64DataTypeClass *klass)
{
}

/**
 * garrow_time64_data_type_new:
 * @unit: %GARROW_TIME_UNIT_SECOND or %GARROW_TIME_UNIT_MILLI.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   A newly created the number of seconds or milliseconds since
 *   midnight in 64-bit signed integer data type.
 *
 * Since: 0.7.0
 */
GArrowTime64DataType *
garrow_time64_data_type_new(GArrowTimeUnit unit, GError **error)
{
  switch (unit) {
  case GARROW_TIME_UNIT_MICRO:
  case GARROW_TIME_UNIT_NANO:
    break;
  default:
    {
      auto enum_class = G_ENUM_CLASS(g_type_class_ref(GARROW_TYPE_TIME_UNIT));
      auto value = g_enum_get_value(enum_class, unit);
      if (value) {
        g_set_error(error,
                    GARROW_ERROR,
                    GARROW_ERROR_INVALID,
                    "[time64-data-type][new] time unit must be micro or nano: "
                    "<%s>",
                    value->value_nick);
      } else {
        g_set_error(error,
                    GARROW_ERROR,
                    GARROW_ERROR_INVALID,
                    "[time64-data-type][new] "
                    "time unit must be micro(%d) or nano(%d): <%d>",
                    GARROW_TIME_UNIT_MICRO,
                    GARROW_TIME_UNIT_NANO,
                    unit);
      }
      g_type_class_unref(enum_class);
    }
    return NULL;
  }

  auto arrow_unit = garrow_time_unit_to_raw(unit);
  auto arrow_data_type = arrow::time64(arrow_unit);
  auto data_type =
    GARROW_TIME64_DATA_TYPE(g_object_new(GARROW_TYPE_TIME64_DATA_TYPE,
                                         "data-type", &arrow_data_type,
                                         NULL));
  return data_type;
}


G_DEFINE_TYPE(GArrowDecimalDataType,
              garrow_decimal_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_decimal_data_type_init(GArrowDecimalDataType *object)
{
}

static void
garrow_decimal_data_type_class_init(GArrowDecimalDataTypeClass *klass)
{
}

/**
 * garrow_decimal_data_type_new:
 * @precision: The precision of decimal data.
 * @scale: The scale of decimal data.
 *
 * Returns: The newly created decimal data type.
 *
 * Since: 0.10.0
 */
GArrowDecimalDataType *
garrow_decimal_data_type_new(gint32 precision,
                             gint32 scale)
{
  auto arrow_data_type = arrow::decimal(precision, scale);

  GArrowDecimalDataType *data_type =
    GARROW_DECIMAL_DATA_TYPE(g_object_new(GARROW_TYPE_DECIMAL_DATA_TYPE,
                                          "data-type", &arrow_data_type,
                                          NULL));
  return data_type;
}

/**
 * garrow_decimal_data_type_get_precision:
 * @decimal_data_type: The #GArrowDecimalDataType.
 *
 * Returns: The precision of the decimal data type.
 *
 * Since: 0.10.0
 */
gint32
garrow_decimal_data_type_get_precision(GArrowDecimalDataType *decimal_data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(decimal_data_type));
  const auto arrow_decimal_type =
    std::static_pointer_cast<arrow::DecimalType>(arrow_data_type);
  return arrow_decimal_type->precision();
}

/**
 * garrow_decimal_data_type_get_scale:
 * @decimal_data_type: The #GArrowDecimalDataType.
 *
 * Returns: The scale of the decimal data type.
 *
 * Since: 0.10.0
 */
gint32
garrow_decimal_data_type_get_scale(GArrowDecimalDataType *decimal_data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(decimal_data_type));
  const auto arrow_decimal_type =
    std::static_pointer_cast<arrow::DecimalType>(arrow_data_type);
  return arrow_decimal_type->scale();
}

G_END_DECLS

GArrowDataType *
garrow_data_type_new_raw(std::shared_ptr<arrow::DataType> *arrow_data_type)
{
  GType type;
  GArrowDataType *data_type;

  switch ((*arrow_data_type)->id()) {
  case arrow::Type::type::NA:
    type = GARROW_TYPE_NULL_DATA_TYPE;
    break;
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_DATA_TYPE;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_DATA_TYPE;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_DATA_TYPE;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_DATA_TYPE;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_DATA_TYPE;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_DATA_TYPE;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_DATA_TYPE;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_DATA_TYPE;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_DATA_TYPE;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_DATA_TYPE;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_DATA_TYPE;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_DATA_TYPE;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_DATA_TYPE;
    break;
  case arrow::Type::type::DATE32:
    type = GARROW_TYPE_DATE32_DATA_TYPE;
    break;
  case arrow::Type::type::DATE64:
    type = GARROW_TYPE_DATE64_DATA_TYPE;
    break;
  case arrow::Type::type::TIMESTAMP:
    type = GARROW_TYPE_TIMESTAMP_DATA_TYPE;
    break;
  case arrow::Type::type::TIME32:
    type = GARROW_TYPE_TIME32_DATA_TYPE;
    break;
  case arrow::Type::type::TIME64:
    type = GARROW_TYPE_TIME64_DATA_TYPE;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_DATA_TYPE;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_DATA_TYPE;
    break;
  case arrow::Type::type::UNION:
    {
      auto arrow_union_data_type =
        std::static_pointer_cast<arrow::UnionType>(*arrow_data_type);
      if (arrow_union_data_type->mode() == arrow::UnionMode::SPARSE) {
        type = GARROW_TYPE_SPARSE_UNION_DATA_TYPE;
      } else {
        type = GARROW_TYPE_DENSE_UNION_DATA_TYPE;
      }
    }
    break;
  case arrow::Type::type::DICTIONARY:
    type = GARROW_TYPE_DICTIONARY_DATA_TYPE;
    break;
  case arrow::Type::type::DECIMAL:
    type = GARROW_TYPE_DECIMAL_DATA_TYPE;
    break;
  default:
    type = GARROW_TYPE_DATA_TYPE;
    break;
  }
  data_type = GARROW_DATA_TYPE(g_object_new(type,
                                            "data-type", arrow_data_type,
                                            NULL));
  return data_type;
}

std::shared_ptr<arrow::DataType>
garrow_data_type_get_raw(GArrowDataType *data_type)
{
  GArrowDataTypePrivate *priv;

  priv = GARROW_DATA_TYPE_GET_PRIVATE(data_type);
  return priv->data_type;
}
