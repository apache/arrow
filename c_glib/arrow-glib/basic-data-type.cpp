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

#include <arrow-glib/array.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/type.hpp>

#include <arrow/c/bridge.h>

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
 * #GArrowNullDataType is a class for the null data type.
 *
 * #GArrowBooleanDataType is a class for the boolean data type.
 *
 * #GArrowInt8DataType is a class for the 8-bit integer data type.
 *
 * #GArrowUInt8DataType is a class for the 8-bit unsigned integer data type.
 *
 * #GArrowInt16DataType is a class for the 16-bit integer data type.
 *
 * #GArrowUInt16DataType is a class for the 16-bit unsigned integer data type.
 *
 * #GArrowInt32DataType is a class for the 32-bit integer data type.
 *
 * #GArrowUInt32DataType is a class for the 32-bit unsigned integer data type.
 *
 * #GArrowInt64DataType is a class for the 64-bit integer data type.
 *
 * #GArrowUInt64DataType is a class for the 64-bit unsigned integer data type.
 *
 * #GArrowHalfFloatDataType is a class for the 16-bit floating point
 * data type.
 *
 * #GArrowFloatDataType is a class for the 32-bit floating point data
 * type.
 *
 * #GArrowDoubleDataType is a class for the 64-bit floating point data
 * type.
 *
 * #GArrowBinaryDataType is a class for the binary data type.
 *
 * #GArrowLargeBinaryDataType is a class for the 64-bit offsets binary
 * data type.
 *
 * #GArrowFixedSizeBinaryDataType is a class for the fixed-size binary
 * data type.
 *
 * #GArrowStringDataType is a class for the UTF-8 encoded string data
 * type.
 *
 * #GArrowLargeStringDataType is a class for the 64-bit offsets UTF-8
 * encoded string data type.
 *
 * #GArrowTemporalDataType is an abstract class for temporal related data type
 * such as #GArrowDate32DataType.
 *
 * #GArrowDate32DataType is a class for the number of days since UNIX
 * epoch in the 32-bit signed integer data type.
 *
 * #GArrowDate64DataType is a class for the number of milliseconds
 * since UNIX epoch in the 64-bit signed integer data type.
 *
 * #GArrowTimestampDataType is a class for the number of
 * seconds/milliseconds/microseconds/nanoseconds since UNIX epoch in
 * the 64-bit signed integer data type.
 *
 * #GArrowTime32DataType is a class for the number of seconds or
 * milliseconds since midnight in the 32-bit signed integer data type.
 *
 * #GArrowTime64DataType is a class for the number of microseconds or
 * nanoseconds since midnight in the 64-bit signed integer data type.
 *
 * #GArrowIntervalDataType is an abstract class for interval related
 * data type such as #GArrowMonthIntervalDataType.
 *
 * #GArrowMonthIntervalDataType is a class for the month intarval data
 * type.
 *
 * #GArrowDayTimeIntervalDataType is a class for the day time intarval
 * data type.
 *
 * #GArrowMonthDayNanoIntervalDataType is a class for the month day
 * nano intarval data type.
 *
 * #GArrowDecimalDataType is a base class for the decimal data types.
 *
 * #GArrowDecimal128DataType is a class for the 128-bit decimal data type.
 *
 * #GArrowDecimal256DataType is a class for the 256-bit decimal data type.
 *
 * #GArrowExtensionDataType is a base class for user-defined extension
 * data types.
 *
 * #GArrowExtensionDataTypeRegistry is a class to manage extension
 * data types.
 */

typedef struct GArrowDataTypePrivate_ {
  std::shared_ptr<arrow::DataType> data_type;
} GArrowDataTypePrivate;

enum {
  PROP_DATA_TYPE = 1
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowDataType,
                                    garrow_data_type,
                                    G_TYPE_OBJECT)

#define GARROW_DATA_TYPE_GET_PRIVATE(obj)         \
  static_cast<GArrowDataTypePrivate *>(           \
    garrow_data_type_get_instance_private(        \
      GARROW_DATA_TYPE(obj)))

static void
garrow_data_type_finalize(GObject *object)
{
  auto priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  priv->data_type.~shared_ptr();

  G_OBJECT_CLASS(garrow_data_type_parent_class)->finalize(object);
}

static void
garrow_data_type_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GARROW_DATA_TYPE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DATA_TYPE:
    {
      auto data_type = g_value_get_pointer(value);
      if (data_type) {
        priv->data_type =
          *static_cast<std::shared_ptr<arrow::DataType> *>(data_type);
      }
    }
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
  auto priv = GARROW_DATA_TYPE_GET_PRIVATE(object);
  new(&priv->data_type) std::shared_ptr<arrow::DataType>;
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
                              "Data type",
                              "The raw std::shared<arrow::DataType> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_data_type_import:
 * @c_abi_schema: (not nullable): A `struct ArrowSchema *`.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An imported #GArrowDataType on success,
 *   %NULL on error.
 *
 *   You don't need to release the passed `struct ArrowSchema *`,
 *   even if this function reports an error.
 *
 * Since: 6.0.0
 */
GArrowDataType *
garrow_data_type_import(gpointer c_abi_schema, GError **error)
{
  auto arrow_data_type_result =
    arrow::ImportType(static_cast<ArrowSchema *>(c_abi_schema));
  if (garrow::check(error, arrow_data_type_result, "[data-type][import]")) {
    return garrow_data_type_new_raw(&(*arrow_data_type_result));
  } else {
    return NULL;
  }
}

/**
 * garrow_data_type_export:
 * @data_type: A #GArrowDataType.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An exported #GArrowDataType as
 *   `struct ArrowStruct *` on success, %NULL on error.
 *
 *   It should be freed with the `ArrowSchema::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gpointer
garrow_data_type_export(GArrowDataType *data_type, GError **error)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto c_abi_schema = g_new(ArrowSchema, 1);
  auto status = arrow::ExportType(*arrow_data_type, c_abi_schema);
  if (garrow::check(error, status, "[data-type][export]")) {
    return c_abi_schema;
  } else {
    g_free(c_abi_schema);
    return NULL;
  }
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
 * Returns: The string representation of the data type.
 *
 *   It should be freed with g_free() when no longer needed.
 */
gchar *
garrow_data_type_to_string(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto string = arrow_data_type->ToString();
  return g_strdup(string.c_str());
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

/**
 * garrow_data_type_get_name:
 * @data_type: A #GArrowDataType.
 *
 * Returns: The name of the data type.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
garrow_data_type_get_name(GArrowDataType *data_type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto name = arrow_data_type->name();
  return g_strdup(name.c_str());
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
 * garrow_fixed_width_data_type_get_bit_width:
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

/**
 * garrow_integer_data_type_is_signed:
 * @data_type: A #GArrowIntegerDataType.
 *
 * Returns: %TRUE if the data type is signed, %FALSE otherwise.
 *
 * Since: 0.16.0
 */
gboolean
garrow_integer_data_type_is_signed(GArrowIntegerDataType *data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  const auto arrow_integer_type =
    std::static_pointer_cast<arrow::IntegerType>(arrow_data_type);
  return arrow_integer_type->is_signed();
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


G_DEFINE_TYPE(GArrowHalfFloatDataType,
              garrow_half_float_data_type,
              GARROW_TYPE_FLOATING_POINT_DATA_TYPE)

static void
garrow_half_float_data_type_init(GArrowHalfFloatDataType *object)
{
}

static void
garrow_half_float_data_type_class_init(GArrowHalfFloatDataTypeClass *klass)
{
}

/**
 * garrow_half_float_data_type_new:
 *
 * Returns: The newly created half float data type.
 *
 * Since: 11.0.0
 */
GArrowHalfFloatDataType *
garrow_half_float_data_type_new(void)
{
  auto arrow_data_type = arrow::float16();
  auto data_type =
    GARROW_HALF_FLOAT_DATA_TYPE(g_object_new(GARROW_TYPE_HALF_FLOAT_DATA_TYPE,
                                             "data-type", &arrow_data_type,
                                             NULL));
  return data_type;
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


G_DEFINE_TYPE(GArrowFixedSizeBinaryDataType,
              garrow_fixed_size_binary_data_type,
              GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_fixed_size_binary_data_type_init(GArrowFixedSizeBinaryDataType *object)
{
}

static void
garrow_fixed_size_binary_data_type_class_init(GArrowFixedSizeBinaryDataTypeClass *klass)
{
}

/**
 * garrow_fixed_size_binary_data_type:
 * @byte_width: The byte width.
 *
 * Returns: The newly created fixed-size binary data type.
 *
 * Since: 0.12.0
 */
GArrowFixedSizeBinaryDataType *
garrow_fixed_size_binary_data_type_new(gint32 byte_width)
{
  auto arrow_fixed_size_binary_data_type = arrow::fixed_size_binary(byte_width);

  auto fixed_size_binary_data_type =
    GARROW_FIXED_SIZE_BINARY_DATA_TYPE(g_object_new(GARROW_TYPE_FIXED_SIZE_BINARY_DATA_TYPE,
                                                    "data-type", &arrow_fixed_size_binary_data_type,
                                                    NULL));
  return fixed_size_binary_data_type;
}

/**
 * garrow_fixed_size_binary_data_type_get_byte_width:
 * @data_type: A #GArrowFixedSizeBinaryDataType.
 *
 * Returns: The number of bytes for one data.
 *
 * Since: 0.12.0
 */
gint32
garrow_fixed_size_binary_data_type_get_byte_width(GArrowFixedSizeBinaryDataType *data_type)
{
  const auto arrow_data_type =
    garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  const auto arrow_fixed_size_binary_type =
    std::static_pointer_cast<arrow::FixedSizeBinaryType>(arrow_data_type);
  return arrow_fixed_size_binary_type->byte_width();
}


G_DEFINE_TYPE(GArrowLargeBinaryDataType,
              garrow_large_binary_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_large_binary_data_type_init(GArrowLargeBinaryDataType *object)
{
}

static void
garrow_large_binary_data_type_class_init(GArrowLargeBinaryDataTypeClass *klass)
{
}

/**
 * garrow_large_binary_data_type_new:
 *
 * Returns: The newly created #GArrowLargeBinaryDataType.
 *
 * Since: 0.17.0
 */
GArrowLargeBinaryDataType *
garrow_large_binary_data_type_new(void)
{
  auto arrow_data_type = arrow::large_binary();

  GArrowLargeBinaryDataType *data_type =
    GARROW_LARGE_BINARY_DATA_TYPE(g_object_new(GARROW_TYPE_LARGE_BINARY_DATA_TYPE,
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


G_DEFINE_TYPE(GArrowLargeStringDataType,
              garrow_large_string_data_type,
              GARROW_TYPE_DATA_TYPE)

static void
garrow_large_string_data_type_init(GArrowLargeStringDataType *object)
{
}

static void
garrow_large_string_data_type_class_init(GArrowLargeStringDataTypeClass *klass)
{
}

/**
 * garrow_large_string_data_type_new:
 *
 * Returns: The newly created #GArrowLargeStringDataType.
 *
 * Since: 0.17.0
 */
GArrowLargeStringDataType *
garrow_large_string_data_type_new(void)
{
  auto arrow_data_type = arrow::large_utf8();

  GArrowLargeStringDataType *data_type =
    GARROW_LARGE_STRING_DATA_TYPE(g_object_new(GARROW_TYPE_LARGE_STRING_DATA_TYPE,
                                               "data-type", &arrow_data_type,
                                               NULL));
  return data_type;
}


G_DEFINE_ABSTRACT_TYPE(GArrowTemporalDataType,
                       garrow_temporal_data_type,
                       GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_temporal_data_type_init(GArrowTemporalDataType *object)
{
}

static void
garrow_temporal_data_type_class_init(GArrowTemporalDataTypeClass *klass)
{
}


G_DEFINE_TYPE(GArrowDate32DataType,
              garrow_date32_data_type,
              GARROW_TYPE_TEMPORAL_DATA_TYPE)

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
              GARROW_TYPE_TEMPORAL_DATA_TYPE)

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
              GARROW_TYPE_TEMPORAL_DATA_TYPE)

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


G_DEFINE_ABSTRACT_TYPE(GArrowTimeDataType,
                       garrow_time_data_type,
                       GARROW_TYPE_TEMPORAL_DATA_TYPE)

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


G_DEFINE_ABSTRACT_TYPE(GArrowIntervalDataType,
                       garrow_interval_data_type,
                       GARROW_TYPE_TEMPORAL_DATA_TYPE)

static void
garrow_interval_data_type_init(GArrowIntervalDataType *object)
{
}

static void
garrow_interval_data_type_class_init(GArrowIntervalDataTypeClass *klass)
{
}

/**
 * garrow_interval_data_type_get_interval_type:
 * @type: The #GArrowIntervalDataType.
 *
 * Returns: The interval type of the given @type.
 *
 * Since: 7.0.0
 */
GArrowIntervalType
garrow_interval_data_type_get_interval_type(GArrowIntervalDataType *type)
{
  const auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(type));
  const auto arrow_interval_type =
    std::static_pointer_cast<arrow::IntervalType>(arrow_data_type);
  return garrow_interval_type_from_raw(arrow_interval_type->interval_type());
}


G_DEFINE_TYPE(GArrowMonthIntervalDataType,
              garrow_month_interval_data_type,
              GARROW_TYPE_INTERVAL_DATA_TYPE)
static void
garrow_month_interval_data_type_init(GArrowMonthIntervalDataType *object)
{
}

static void
garrow_month_interval_data_type_class_init(
  GArrowMonthIntervalDataTypeClass *klass)
{
}

/**
 * garrow_month_interval_data_type_new:
 *
 * Returns: The newly created month interval data type.
 *
 * Since: 7.0.0
 */
GArrowMonthIntervalDataType *
garrow_month_interval_data_type_new(void)
{
  auto arrow_data_type = arrow::month_interval();

  auto data_type = g_object_new(GARROW_TYPE_MONTH_INTERVAL_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
  return GARROW_MONTH_INTERVAL_DATA_TYPE(data_type);
}


G_DEFINE_TYPE(GArrowDayTimeIntervalDataType,
              garrow_day_time_interval_data_type,
              GARROW_TYPE_INTERVAL_DATA_TYPE)

static void
garrow_day_time_interval_data_type_init(GArrowDayTimeIntervalDataType *object)
{
}

static void
garrow_day_time_interval_data_type_class_init(
  GArrowDayTimeIntervalDataTypeClass *klass)
{
}

/**
 * garrow_day_time_interval_data_type_new:
 *
 * Returns: The newly created day time interval data type.
 *
 * Since: 7.0.0
 */
GArrowDayTimeIntervalDataType *
garrow_day_time_interval_data_type_new(void)
{
  auto arrow_data_type = arrow::day_time_interval();

  auto data_type = g_object_new(GARROW_TYPE_DAY_TIME_INTERVAL_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
  return GARROW_DAY_TIME_INTERVAL_DATA_TYPE(data_type);
}


G_DEFINE_TYPE(GArrowMonthDayNanoIntervalDataType,
              garrow_month_day_nano_interval_data_type,
              GARROW_TYPE_INTERVAL_DATA_TYPE)

static void
garrow_month_day_nano_interval_data_type_init(
  GArrowMonthDayNanoIntervalDataType *object)
{
}

static void
garrow_month_day_nano_interval_data_type_class_init(
  GArrowMonthDayNanoIntervalDataTypeClass *klass)
{
}

/**
 * garrow_month_day_nano_interval_data_type_new:
 *
 * Returns: The newly created month day nano interval data type.
 *
 * Since: 7.0.0
 */
GArrowMonthDayNanoIntervalDataType *
garrow_month_day_nano_interval_data_type_new(void)
{
  auto arrow_data_type = arrow::month_day_nano_interval();

  auto data_type = g_object_new(GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
  return GARROW_MONTH_DAY_NANO_INTERVAL_DATA_TYPE(data_type);
}


G_DEFINE_ABSTRACT_TYPE(GArrowDecimalDataType,
                       garrow_decimal_data_type,
                       GARROW_TYPE_FIXED_SIZE_BINARY_DATA_TYPE)

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
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The newly created decimal data type on success, %NULL on error.
 *
 *   #GArrowDecimal256DataType is used if @precision is larger than
 *   garrow_decimal128_data_type_max_precision(),
 *   #GArrowDecimal128DataType is used otherwise.
 *
 * Since: 0.10.0
 */
GArrowDecimalDataType *
garrow_decimal_data_type_new(gint32 precision,
                             gint32 scale,
                             GError **error)
{
  if (precision <= garrow_decimal128_data_type_max_precision()) {
    return GARROW_DECIMAL_DATA_TYPE(
      garrow_decimal128_data_type_new(precision, scale, error));
  } else {
    return GARROW_DECIMAL_DATA_TYPE(
      garrow_decimal256_data_type_new(precision, scale, error));
  }
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


G_DEFINE_TYPE(GArrowDecimal128DataType,
              garrow_decimal128_data_type,
              GARROW_TYPE_DECIMAL_DATA_TYPE)

static void
garrow_decimal128_data_type_init(GArrowDecimal128DataType *object)
{
}

static void
garrow_decimal128_data_type_class_init(GArrowDecimal128DataTypeClass *klass)
{
}

/**
 * garrow_decimal128_data_type_max_precision:
 *
 * Returns: The max precision of 128-bit decimal data type.
 *
 * Since: 3.0.0
 */
gint32
garrow_decimal128_data_type_max_precision()
{
  return arrow::Decimal128Type::kMaxPrecision;
}

/**
 * garrow_decimal128_data_type_new:
 * @precision: The precision of decimal data.
 * @scale: The scale of decimal data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The newly created 128-bit decimal data type on success, %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowDecimal128DataType *
garrow_decimal128_data_type_new(gint32 precision,
                                gint32 scale,
                                GError **error)
{
  auto arrow_data_type_result = arrow::Decimal128Type::Make(precision, scale);
  if (garrow::check(error,
                    arrow_data_type_result,
                    "[decimal128-data-type][new]")) {
    auto arrow_data_type = *arrow_data_type_result;
    return GARROW_DECIMAL128_DATA_TYPE(
      g_object_new(GARROW_TYPE_DECIMAL128_DATA_TYPE,
                   "data-type", &arrow_data_type,
                   NULL));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowDecimal256DataType,
              garrow_decimal256_data_type,
              GARROW_TYPE_DECIMAL_DATA_TYPE)

static void
garrow_decimal256_data_type_init(GArrowDecimal256DataType *object)
{
}

static void
garrow_decimal256_data_type_class_init(GArrowDecimal256DataTypeClass *klass)
{
}

/**
 * garrow_decimal256_data_type_max_precision:
 *
 * Returns: The max precision of 256-bit decimal data type.
 *
 * Since: 3.0.0
 */
gint32
garrow_decimal256_data_type_max_precision()
{
  return arrow::Decimal256Type::kMaxPrecision;
}

/**
 * garrow_decimal256_data_type_new:
 * @precision: The precision of decimal data.
 * @scale: The scale of decimal data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The newly created 256-bit decimal data type on success, %NULL on error.
 *
 * Since: 3.0.0
 */
GArrowDecimal256DataType *
garrow_decimal256_data_type_new(gint32 precision,
                                gint32 scale,
                                GError **error)
{
  auto arrow_data_type_result = arrow::Decimal256Type::Make(precision, scale);
  if (garrow::check(error,
                    arrow_data_type_result,
                    "[decimal256-data-type][new]")) {
    auto arrow_data_type = *arrow_data_type_result;
    return GARROW_DECIMAL256_DATA_TYPE(
      g_object_new(GARROW_TYPE_DECIMAL256_DATA_TYPE,
                   "data-type", &arrow_data_type,
                   NULL));
  } else {
    return NULL;
  }
}


typedef struct GArrowExtensionDataTypePrivate_ {
  GArrowDataType *storage_data_type;
} GArrowExtensionDataTypePrivate;

enum {
  PROP_STORAGE_DATA_TYPE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExtensionDataType,
                           garrow_extension_data_type,
                           GARROW_TYPE_DATA_TYPE)

#define GARROW_EXTENSION_DATA_TYPE_GET_PRIVATE(obj)         \
  static_cast<GArrowExtensionDataTypePrivate *>(            \
    garrow_extension_data_type_get_instance_private(        \
      GARROW_EXTENSION_DATA_TYPE(obj)))

static void
garrow_extension_data_type_dispose(GObject *object)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_GET_PRIVATE(object);

  if (priv->storage_data_type) {
    g_object_unref(priv->storage_data_type);
    priv->storage_data_type = NULL;
  }

  G_OBJECT_CLASS(garrow_extension_data_type_parent_class)->dispose(object);
}

static void
garrow_extension_data_type_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STORAGE_DATA_TYPE:
    priv->storage_data_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extension_data_type_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STORAGE_DATA_TYPE:
    g_value_set_object(value, priv->storage_data_type);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extension_data_type_init(GArrowExtensionDataType *object)
{
}

static void
garrow_extension_data_type_class_init(GArrowExtensionDataTypeClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = garrow_extension_data_type_dispose;
  gobject_class->set_property = garrow_extension_data_type_set_property;
  gobject_class->get_property = garrow_extension_data_type_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("storage-data-type",
                             "Storage data type",
                             "The underlying GArrowDataType",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STORAGE_DATA_TYPE, spec);
}

/**
 * garrow_extension_data_type_get_extension_name:
 * @data_type: A #GArrowExtensionDataType.
 *
 * Returns: The extension name of the type.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
garrow_extension_data_type_get_extension_name(GArrowExtensionDataType *data_type)
{
  auto arrow_data_type =
    std::static_pointer_cast<arrow::ExtensionType>(
      garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type)));
  const auto name = arrow_data_type->extension_name();
  return g_strdup(name.c_str());
}

/**
 * garrow_extension_data_type_wrap_array:
 * @data_type: A #GArrowExtensionDataType.
 * @storage: A #GArrowArray.
 *
 * Returns: (transfer full): The array that wraps underlying storage array.
 *
 * Since: 3.0.0
 */
GArrowExtensionArray *
garrow_extension_data_type_wrap_array(GArrowExtensionDataType *data_type,
                                      GArrowArray *storage)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_storage = garrow_array_get_raw(storage);
  auto arrow_extension_array = arrow::ExtensionType::WrapArray(arrow_data_type,
                                                               arrow_storage);
  auto array = garrow_extension_array_new_raw(&arrow_extension_array, storage);
  return GARROW_EXTENSION_ARRAY(array);
}

/**
 * garrow_extension_data_type_wrap_chunked_array:
 * @data_type: A #GArrowExtensionDataType.
 * @storage: A #GArrowChunkedArray.
 *
 * Returns: (transfer full): The chunked array that wraps underlying
 *   storage chunked array.
 *
 * Since: 3.0.0
 */
GArrowChunkedArray *
garrow_extension_data_type_wrap_chunked_array(GArrowExtensionDataType *data_type,
                                              GArrowChunkedArray *storage)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_storage = garrow_chunked_array_get_raw(storage);
  auto arrow_extension_chunked_array =
    arrow::ExtensionType::WrapArray(arrow_data_type,
                                    arrow_storage);
  return garrow_chunked_array_new_raw(&arrow_extension_chunked_array);
}


static std::shared_ptr<arrow::DataType>
garrow_extension_data_type_get_storage_data_type_raw(
  GArrowExtensionDataType *data_type)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_GET_PRIVATE(data_type);
  return garrow_data_type_get_raw(priv->storage_data_type);
}

G_END_DECLS

namespace garrow {
  GExtensionType::GExtensionType(GArrowExtensionDataType *garrow_data_type) :
    arrow::ExtensionType(
      garrow_extension_data_type_get_storage_data_type_raw(garrow_data_type)),
    garrow_data_type_(garrow_data_type) {
    g_object_ref(garrow_data_type_);
  }

  GExtensionType::~GExtensionType() {
    g_object_unref(garrow_data_type_);
  }

  GArrowExtensionDataType *GExtensionType::garrow_data_type() const {
    return garrow_data_type_;
  }

  std::string GExtensionType::extension_name() const {
    auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(garrow_data_type_);
    auto c_name = klass->get_extension_name(garrow_data_type_);
    std::string name(c_name);
    g_free(c_name);
    return name;
  }

  bool GExtensionType::ExtensionEquals(const arrow::ExtensionType& other) const {
    if (extension_name() != other.extension_name()) {
      return false;
    }
    auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(garrow_data_type_);
    auto garrow_other_data_type =
      static_cast<const GExtensionType&>(other).garrow_data_type_;
    return klass->equal(garrow_data_type_,
                        garrow_other_data_type);
  }

  std::shared_ptr<arrow::Array>
  GExtensionType::MakeArray(std::shared_ptr<arrow::ArrayData> data) const {
    return std::make_shared<arrow::ExtensionArray>(data);
  }

  arrow::Result<std::shared_ptr<arrow::DataType>>
  GExtensionType::Deserialize(std::shared_ptr<arrow::DataType> storage_data_type,
                              const std::string& serialized_data) const {
    auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(garrow_data_type_);
    auto garrow_storage_data_type = garrow_data_type_new_raw(&storage_data_type);
    GBytes *g_serialized_data = g_bytes_new_static(serialized_data.data(),
                                                   serialized_data.size());
    GError *error = NULL;
    auto garrow_deserialized_data_type =
      klass->deserialize(garrow_data_type_,
                         garrow_storage_data_type,
                         g_serialized_data,
                         &error);
    g_bytes_unref(g_serialized_data);
    g_object_unref(garrow_storage_data_type);
    if (error) {
      return garrow_error_to_status(error,
                                    arrow::StatusCode::SerializationError,
                                    "[extension-type][deserialize]");
    }

    auto deserialized_data_type =
      garrow_data_type_get_raw(garrow_deserialized_data_type);
    g_object_unref(garrow_deserialized_data_type);
    return deserialized_data_type;
  }

  std::string
  GExtensionType::Serialize() const {
    auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(garrow_data_type_);
    auto g_bytes = klass->serialize(garrow_data_type_);
    gsize raw_data_size = 0;
    auto raw_data = g_bytes_get_data(g_bytes, &raw_data_size);
    std::string data(static_cast<const char *>(raw_data),
                     raw_data_size);
    g_bytes_unref(g_bytes);
    return data;
  }

  GType GExtensionType::array_gtype() const {
    auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(garrow_data_type_);
    return klass->get_array_gtype(garrow_data_type_);
  }
}

G_BEGIN_DECLS


typedef struct GArrowExtensionDataTypeRegistryPrivate_ {
  std::shared_ptr<arrow::ExtensionTypeRegistry> registry;
} GArrowExtensionDataTypeRegistryPrivate;

enum {
  PROP_REGISTRY = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExtensionDataTypeRegistry,
                           garrow_extension_data_type_registry,
                           G_TYPE_OBJECT)

#define GARROW_EXTENSION_DATA_TYPE_REGISTRY_GET_PRIVATE(obj)    \
  static_cast<GArrowExtensionDataTypeRegistryPrivate *>(        \
    garrow_extension_data_type_registry_get_instance_private(   \
      GARROW_EXTENSION_DATA_TYPE_REGISTRY(obj)))

static void
garrow_extension_data_type_registry_finalize(GObject *object)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_REGISTRY_GET_PRIVATE(object);

  priv->registry.~shared_ptr();

  G_OBJECT_CLASS(garrow_extension_data_type_registry_parent_class)->finalize(object);
}

static void
garrow_extension_data_type_registry_set_property(GObject *object,
                                                 guint prop_id,
                                                 const GValue *value,
                                                 GParamSpec *pspec)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_REGISTRY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_REGISTRY:
    priv->registry =
      *static_cast<std::shared_ptr<arrow::ExtensionTypeRegistry> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extension_data_type_registry_init(GArrowExtensionDataTypeRegistry *object)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_REGISTRY_GET_PRIVATE(object);
  new(&priv->registry) std::shared_ptr<arrow::ExtensionTypeRegistry>;
}

static void
garrow_extension_data_type_registry_class_init(GArrowExtensionDataTypeRegistryClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_extension_data_type_registry_finalize;
  gobject_class->set_property = garrow_extension_data_type_registry_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("registry",
                              "Registry",
                              "The raw std::shared<arrow::ExtensionTypeRegistry> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_REGISTRY, spec);
}

/**
 * garrow_extension_data_type_registry_default:
 *
 * Returns: (transfer full): The default global extension data type registry.
 *
 * Since: 3.0.0
 */
GArrowExtensionDataTypeRegistry *
garrow_extension_data_type_registry_default(void)
{
  auto arrow_registry = arrow::ExtensionTypeRegistry::GetGlobalRegistry();
  return garrow_extension_data_type_registry_new_raw(&arrow_registry);
}

/**
 * garrow_extension_data_type_registry_register:
 * @registry: A #GArrowExtensionDataTypeRegistry.
 * @data_type: A #GArrowExtensionDataType to be registered.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Register the given @data_type to the @registry.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 3.0.0
 */
gboolean
garrow_extension_data_type_registry_register(
  GArrowExtensionDataTypeRegistry *registry,
  GArrowExtensionDataType *data_type,
  GError **error)
{
  const gchar *context = "[extension-data-type-registry][register]";
  auto klass = GARROW_EXTENSION_DATA_TYPE_GET_CLASS(data_type);
  auto set_error = [&](const gchar *name) -> void {
    auto klass_name = G_OBJECT_CLASS_NAME(klass);
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "%s %s::%s() isn't implemented",
                context,
                klass_name,
                name);
  };
  if (!klass->get_extension_name) {
    set_error("get_extension_name");
    return FALSE;
  }
  if (!klass->equal) {
    set_error("equal");
    return FALSE;
  }
  if (!klass->deserialize) {
    set_error("deserialize");
    return FALSE;
  }
  if (!klass->serialize) {
    set_error("serialize");
    return FALSE;
  }
  if (!klass->get_array_gtype) {
    set_error("get_array_gtype");
    return FALSE;
  }

  auto arrow_registry = garrow_extension_data_type_registry_get_raw(registry);
  auto arrow_data_type =
    std::static_pointer_cast<arrow::ExtensionType>(
      garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type)));
  auto status = arrow_registry->RegisterType(arrow_data_type);
  return garrow::check(error, status, context);
}

/**
 * garrow_extension_data_type_registry_unregister:
 * @registry: A #GArrowExtensionDataTypeRegistry.
 * @name: An extension data type name to be unregistered.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Unregister an extension data type that has the given @name from the
 * @registry.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 3.0.0
 */
gboolean
garrow_extension_data_type_registry_unregister(
  GArrowExtensionDataTypeRegistry *registry,
  const gchar *name,
  GError **error)
{
  auto arrow_registry = garrow_extension_data_type_registry_get_raw(registry);
  auto status = arrow_registry->UnregisterType(name);
  return garrow::check(error,
                       status,
                       "[extension-data-type-registry][unregister]");
}

/**
 * garrow_extension_data_type_registry_lookup:
 * @registry: A #GArrowExtensionDataTypeRegistry.
 * @name: An extension data type name to be looked up.
 *
 * Returns: (transfer full): A found #GArrowExtensionDataType on
 *   found, %NULL on not found.
 *
 * Since: 3.0.0
 */
GArrowExtensionDataType *
garrow_extension_data_type_registry_lookup(
  GArrowExtensionDataTypeRegistry *registry,
  const gchar *name)
{
  auto arrow_registry = garrow_extension_data_type_registry_get_raw(registry);
  auto arrow_extension_data_type = arrow_registry->GetType(name);
  if (!arrow_extension_data_type) {
    return NULL;
  }
  auto arrow_data_type =
    std::static_pointer_cast<arrow::DataType>(arrow_extension_data_type);
  auto data_type = garrow_data_type_new_raw(&arrow_data_type);
  return GARROW_EXTENSION_DATA_TYPE(data_type);
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
  case arrow::Type::type::HALF_FLOAT:
    type = GARROW_TYPE_HALF_FLOAT_DATA_TYPE;
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
  case arrow::Type::type::LARGE_BINARY:
    type = GARROW_TYPE_LARGE_BINARY_DATA_TYPE;
    break;
  case arrow::Type::type::FIXED_SIZE_BINARY:
    type = GARROW_TYPE_FIXED_SIZE_BINARY_DATA_TYPE;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_DATA_TYPE;
    break;
  case arrow::Type::type::LARGE_STRING:
    type = GARROW_TYPE_LARGE_STRING_DATA_TYPE;
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
  case arrow::Type::type::LARGE_LIST:
    type = GARROW_TYPE_LARGE_LIST_DATA_TYPE;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_DATA_TYPE;
    break;
  case arrow::Type::type::SPARSE_UNION:
    type = GARROW_TYPE_SPARSE_UNION_DATA_TYPE;
    break;
  case arrow::Type::type::DENSE_UNION:
    type = GARROW_TYPE_DENSE_UNION_DATA_TYPE;
    break;
  case arrow::Type::type::DICTIONARY:
    type = GARROW_TYPE_DICTIONARY_DATA_TYPE;
    break;
  case arrow::Type::type::MAP:
    type = GARROW_TYPE_MAP_DATA_TYPE;
    break;
  case arrow::Type::type::DECIMAL128:
    type = GARROW_TYPE_DECIMAL128_DATA_TYPE;
    break;
  case arrow::Type::type::DECIMAL256:
    type = GARROW_TYPE_DECIMAL256_DATA_TYPE;
    break;
  case arrow::Type::type::INTERVAL_MONTHS:
    type = GARROW_TYPE_MONTH_INTERVAL_DATA_TYPE;
    break;
  case arrow::Type::type::INTERVAL_DAY_TIME:
    type = GARROW_TYPE_DAY_TIME_INTERVAL_DATA_TYPE;
    break;
  case arrow::Type::type::INTERVAL_MONTH_DAY_NANO:
    type = GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_DATA_TYPE;
    break;
  case arrow::Type::type::EXTENSION:
    {
      auto g_extension_data_type =
        std::static_pointer_cast<garrow::GExtensionType>(*arrow_data_type);
      if (g_extension_data_type) {
        auto garrow_data_type = g_extension_data_type->garrow_data_type();
        g_object_ref(garrow_data_type);
        return GARROW_DATA_TYPE(garrow_data_type);
      }
    }
    type = GARROW_TYPE_EXTENSION_DATA_TYPE;
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
  auto priv = GARROW_DATA_TYPE_GET_PRIVATE(data_type);
  if (!priv->data_type &&
      g_type_is_a(G_OBJECT_TYPE(data_type), GARROW_TYPE_EXTENSION_DATA_TYPE)) {
    priv->data_type = std::make_shared<garrow::GExtensionType>(
      GARROW_EXTENSION_DATA_TYPE(data_type));
  }
  return priv->data_type;
}

GArrowExtensionDataTypeRegistry *
garrow_extension_data_type_registry_new_raw(
  std::shared_ptr<arrow::ExtensionTypeRegistry> *arrow_registry)
{
  auto registry = g_object_new(GARROW_TYPE_EXTENSION_DATA_TYPE_REGISTRY,
                               "registry", arrow_registry,
                               NULL);
  return GARROW_EXTENSION_DATA_TYPE_REGISTRY(registry);
}

std::shared_ptr<arrow::ExtensionTypeRegistry>
garrow_extension_data_type_registry_get_raw(
  GArrowExtensionDataTypeRegistry *registry)
{
  auto priv = GARROW_EXTENSION_DATA_TYPE_REGISTRY_GET_PRIVATE(registry);
  return priv->registry;
}
