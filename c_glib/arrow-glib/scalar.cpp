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

#include <arrow-glib/basic-array.hpp>
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/compute.h>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/decimal.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/interval.hpp>
#include <arrow-glib/scalar.hpp>

G_BEGIN_DECLS

/**
 * SECTION: scalar
 * @section_id: scalar-classes
 * @title: Scalar classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowScalar is a base class for all scalar classes such as
 * #GArrowBooleanScalar.
 *
 * #GArrowNullScalar is a class for a null scalar.
 *
 * #GArrowBooleanScalar is a class for a boolean scalar.
 *
 * #GArrowInt8Scalar is a class for a 8-bit integer scalar.
 *
 * #GArrowInt16Scalar is a class for a 16-bit integer scalar.
 *
 * #GArrowInt32Scalar is a class for a 32-bit integer scalar.
 *
 * #GArrowInt64Scalar is a class for a 64-bit integer scalar.
 *
 * #GArrowUInt8Scalar is a class for a 8-bit unsigned integer scalar.
 *
 * #GArrowUInt16Scalar is a class for a 16-bit unsigned integer scalar.
 *
 * #GArrowUInt32Scalar is a class for a 32-bit unsigned integer scalar.
 *
 * #GArrowUInt64Scalar is a class for a 64-bit unsigned integer scalar.
 *
 * #GArrowHalfFloatScalar is a class for a 16-bit floating point scalar.
 *
 * #GArrowFloatScalar is a class for a 32-bit floating point scalar.
 *
 * #GArrowDoubleScalar is a class for a 64-bit floating point scalar.
 *
 * #GArrowBaseBinaryScalar is a base class for all binary and string
 * scalar classes such as #GArrowBinaryScalar.
 *
 * #GArrowBinaryScalar is a class for a binary scalar.
 *
 * #GArrowStringScalar is a class for an UTF-8 encoded string scalar.
 *
 * #GArrowLargeBinaryScalar is a class for a 64-bit offsets binary
 * scalar.
 *
 * #GArrowLargeStringScalar is a class for a 64-bit offsets UTF-8
 * encoded string scalar.
 *
 * #GArrowFixedSizeBinaryScalar is a class for a fixed-size binary
 * scalar.
 *
 * #GArrowDate32Scalar is a class for the number of days since UNIX
 * epoch in a 32-bit signed integer scalar.
 *
 * #GArrowDate64Scalar is a class for the number of milliseconds
 * since UNIX epoch in a 64-bit signed integer scalar.
 *
 * #GArrowTime32Scalar is a class for the number of seconds or
 * milliseconds since midnight in a 32-bit signed integer scalar.
 *
 * #GArrowTime64Scalar is a class for the number of microseconds or
 * nanoseconds since midnight in a 64-bit signed integer scalar.
 *
 * #GArrowTimestampScalar is a class for the number of
 * seconds/milliseconds/microseconds/nanoseconds since UNIX epoch in
 * a 64-bit signed integer scalar.
 *
 * #GArrowMonthIntervalScalar is a class for the month intarval scalar.
 *
 * #GArrowDayTimeIntervalScalar is a class for the day time intarval scalar.
 *
 * #GArrowMonthDayNanoIntervalScalar is a class for the month day nano
 * intarval scalar.
 *
 * #GArrowDecimal128Scalar is a class for a 128-bit decimal scalar.
 *
 * #GArrowDecimal256Scalar is a class for a 256-bit decimal scalar.
 *
 * #GArrowBaseListScalar is a base class for all list scalar classes
 * such as #GArrowListScalar.
 *
 * #GArrowListScalar is a class for a list scalar.
 *
 * #GArrowLargeListScalar is a class for a large list scalar.
 *
 * #GArrowMapScalar is a class for a map list scalar.
 *
 * #GArrowStructScalar is a class for a struct list scalar.
 *
 * #GArrowUnionScalar is a base class for all union scalar classes
 * such as #GArrowSparseUnionScalar.
 *
 * #GArrowSparseUnionScalar is a class for a sparse union scalar.
 *
 * #GArrowDenseUnionScalar is a class for a dense union scalar.
 *
 * #GArrowExtensionScalar is a base class for user-defined extension
 * scalar.
 */

typedef struct GArrowScalarPrivate_ {
  std::shared_ptr<arrow::Scalar> scalar;
  GArrowDataType *data_type;
} GArrowScalarPrivate;

enum {
  PROP_SCALAR = 1,
  PROP_DATA_TYPE,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowScalar,
                                    garrow_scalar,
                                    G_TYPE_OBJECT)

#define GARROW_SCALAR_GET_PRIVATE(obj)            \
  static_cast<GArrowScalarPrivate *>(             \
    garrow_scalar_get_instance_private(           \
      GARROW_SCALAR(obj)))

static void
garrow_scalar_dispose(GObject *object)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(object);

  if (priv->data_type) {
    g_object_unref(priv->data_type);
    priv->data_type = NULL;
  }

  G_OBJECT_CLASS(garrow_scalar_parent_class)->dispose(object);
}

static void
garrow_scalar_finalize(GObject *object)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(object);

  priv->scalar.~shared_ptr();

  G_OBJECT_CLASS(garrow_scalar_parent_class)->finalize(object);
}

static void
garrow_scalar_set_property(GObject *object,
                           guint prop_id,
                           const GValue *value,
                           GParamSpec *pspec)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SCALAR:
    priv->scalar =
      *static_cast<std::shared_ptr<arrow::Scalar> *>(g_value_get_pointer(value));
    break;
  case PROP_DATA_TYPE:
    priv->data_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_scalar_init(GArrowScalar *object)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(object);
  new(&priv->scalar) std::shared_ptr<arrow::Scalar>;
}

static void
garrow_scalar_class_init(GArrowScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_scalar_dispose;
  gobject_class->finalize     = garrow_scalar_finalize;
  gobject_class->set_property = garrow_scalar_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("scalar",
                              "Scalar",
                              "The raw std::shared<arrow::Scalar> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SCALAR, spec);

  /**
   * GArrowScalar:data-type:
   *
   * The data type of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("data-type",
                             "Data type",
                             "The data type of the scalar",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DATA_TYPE, spec);
}

/**
 * garrow_scalar_parse:
 * @data_type: A #GArrowDataType for the parsed scalar.
 * @data: (array length=size): Data to be parsed.
 * @size: The number of bytes of the data.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created #GArrowScalar if the data is parsed successfully,
 *   %NULL otherwise.
 *
 * Since: 5.0.0
 */
GArrowScalar *
garrow_scalar_parse(GArrowDataType *data_type,
                    const guint8 *data,
                    gsize size,
                    GError **error)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_data = std::string_view(reinterpret_cast<const char *>(data),
                                     size);
  auto arrow_scalar_result = arrow::Scalar::Parse(arrow_data_type, arrow_data);
  if (garrow::check(error, arrow_scalar_result, "[scalar][parse]")) {
    auto arrow_scalar = *arrow_scalar_result;
    return garrow_scalar_new_raw(&arrow_scalar,
                                 "scalar", &arrow_scalar,
                                 "data-type", data_type,
                                 NULL);
  } else {
    return NULL;
  }
}

/**
 * garrow_scalar_get_data_type:
 * @scalar: A #GArrowScalar.
 *
 * Returns: (transfer none): The #GArrowDataType for the scalar.
 *
 * Since: 5.0.0
 */
GArrowDataType *
garrow_scalar_get_data_type(GArrowScalar *scalar)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(scalar);
  if (!priv->data_type) {
    priv->data_type = garrow_data_type_new_raw(&(priv->scalar->type));
  }
  return priv->data_type;
}

/**
 * garrow_scalar_is_valid:
 * @scalar: A #GArrowScalar.
 *
 * Returns: %TRUE if the scalar is valid, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
garrow_scalar_is_valid(GArrowScalar *scalar)
{
  const auto arrow_scalar = garrow_scalar_get_raw(scalar);
  return arrow_scalar->is_valid;
}

/**
 * garrow_scalar_equal:
 * @scalar: A #GArrowScalar.
 * @other_scalar: A #GArrowScalar to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 5.0.0
 */
gboolean
garrow_scalar_equal(GArrowScalar *scalar,
                    GArrowScalar *other_scalar)
{
  return garrow_scalar_equal_options(scalar, other_scalar, NULL);
}

/**
 * garrow_scalar_equal_options:
 * @scalar: A #GArrowScalar.
 * @other_scalar: A #GArrowScalar to be compared.
 * @options: (nullable): A #GArrowEqualOptions.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 5.0.0
 */
gboolean
garrow_scalar_equal_options(GArrowScalar *scalar,
                            GArrowScalar *other_scalar,
                            GArrowEqualOptions *options)
{
  const auto arrow_scalar = garrow_scalar_get_raw(scalar);
  const auto arrow_other_scalar = garrow_scalar_get_raw(other_scalar);
  if (options) {
    auto is_approx = garrow_equal_options_is_approx(options);
    const auto arrow_options = garrow_equal_options_get_raw(options);
    if (is_approx) {
      return arrow_scalar->ApproxEquals(*arrow_other_scalar, *arrow_options);
    } else {
      return arrow_scalar->Equals(*arrow_other_scalar, *arrow_options);
    }
  } else {
    return arrow_scalar->Equals(*arrow_other_scalar);
  }
}

/**
 * garrow_scalar_to_string:
 * @scalar: A #GArrowScalar.
 *
 * Returns: The string representation of the scalar.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 5.0.0
 */
gchar *
garrow_scalar_to_string(GArrowScalar *scalar)
{
  const auto arrow_scalar = garrow_scalar_get_raw(scalar);
  const auto string = arrow_scalar->ToString();
  return g_strdup(string.c_str());
}

/**
 * garrow_scalar_cast:
 * @scalar: A #GArrowScalar.
 * @data_type: A #GArrowDataType of the casted scalar.
 * @options: (nullable): A #GArrowCastOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created casted scalar on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GArrowScalar *
garrow_scalar_cast(GArrowScalar *scalar,
                   GArrowDataType *data_type,
                   GArrowCastOptions *options,
                   GError **error)
{
  const auto arrow_scalar = garrow_scalar_get_raw(scalar);
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_casted_scalar_result = arrow_scalar->CastTo(arrow_data_type);
  if (garrow::check(error, arrow_casted_scalar_result, "[scalar][cast]")) {
    auto arrow_casted_scalar = *arrow_casted_scalar_result;
    return garrow_scalar_new_raw(&arrow_casted_scalar,
                                 "scalar", &arrow_casted_scalar,
                                 "data-type", data_type,
                                 NULL);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowNullScalar,
              garrow_null_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_null_scalar_init(GArrowNullScalar *object)
{
}

static void
garrow_null_scalar_class_init(GArrowNullScalarClass *klass)
{
}

/**
 * garrow_null_scalar_new:
 *
 * Returns: A newly created #GArrowNullScalar.
 *
 * Since: 5.0.0
 */
GArrowNullScalar *
garrow_null_scalar_new(void)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::NullScalar>());
  return GARROW_NULL_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}


G_DEFINE_TYPE(GArrowBooleanScalar,
              garrow_boolean_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_boolean_scalar_init(GArrowBooleanScalar *object)
{
}

static void
garrow_boolean_scalar_class_init(GArrowBooleanScalarClass *klass)
{
}

/**
 * garrow_boolean_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowBooleanScalar.
 *
 * Since: 5.0.0
 */
GArrowBooleanScalar *
garrow_boolean_scalar_new(gboolean value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::BooleanScalar>(value));
  return GARROW_BOOLEAN_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_boolean_scalar_get_value:
 * @scalar: A #GArrowBooleanScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gboolean
garrow_boolean_scalar_get_value(GArrowBooleanScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::BooleanScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowInt8Scalar,
              garrow_int8_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_int8_scalar_init(GArrowInt8Scalar *object)
{
}

static void
garrow_int8_scalar_class_init(GArrowInt8ScalarClass *klass)
{
}

/**
 * garrow_int8_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowInt8Scalar.
 *
 * Since: 5.0.0
 */
GArrowInt8Scalar *
garrow_int8_scalar_new(gint8 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Int8Scalar>(value));
  return GARROW_INT8_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_int8_scalar_get_value:
 * @scalar: A #GArrowInt8Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint8
garrow_int8_scalar_get_value(GArrowInt8Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Int8Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowInt16Scalar,
              garrow_int16_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_int16_scalar_init(GArrowInt16Scalar *object)
{
}

static void
garrow_int16_scalar_class_init(GArrowInt16ScalarClass *klass)
{
}

/**
 * garrow_int16_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowInt16Scalar.
 *
 * Since: 5.0.0
 */
GArrowInt16Scalar *
garrow_int16_scalar_new(gint16 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Int16Scalar>(value));
  return GARROW_INT16_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_int16_scalar_get_value:
 * @scalar: A #GArrowInt16Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint16
garrow_int16_scalar_get_value(GArrowInt16Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Int16Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowInt32Scalar,
              garrow_int32_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_int32_scalar_init(GArrowInt32Scalar *object)
{
}

static void
garrow_int32_scalar_class_init(GArrowInt32ScalarClass *klass)
{
}

/**
 * garrow_int32_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowInt32Scalar.
 *
 * Since: 5.0.0
 */
GArrowInt32Scalar *
garrow_int32_scalar_new(gint32 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Int32Scalar>(value));
  return GARROW_INT32_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_int32_scalar_get_value:
 * @scalar: A #GArrowInt32Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint32
garrow_int32_scalar_get_value(GArrowInt32Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Int32Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowInt64Scalar,
              garrow_int64_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_int64_scalar_init(GArrowInt64Scalar *object)
{
}

static void
garrow_int64_scalar_class_init(GArrowInt64ScalarClass *klass)
{
}

/**
 * garrow_int64_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowInt64Scalar.
 *
 * Since: 5.0.0
 */
GArrowInt64Scalar *
garrow_int64_scalar_new(gint64 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Int64Scalar>(value));
  return GARROW_INT64_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_int64_scalar_get_value:
 * @scalar: A #GArrowInt64Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint64
garrow_int64_scalar_get_value(GArrowInt64Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Int64Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowUInt8Scalar,
              garrow_uint8_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_uint8_scalar_init(GArrowUInt8Scalar *object)
{
}

static void
garrow_uint8_scalar_class_init(GArrowUInt8ScalarClass *klass)
{
}

/**
 * garrow_uint8_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowUInt8Scalar.
 *
 * Since: 5.0.0
 */
GArrowUInt8Scalar *
garrow_uint8_scalar_new(guint8 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::UInt8Scalar>(value));
  return GARROW_UINT8_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_uint8_scalar_get_value:
 * @scalar: A #GArrowUInt8Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
guint8
garrow_uint8_scalar_get_value(GArrowUInt8Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::UInt8Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowUInt16Scalar,
              garrow_uint16_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_uint16_scalar_init(GArrowUInt16Scalar *object)
{
}

static void
garrow_uint16_scalar_class_init(GArrowUInt16ScalarClass *klass)
{
}

/**
 * garrow_uint16_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowUInt16Scalar.
 *
 * Since: 5.0.0
 */
GArrowUInt16Scalar *
garrow_uint16_scalar_new(guint16 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::UInt16Scalar>(value));
  return GARROW_UINT16_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_uint16_scalar_get_value:
 * @scalar: A #GArrowUInt16Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
guint16
garrow_uint16_scalar_get_value(GArrowUInt16Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::UInt16Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowUInt32Scalar,
              garrow_uint32_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_uint32_scalar_init(GArrowUInt32Scalar *object)
{
}

static void
garrow_uint32_scalar_class_init(GArrowUInt32ScalarClass *klass)
{
}

/**
 * garrow_uint32_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowUInt32Scalar.
 *
 * Since: 5.0.0
 */
GArrowUInt32Scalar *
garrow_uint32_scalar_new(guint32 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::UInt32Scalar>(value));
  return GARROW_UINT32_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_uint32_scalar_get_value:
 * @scalar: A #GArrowUInt32Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
guint32
garrow_uint32_scalar_get_value(GArrowUInt32Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::UInt32Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowUInt64Scalar,
              garrow_uint64_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_uint64_scalar_init(GArrowUInt64Scalar *object)
{
}

static void
garrow_uint64_scalar_class_init(GArrowUInt64ScalarClass *klass)
{
}

/**
 * garrow_uint64_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowUInt64Scalar.
 *
 * Since: 5.0.0
 */
GArrowUInt64Scalar *
garrow_uint64_scalar_new(guint64 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::UInt64Scalar>(value));
  return GARROW_UINT64_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_uint64_scalar_get_value:
 * @scalar: A #GArrowUInt64Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
guint64
garrow_uint64_scalar_get_value(GArrowUInt64Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::UInt64Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowHalfFloatScalar,
              garrow_half_float_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_half_float_scalar_init(GArrowHalfFloatScalar *object)
{
}

static void
garrow_half_float_scalar_class_init(GArrowHalfFloatScalarClass *klass)
{
}

/**
 * garrow_half_float_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowHalfFloatScalar.
 *
 * Since: 11.0.0
 */
GArrowHalfFloatScalar *
garrow_half_float_scalar_new(guint16 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::HalfFloatScalar>(value));
  return GARROW_HALF_FLOAT_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_half_float_scalar_get_value:
 * @scalar: A #GArrowHalfFloatScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 11.0.0
 */
guint16
garrow_half_float_scalar_get_value(GArrowHalfFloatScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::HalfFloatScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowFloatScalar,
              garrow_float_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_float_scalar_init(GArrowFloatScalar *object)
{
}

static void
garrow_float_scalar_class_init(GArrowFloatScalarClass *klass)
{
}

/**
 * garrow_float_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowFloatScalar.
 *
 * Since: 5.0.0
 */
GArrowFloatScalar *
garrow_float_scalar_new(gfloat value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::FloatScalar>(value));
  return GARROW_FLOAT_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_float_scalar_get_value:
 * @scalar: A #GArrowFloatScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gfloat
garrow_float_scalar_get_value(GArrowFloatScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::FloatScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowDoubleScalar,
              garrow_double_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_double_scalar_init(GArrowDoubleScalar *object)
{
}

static void
garrow_double_scalar_class_init(GArrowDoubleScalarClass *klass)
{
}

/**
 * garrow_double_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDoubleScalar.
 *
 * Since: 5.0.0
 */
GArrowDoubleScalar *
garrow_double_scalar_new(gdouble value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::DoubleScalar>(value));
  return GARROW_DOUBLE_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_double_scalar_get_value:
 * @scalar: A #GArrowDoubleScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gdouble
garrow_double_scalar_get_value(GArrowDoubleScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::DoubleScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


typedef struct GArrowBaseBinaryScalarPrivate_ {
  GArrowBuffer *value;
} GArrowBaseBinaryScalarPrivate;

enum {
  PROP_VALUE = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowBaseBinaryScalar,
                                    garrow_base_binary_scalar,
                                    GARROW_TYPE_SCALAR)

#define GARROW_BASE_BINARY_SCALAR_GET_PRIVATE(obj)            \
  static_cast<GArrowBaseBinaryScalarPrivate *>(               \
    garrow_base_binary_scalar_get_instance_private(           \
      GARROW_BASE_BINARY_SCALAR(obj)))

static void
garrow_base_binary_scalar_dispose(GObject *object)
{
  auto priv = GARROW_BASE_BINARY_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_base_binary_scalar_parent_class)->dispose(object);
}

static void
garrow_base_binary_scalar_set_property(GObject *object,
                                       guint prop_id,
                                       const GValue *value,
                                       GParamSpec *pspec)
{
  auto priv = GARROW_BASE_BINARY_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_BUFFER(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_base_binary_scalar_init(GArrowBaseBinaryScalar *object)
{
}

static void
garrow_base_binary_scalar_class_init(GArrowBaseBinaryScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = garrow_base_binary_scalar_dispose;
  gobject_class->set_property = garrow_base_binary_scalar_set_property;

  GParamSpec *spec;
  /**
   * GArrowBaseBinaryScalar:value:
   *
   * The value of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value of the scalar",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

G_END_DECLS
template<typename ArrowBinaryScalarType>
GArrowScalar *
garrow_base_binary_scalar_new(GArrowBuffer *value)
{
  auto arrow_value = garrow_buffer_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<ArrowBinaryScalarType>(arrow_value));
  return garrow_scalar_new_raw(&arrow_scalar,
                               "scalar", &arrow_scalar,
                               "value", value,
                               NULL);
}
G_BEGIN_DECLS

/**
 * garrow_base_binary_scalar_get_value:
 * @scalar: A #GArrowBaseBinaryScalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 5.0.0
 */
GArrowBuffer *
garrow_base_binary_scalar_get_value(GArrowBaseBinaryScalar *scalar)
{
  auto priv = GARROW_BASE_BINARY_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    const auto arrow_scalar =
      std::static_pointer_cast<arrow::BaseBinaryScalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    priv->value = garrow_buffer_new_raw(&(arrow_scalar->value));
  }
  return priv->value;
}


G_DEFINE_TYPE(GArrowBinaryScalar,
              garrow_binary_scalar,
              GARROW_TYPE_BASE_BINARY_SCALAR)

static void
garrow_binary_scalar_init(GArrowBinaryScalar *object)
{
}

static void
garrow_binary_scalar_class_init(GArrowBinaryScalarClass *klass)
{
}

/**
 * garrow_binary_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowBinaryScalar.
 *
 * Since: 5.0.0
 */
GArrowBinaryScalar *
garrow_binary_scalar_new(GArrowBuffer *value)
{
  return GARROW_BINARY_SCALAR(
    garrow_base_binary_scalar_new<arrow::BinaryScalar>(value));
}


G_DEFINE_TYPE(GArrowStringScalar,
              garrow_string_scalar,
              GARROW_TYPE_BASE_BINARY_SCALAR)

static void
garrow_string_scalar_init(GArrowStringScalar *object)
{
}

static void
garrow_string_scalar_class_init(GArrowStringScalarClass *klass)
{
}

/**
 * garrow_string_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowStringScalar.
 *
 * Since: 5.0.0
 */
GArrowStringScalar *
garrow_string_scalar_new(GArrowBuffer *value)
{
  return GARROW_STRING_SCALAR(
    garrow_base_binary_scalar_new<arrow::StringScalar>(value));
}


G_DEFINE_TYPE(GArrowLargeBinaryScalar,
              garrow_large_binary_scalar,
              GARROW_TYPE_BASE_BINARY_SCALAR)

static void
garrow_large_binary_scalar_init(GArrowLargeBinaryScalar *object)
{
}

static void
garrow_large_binary_scalar_class_init(GArrowLargeBinaryScalarClass *klass)
{
}

/**
 * garrow_large_binary_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowLargeBinaryScalar.
 *
 * Since: 5.0.0
 */
GArrowLargeBinaryScalar *
garrow_large_binary_scalar_new(GArrowBuffer *value)
{
  return GARROW_LARGE_BINARY_SCALAR(
    garrow_base_binary_scalar_new<arrow::LargeBinaryScalar>(value));
}


G_DEFINE_TYPE(GArrowLargeStringScalar,
              garrow_large_string_scalar,
              GARROW_TYPE_BASE_BINARY_SCALAR)

static void
garrow_large_string_scalar_init(GArrowLargeStringScalar *object)
{
}

static void
garrow_large_string_scalar_class_init(GArrowLargeStringScalarClass *klass)
{
}

/**
 * garrow_large_string_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowLargeStringScalar.
 *
 * Since: 5.0.0
 */
GArrowLargeStringScalar *
garrow_large_string_scalar_new(GArrowBuffer *value)
{
  return GARROW_LARGE_STRING_SCALAR(
    garrow_base_binary_scalar_new<arrow::LargeStringScalar>(value));
}


G_DEFINE_TYPE(GArrowFixedSizeBinaryScalar,
              garrow_fixed_size_binary_scalar,
              GARROW_TYPE_BASE_BINARY_SCALAR)

static void
garrow_fixed_size_binary_scalar_init(GArrowFixedSizeBinaryScalar *object)
{
}

static void
garrow_fixed_size_binary_scalar_class_init(
  GArrowFixedSizeBinaryScalarClass *klass)
{
}

/**
 * garrow_fixed_size_binary_scalar_new:
 * @data_type: A #GArrowFixedSizeBinaryDataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowFixedSizeBinaryScalar.
 *
 * Since: 5.0.0
 */
GArrowFixedSizeBinaryScalar *
garrow_fixed_size_binary_scalar_new(GArrowFixedSizeBinaryDataType *data_type,
                                    GArrowBuffer *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_value = garrow_buffer_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::FixedSizeBinaryScalar>(
        arrow_value, arrow_data_type));
  return GARROW_FIXED_SIZE_BINARY_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar,
                          "scalar", &arrow_scalar,
                          "data-type", data_type,
                          "value", value,
                          NULL));
}


G_DEFINE_TYPE(GArrowDate32Scalar,
              garrow_date32_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_date32_scalar_init(GArrowDate32Scalar *object)
{
}

static void
garrow_date32_scalar_class_init(GArrowDate32ScalarClass *klass)
{
}

/**
 * garrow_date32_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDate32Scalar.
 *
 * Since: 5.0.0
 */
GArrowDate32Scalar *
garrow_date32_scalar_new(gint32 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Date32Scalar>(value));
  return GARROW_DATE32_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_date32_scalar_get_value:
 * @scalar: A #GArrowDate32Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint32
garrow_date32_scalar_get_value(GArrowDate32Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Date32Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowDate64Scalar,
              garrow_date64_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_date64_scalar_init(GArrowDate64Scalar *object)
{
}

static void
garrow_date64_scalar_class_init(GArrowDate64ScalarClass *klass)
{
}

/**
 * garrow_date64_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDate64Scalar.
 *
 * Since: 5.0.0
 */
GArrowDate64Scalar *
garrow_date64_scalar_new(gint64 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Date64Scalar>(value));
  return GARROW_DATE64_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_date64_scalar_get_value:
 * @scalar: A #GArrowDate64Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint64
garrow_date64_scalar_get_value(GArrowDate64Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Date64Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowTime32Scalar,
              garrow_time32_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_time32_scalar_init(GArrowTime32Scalar *object)
{
}

static void
garrow_time32_scalar_class_init(GArrowTime32ScalarClass *klass)
{
}

/**
 * garrow_time32_scalar_new:
 * @data_type: A #GArrowTime32DataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowTime32Scalar.
 *
 * Since: 5.0.0
 */
GArrowTime32Scalar *
garrow_time32_scalar_new(GArrowTime32DataType *data_type,
                         gint32 value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Time32Scalar>(value, arrow_data_type));
  return GARROW_TIME32_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar,
                          "scalar", &arrow_scalar,
                          "data-type", data_type,
                          NULL));
}

/**
 * garrow_time32_scalar_get_value:
 * @scalar: A #GArrowTime32Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint32
garrow_time32_scalar_get_value(GArrowTime32Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Time32Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowTime64Scalar,
              garrow_time64_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_time64_scalar_init(GArrowTime64Scalar *object)
{
}

static void
garrow_time64_scalar_class_init(GArrowTime64ScalarClass *klass)
{
}

/**
 * garrow_time64_scalar_new:
 * @data_type: A #GArrowTime64DataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowTime64Scalar.
 *
 * Since: 5.0.0
 */
GArrowTime64Scalar *
garrow_time64_scalar_new(GArrowTime64DataType *data_type,
                         gint64 value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Time64Scalar>(value, arrow_data_type));
  return GARROW_TIME64_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar,
                          "scalar", &arrow_scalar,
                          "data-type", data_type,
                          NULL));
}

/**
 * garrow_time64_scalar_get_value:
 * @scalar: A #GArrowTime64Scalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint64
garrow_time64_scalar_get_value(GArrowTime64Scalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::Time64Scalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowTimestampScalar,
              garrow_timestamp_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_timestamp_scalar_init(GArrowTimestampScalar *object)
{
}

static void
garrow_timestamp_scalar_class_init(GArrowTimestampScalarClass *klass)
{
}

/**
 * garrow_timestamp_scalar_new:
 * @data_type: A #GArrowTimestampDataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowTimestampScalar.
 *
 * Since: 5.0.0
 */
GArrowTimestampScalar *
garrow_timestamp_scalar_new(GArrowTimestampDataType *data_type,
                            gint64 value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::TimestampScalar>(value, arrow_data_type));
  return GARROW_TIMESTAMP_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar,
                          "scalar", &arrow_scalar,
                          "data-type", data_type,
                          NULL));
}

/**
 * garrow_timestamp_scalar_get_value:
 * @scalar: A #GArrowTimestampScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 5.0.0
 */
gint64
garrow_timestamp_scalar_get_value(GArrowTimestampScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::TimestampScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


G_DEFINE_TYPE(GArrowMonthIntervalScalar,
              garrow_month_interval_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_month_interval_scalar_init(GArrowMonthIntervalScalar *object)
{
}

static void
garrow_month_interval_scalar_class_init(GArrowMonthIntervalScalarClass *klass)
{
}

/**
 * garrow_month_interval_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowMonthIntervalScalar.
 *
 * Since: 8.0.0
 */
GArrowMonthIntervalScalar *
garrow_month_interval_scalar_new(gint32 value)
{
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::MonthIntervalScalar>(value));
  return GARROW_MONTH_INTERVAL_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_month_interval_scalar_get_value:
 * @scalar: A #GArrowMonthIntervalScalar.
 *
 * Returns: The value of this scalar.
 *
 * Since: 8.0.0
 */
gint32
garrow_month_interval_scalar_get_value(GArrowMonthIntervalScalar *scalar)
{
  const auto arrow_scalar =
    std::static_pointer_cast<arrow::MonthIntervalScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->value;
}


typedef struct GArrowDayTimeIntervalScalarPrivate_ {
  GArrowDayMillisecond *value;
} GArrowDayTimeIntervalScalarPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDayTimeIntervalScalar,
                           garrow_day_time_interval_scalar,
                           GARROW_TYPE_SCALAR)

#define GARROW_DAY_TIME_INTERVAL_SCALAR_GET_PRIVATE(obj)         \
  static_cast<GArrowDayTimeIntervalScalarPrivate *>(             \
    garrow_day_time_interval_scalar_get_instance_private(        \
      GARROW_DAY_TIME_INTERVAL_SCALAR(obj)))

static void
garrow_day_time_interval_scalar_init(GArrowDayTimeIntervalScalar *object)
{
}

static void
garrow_day_time_interval_scalar_class_init(
  GArrowDayTimeIntervalScalarClass *klass)
{
}

/**
 * garrow_day_time_interval_scalar_new:
 * @value: The value of GArrowDayMillisecond.
 *
 * Returns: A newly created #GArrowDayTimeIntervalScalar.
 *
 * Since: 8.0.0
 */
GArrowDayTimeIntervalScalar *
garrow_day_time_interval_scalar_new(GArrowDayMillisecond *value)
{
  auto arrow_value = garrow_day_millisecond_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::DayTimeIntervalScalar>(*arrow_value));
  return GARROW_DAY_TIME_INTERVAL_SCALAR(garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_day_time_interval_scalar_get_value:
 * @scalar: A #GArrowDayTimeIntervalScalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 8.0.0
 */
GArrowDayMillisecond *
garrow_day_time_interval_scalar_get_value(GArrowDayTimeIntervalScalar *scalar)
{
  auto priv = GARROW_DAY_TIME_INTERVAL_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    auto arrow_scalar =
      std::static_pointer_cast<arrow::DayTimeIntervalScalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    auto arrow_value = arrow_scalar->value;
    priv->value = garrow_day_millisecond_new_raw(&arrow_value);
  }

  return priv->value;
}


typedef struct GArrowMonthDayNanoIntervalScalarPrivate_ {
  GArrowMonthDayNano *value;
} GArrowMonthDayNanoIntervalScalarPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowMonthDayNanoIntervalScalar,
                           garrow_month_day_nano_interval_scalar,
                           GARROW_TYPE_SCALAR)

#define GARROW_MONTH_DAY_NANO_INTERVAL_SCALAR_GET_PRIVATE(obj)         \
  static_cast<GArrowMonthDayNanoIntervalScalarPrivate *>(              \
    garrow_month_day_nano_interval_scalar_get_instance_private(        \
      GARROW_MONTH_DAY_NANO_INTERVAL_SCALAR(obj)))

static void
garrow_month_day_nano_interval_scalar_init(
  GArrowMonthDayNanoIntervalScalar *object)
{
}

static void
garrow_month_day_nano_interval_scalar_class_init(
  GArrowMonthDayNanoIntervalScalarClass *klass)
{
}

/**
 * garrow_month_day_nano_interval_scalar_new:
 * @value: The value of GArrowMonthDayNano.
 *
 * Returns: A newly created #GArrowMonthDayNanoIntervalScalar.
 *
 * Since: 8.0.0
 */
GArrowMonthDayNanoIntervalScalar *
garrow_month_day_nano_interval_scalar_new(GArrowMonthDayNano *value)
{
  auto arrow_value = garrow_month_day_nano_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::MonthDayNanoIntervalScalar>(*arrow_value));
  return GARROW_MONTH_DAY_NANO_INTERVAL_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar));
}

/**
 * garrow_month_day_nano_interval_scalar_get_value:
 * @scalar: A #GArrowMonthDayNanoIntervalScalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 8.0.0
 */
GArrowMonthDayNano *
garrow_month_day_nano_interval_scalar_get_value(GArrowMonthDayNanoIntervalScalar *scalar)
{
  auto priv = GARROW_MONTH_DAY_NANO_INTERVAL_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    auto arrow_scalar =
      std::static_pointer_cast<arrow::MonthDayNanoIntervalScalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    priv->value = garrow_month_day_nano_new_raw(&arrow_scalar->value);
  }

  return priv->value;
}


typedef struct GArrowDecimal128ScalarPrivate_ {
  GArrowDecimal128 *value;
} GArrowDecimal128ScalarPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal128Scalar,
                           garrow_decimal128_scalar,
                           GARROW_TYPE_SCALAR)

#define GARROW_DECIMAL128_SCALAR_GET_PRIVATE(obj)            \
  static_cast<GArrowDecimal128ScalarPrivate *>(              \
    garrow_decimal128_scalar_get_instance_private(           \
      GARROW_DECIMAL128_SCALAR(obj)))

static void
garrow_decimal128_scalar_dispose(GObject *object)
{
  auto priv = GARROW_DECIMAL128_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_decimal128_scalar_parent_class)->dispose(object);
}

static void
garrow_decimal128_scalar_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL128_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_DECIMAL128(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal128_scalar_init(GArrowDecimal128Scalar *object)
{
}

static void
garrow_decimal128_scalar_class_init(GArrowDecimal128ScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_decimal128_scalar_dispose;
  gobject_class->set_property = garrow_decimal128_scalar_set_property;

  GParamSpec *spec;
  /**
   * GArrowDecimal128Scalar:value:
   *
   * The value of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value of the scalar",
                             garrow_decimal128_get_type(),
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_decimal128_scalar_new:
 * @data_type: A #GArrowDecimal128DataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDecimal128Scalar.
 *
 * Since: 5.0.0
 */
GArrowDecimal128Scalar *
garrow_decimal128_scalar_new(GArrowDecimal128DataType *data_type,
                             GArrowDecimal128 *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_value = garrow_decimal128_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Decimal128Scalar>(*arrow_value, arrow_data_type));
  return GARROW_DECIMAL128_SCALAR(
    garrow_scalar_new_raw(&arrow_scalar,
                          "scalar", &arrow_scalar,
                          "data-type", data_type,
                          "value", value,
                          NULL));
}

/**
 * garrow_decimal128_scalar_get_value:
 * @scalar: A #GArrowDecimal128Scalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 5.0.0
 */
GArrowDecimal128 *
garrow_decimal128_scalar_get_value(GArrowDecimal128Scalar *scalar)
{
  auto priv = GARROW_DECIMAL128_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    auto arrow_scalar =
      std::static_pointer_cast<arrow::Decimal128Scalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    auto arrow_value = std::make_shared<arrow::Decimal128>(arrow_scalar->value);
    priv->value = garrow_decimal128_new_raw(&arrow_value);
  }
  return priv->value;
}


typedef struct GArrowDecimal256ScalarPrivate_ {
  GArrowDecimal256 *value;
} GArrowDecimal256ScalarPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDecimal256Scalar,
                           garrow_decimal256_scalar,
                           GARROW_TYPE_SCALAR)

#define GARROW_DECIMAL256_SCALAR_GET_PRIVATE(obj)            \
  static_cast<GArrowDecimal256ScalarPrivate *>(              \
    garrow_decimal256_scalar_get_instance_private(           \
      GARROW_DECIMAL256_SCALAR(obj)))

static void
garrow_decimal256_scalar_dispose(GObject *object)
{
  auto priv = GARROW_DECIMAL256_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_decimal256_scalar_parent_class)->dispose(object);
}

static void
garrow_decimal256_scalar_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_DECIMAL256_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_DECIMAL256(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_decimal256_scalar_init(GArrowDecimal256Scalar *object)
{
}

static void
garrow_decimal256_scalar_class_init(GArrowDecimal256ScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_decimal256_scalar_dispose;
  gobject_class->set_property = garrow_decimal256_scalar_set_property;

  GParamSpec *spec;
  /**
   * GArrowDecimal256Scalar:value:
   *
   * The value of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value of the scalar",
                             garrow_decimal256_get_type(),
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

/**
 * garrow_decimal256_scalar_new:
 * @data_type: A #GArrowDecimal256DataType for this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDecimal256Scalar.
 *
 * Since: 5.0.0
 */
GArrowDecimal256Scalar *
garrow_decimal256_scalar_new(GArrowDecimal256DataType *data_type,
                             GArrowDecimal256 *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_value = garrow_decimal256_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::Decimal256Scalar>(*arrow_value, arrow_data_type));
  return GARROW_DECIMAL256_SCALAR(garrow_scalar_new_raw(&arrow_scalar,
                                                        "scalar", &arrow_scalar,
                                                        "data-type", data_type,
                                                        "value", value,
                                                        NULL));
}

/**
 * garrow_decimal256_scalar_get_value:
 * @scalar: A #GArrowDecimal256Scalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 5.0.0
 */
GArrowDecimal256 *
garrow_decimal256_scalar_get_value(GArrowDecimal256Scalar *scalar)
{
  auto priv = GARROW_DECIMAL256_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    auto arrow_scalar =
      std::static_pointer_cast<arrow::Decimal256Scalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    auto arrow_value = std::make_shared<arrow::Decimal256>(arrow_scalar->value);
    priv->value = garrow_decimal256_new_raw(&arrow_value);
  }
  return priv->value;
}


typedef struct GArrowBaseListScalarPrivate_ {
  GArrowArray *value;
} GArrowBaseListScalarPrivate;

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowBaseListScalar,
                                    garrow_base_list_scalar,
                                    GARROW_TYPE_SCALAR)

#define GARROW_BASE_LIST_SCALAR_GET_PRIVATE(obj)            \
  static_cast<GArrowBaseListScalarPrivate *>(               \
    garrow_base_list_scalar_get_instance_private(           \
      GARROW_BASE_LIST_SCALAR(obj)))

static void
garrow_base_list_scalar_dispose(GObject *object)
{
  auto priv = GARROW_BASE_LIST_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_base_list_scalar_parent_class)->dispose(object);
}

static void
garrow_base_list_scalar_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_BASE_LIST_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_base_list_scalar_init(GArrowBaseListScalar *object)
{
}

static void
garrow_base_list_scalar_class_init(GArrowBaseListScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_base_list_scalar_dispose;
  gobject_class->set_property = garrow_base_list_scalar_set_property;

  GParamSpec *spec;
  /**
   * GArrowBaseListScalar:value:
   *
   * The value of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value of the scalar",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

G_END_DECLS
template<typename ArrowListScalarType>
GArrowScalar *
garrow_base_list_scalar_new(GArrowArray *value)
{
  auto arrow_value = garrow_array_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<ArrowListScalarType>(arrow_value));
  auto data_type = garrow_array_get_value_data_type(value);
  auto scalar = garrow_scalar_new_raw(&arrow_scalar,
                                      "scalar", &arrow_scalar,
                                      "data-type", data_type,
                                      "value", value,
                                      NULL);
  g_object_unref(data_type);
  return scalar;
}
G_BEGIN_DECLS

/**
 * garrow_base_list_scalar_get_value:
 * @scalar: A #GArrowBaseListScalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 5.0.0
 */
GArrowArray *
garrow_base_list_scalar_get_value(GArrowBaseListScalar *scalar)
{
  auto priv = GARROW_BASE_LIST_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    const auto arrow_scalar =
      std::static_pointer_cast<arrow::BaseListScalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    priv->value = garrow_array_new_raw(&(arrow_scalar->value));
  }
  return priv->value;
}


G_DEFINE_TYPE(GArrowListScalar,
              garrow_list_scalar,
              GARROW_TYPE_BASE_LIST_SCALAR)

static void
garrow_list_scalar_init(GArrowListScalar *object)
{
}

static void
garrow_list_scalar_class_init(GArrowListScalarClass *klass)
{
}

/**
 * garrow_list_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowListScalar.
 *
 * Since: 5.0.0
 */
GArrowListScalar *
garrow_list_scalar_new(GArrowListArray *value)
{
  return GARROW_LIST_SCALAR(
    garrow_base_list_scalar_new<arrow::ListScalar>(GARROW_ARRAY(value)));
}


G_DEFINE_TYPE(GArrowLargeListScalar,
              garrow_large_list_scalar,
              GARROW_TYPE_BASE_LIST_SCALAR)

static void
garrow_large_list_scalar_init(GArrowLargeListScalar *object)
{
}

static void
garrow_large_list_scalar_class_init(GArrowLargeListScalarClass *klass)
{
}

/**
 * garrow_large_list_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowLargeListScalar.
 *
 * Since: 5.0.0
 */
GArrowLargeListScalar *
garrow_large_list_scalar_new(GArrowLargeListArray *value)
{
  return GARROW_LARGE_LIST_SCALAR(
    garrow_base_list_scalar_new<arrow::LargeListScalar>(GARROW_ARRAY(value)));
}


G_DEFINE_TYPE(GArrowMapScalar,
              garrow_map_scalar,
              GARROW_TYPE_BASE_LIST_SCALAR)

static void
garrow_map_scalar_init(GArrowMapScalar *object)
{
}

static void
garrow_map_scalar_class_init(GArrowMapScalarClass *klass)
{
}

/**
 * garrow_map_scalar_new:
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowMapScalar.
 *
 * Since: 5.0.0
 */
GArrowMapScalar *
garrow_map_scalar_new(GArrowStructArray *value)
{
  return GARROW_MAP_SCALAR(
    garrow_base_list_scalar_new<arrow::MapScalar>(GARROW_ARRAY(value)));
}


typedef struct GArrowStructScalarPrivate_ {
  GList *value;
} GArrowStructScalarPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowStructScalar,
                           garrow_struct_scalar,
                           GARROW_TYPE_SCALAR)

#define GARROW_STRUCT_SCALAR_GET_PRIVATE(obj)             \
  static_cast<GArrowStructScalarPrivate *>(               \
    garrow_struct_scalar_get_instance_private(            \
      GARROW_STRUCT_SCALAR(obj)))

static void
garrow_struct_scalar_dispose(GObject *object)
{
  auto priv = GARROW_STRUCT_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_list_free_full(priv->value, g_object_unref);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_struct_scalar_parent_class)->dispose(object);
}

static void
garrow_struct_scalar_init(GArrowStructScalar *object)
{
}

static void
garrow_struct_scalar_class_init(GArrowStructScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_struct_scalar_dispose;
}

/**
 * garrow_struct_scalar_new:
 * @data_type: A #GArrowStructDataType for this scalar.
 * @value: (element-type GArrowScalar): The value of this scalar.
 *
 * Returns: A newly created #GArrowDecimal256Scalar.
 *
 * Since: 5.0.0
 */
GArrowStructScalar *
garrow_struct_scalar_new(GArrowStructDataType *data_type,
                         GList *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  std::vector<std::shared_ptr<arrow::Scalar>> arrow_value;
  for (GList *node = value; node; node = node->next) {
    auto field = GARROW_SCALAR(node->data);
    auto arrow_field = garrow_scalar_get_raw(field);
    arrow_value.push_back(arrow_field);
  }
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::StructScalar>(arrow_value, arrow_data_type));
  auto scalar =
    GARROW_STRUCT_SCALAR(
      garrow_scalar_new_raw(&arrow_scalar,
                            "scalar", &arrow_scalar,
                            "data-type", data_type,
                            NULL));
  auto priv = GARROW_STRUCT_SCALAR_GET_PRIVATE(scalar);
  priv->value = g_list_copy_deep(value,
                                 reinterpret_cast<GCopyFunc>(g_object_ref),
                                 NULL);
  return scalar;
}

/**
 * garrow_struct_scalar_get_value:
 * @scalar: A #GArrowStructScalar.
 *
 * Returns: (element-type GArrowScalar) (transfer none):
 *   The value of this scalar.
 *
 * Since: 5.0.0
 */
GList *
garrow_struct_scalar_get_value(GArrowStructScalar *scalar)
{
  auto priv = GARROW_STRUCT_SCALAR_GET_PRIVATE(scalar);
  if (!priv->value) {
    auto arrow_scalar =
      std::static_pointer_cast<arrow::StructScalar>(
        garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
    for (auto arrow_element : arrow_scalar->value) {
      priv->value = g_list_prepend(priv->value,
                                   garrow_scalar_new_raw(&arrow_element));
    }
    priv->value = g_list_reverse(priv->value);
  }
  return priv->value;
}


typedef struct GArrowUnionScalarPrivate_ {
  GArrowScalar *value;
} GArrowUnionScalarPrivate;

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowUnionScalar,
                                    garrow_union_scalar,
                                    GARROW_TYPE_SCALAR)

#define GARROW_UNION_SCALAR_GET_PRIVATE(obj)             \
  static_cast<GArrowUnionScalarPrivate *>(               \
    garrow_union_scalar_get_instance_private(            \
      GARROW_UNION_SCALAR(obj)))

static void
garrow_union_scalar_dispose(GObject *object)
{
  auto priv = GARROW_UNION_SCALAR_GET_PRIVATE(object);

  if (priv->value) {
    g_object_unref(priv->value);
    priv->value = NULL;
  }

  G_OBJECT_CLASS(garrow_union_scalar_parent_class)->dispose(object);
}

static void
garrow_union_scalar_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_UNION_SCALAR_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE:
    priv->value = GARROW_SCALAR(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_union_scalar_init(GArrowUnionScalar *object)
{
}

static void
garrow_union_scalar_class_init(GArrowUnionScalarClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose      = garrow_union_scalar_dispose;
  gobject_class->set_property = garrow_union_scalar_set_property;

  GParamSpec *spec;
  /**
   * GArrowUnionScalar:value:
   *
   * The value of the scalar.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("value",
                             "Value",
                             "The value of the scalar",
                             GARROW_TYPE_SCALAR,
                             static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE, spec);
}

G_END_DECLS
template<typename ArrowUnionScalarType>
GArrowScalar *
garrow_union_scalar_new(GArrowDataType *data_type,
                        gint8 type_code,
                        GArrowScalar *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_value = garrow_scalar_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<ArrowUnionScalarType>(arrow_value, type_code,
                                             arrow_data_type));
  auto scalar = garrow_scalar_new_raw(&arrow_scalar,
                                      "scalar", &arrow_scalar,
                                      "data-type", data_type,
                                      "value", value,
                                      NULL);
  return scalar;
}
G_BEGIN_DECLS

/**
 * garrow_union_scalar_get_type_code:
 * @scalar: A #GArrowUnionScalar.
 *
 * Returns: The type code of this scalar.
 *
 * Since: 6.0.0
 */
gint8
garrow_union_scalar_get_type_code(GArrowUnionScalar *scalar)
{
  const auto &arrow_scalar =
    std::static_pointer_cast<arrow::UnionScalar>(
      garrow_scalar_get_raw(GARROW_SCALAR(scalar)));
  return arrow_scalar->type_code;
}

/**
 * garrow_union_scalar_get_value:
 * @scalar: A #GArrowUnionScalar.
 *
 * Returns: (transfer none): The value of this scalar.
 *
 * Since: 5.0.0
 */
GArrowScalar *
garrow_union_scalar_get_value(GArrowUnionScalar *scalar)
{
  auto priv = GARROW_UNION_SCALAR_GET_PRIVATE(scalar);
  return priv->value;
}


G_DEFINE_TYPE(GArrowSparseUnionScalar,
              garrow_sparse_union_scalar,
              GARROW_TYPE_UNION_SCALAR)

static void
garrow_sparse_union_scalar_init(GArrowSparseUnionScalar *object)
{
}

static void
garrow_sparse_union_scalar_class_init(GArrowSparseUnionScalarClass *klass)
{
}

/**
 * garrow_sparse_union_scalar_new:
 * @data_type: A #GArrowSparseUnionDataType for this scalar.
 * @type_code: The type code of this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowSparseUnionScalar.
 *
 * Since: 5.0.0
 */
GArrowSparseUnionScalar *
garrow_sparse_union_scalar_new(GArrowSparseUnionDataType *data_type,
                               gint8 type_code,
                               GArrowScalar *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  const auto &arrow_type_codes =
    std::dynamic_pointer_cast<arrow::SparseUnionType>(
      arrow_data_type)->type_codes();
  auto arrow_value = garrow_scalar_get_raw(value);
  arrow::SparseUnionScalar::ValueType arrow_field_values;
  for (int i = 0; i < arrow_data_type->num_fields(); ++i) {
    if (arrow_type_codes[i] == type_code) {
      arrow_field_values.emplace_back(arrow_value);
    } else {
      arrow_field_values.emplace_back(
        arrow::MakeNullScalar(arrow_data_type->field(i)->type()));
    }
  }
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::SparseUnionScalar>(arrow_field_values,
                                                 type_code,
                                                 arrow_data_type));
  auto scalar = garrow_scalar_new_raw(&arrow_scalar,
                                      "scalar", &arrow_scalar,
                                      "data-type", data_type,
                                      "value", value,
                                      NULL);
  return GARROW_SPARSE_UNION_SCALAR(scalar);
}


G_DEFINE_TYPE(GArrowDenseUnionScalar,
              garrow_dense_union_scalar,
              GARROW_TYPE_UNION_SCALAR)

static void
garrow_dense_union_scalar_init(GArrowDenseUnionScalar *object)
{
}

static void
garrow_dense_union_scalar_class_init(GArrowDenseUnionScalarClass *klass)
{
}

/**
 * garrow_dense_union_scalar_new:
 * @data_type: A #GArrowDenseUnionDataType for this scalar.
 * @type_code: The type code of this scalar.
 * @value: The value of this scalar.
 *
 * Returns: A newly created #GArrowDenseUnionScalar.
 *
 * Since: 5.0.0
 */
GArrowDenseUnionScalar *
garrow_dense_union_scalar_new(GArrowDenseUnionDataType *data_type,
                              gint8 type_code,
                              GArrowScalar *value)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_value = garrow_scalar_get_raw(value);
  auto arrow_scalar =
    std::static_pointer_cast<arrow::Scalar>(
      std::make_shared<arrow::DenseUnionScalar>(arrow_value,
                                                type_code,
                                                arrow_data_type));
  auto scalar = garrow_scalar_new_raw(&arrow_scalar,
                                      "scalar", &arrow_scalar,
                                      "data-type", data_type,
                                      "value", value,
                                      NULL);
  return GARROW_DENSE_UNION_SCALAR(scalar);
}


G_DEFINE_TYPE(GArrowExtensionScalar,
              garrow_extension_scalar,
              GARROW_TYPE_SCALAR)

static void
garrow_extension_scalar_init(GArrowExtensionScalar *object)
{
}

static void
garrow_extension_scalar_class_init(GArrowExtensionScalarClass *klass)
{
}


G_END_DECLS

GArrowScalar *
garrow_scalar_new_raw(std::shared_ptr<arrow::Scalar> *arrow_scalar)
{
  return garrow_scalar_new_raw(arrow_scalar,
                               "scalar", arrow_scalar,
                               NULL);
}

GArrowScalar *
garrow_scalar_new_raw(std::shared_ptr<arrow::Scalar> *arrow_scalar,
                      const gchar *first_property_name,
                      ...)
{
  va_list args;
  va_start(args, first_property_name);
  auto array = garrow_scalar_new_raw_valist(arrow_scalar,
                                            first_property_name,
                                            args);
  va_end(args);
  return array;
}

GArrowScalar *
garrow_scalar_new_raw_valist(std::shared_ptr<arrow::Scalar> *arrow_scalar,
                             const gchar *first_property_name,
                             va_list args)
{
  GType type;
  GArrowScalar *scalar;

  switch ((*arrow_scalar)->type->id()) {
  case arrow::Type::type::NA:
    type = GARROW_TYPE_NULL_SCALAR;
    break;
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_SCALAR;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_SCALAR;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_SCALAR;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_SCALAR;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_SCALAR;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_SCALAR;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_SCALAR;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_SCALAR;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_SCALAR;
    break;
  case arrow::Type::type::HALF_FLOAT:
    type = GARROW_TYPE_HALF_FLOAT_SCALAR;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_SCALAR;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_SCALAR;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_SCALAR;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_SCALAR;
    break;
  case arrow::Type::type::LARGE_BINARY:
    type = GARROW_TYPE_LARGE_BINARY_SCALAR;
    break;
  case arrow::Type::type::LARGE_STRING:
    type = GARROW_TYPE_LARGE_STRING_SCALAR;
    break;
  case arrow::Type::type::FIXED_SIZE_BINARY:
    type = GARROW_TYPE_FIXED_SIZE_BINARY_SCALAR;
    break;
  case arrow::Type::type::DATE32:
    type = GARROW_TYPE_DATE32_SCALAR;
    break;
  case arrow::Type::type::DATE64:
    type = GARROW_TYPE_DATE64_SCALAR;
    break;
  case arrow::Type::type::TIME32:
    type = GARROW_TYPE_TIME32_SCALAR;
    break;
  case arrow::Type::type::TIME64:
    type = GARROW_TYPE_TIME64_SCALAR;
    break;
  case arrow::Type::type::TIMESTAMP:
    type = GARROW_TYPE_TIMESTAMP_SCALAR;
    break;
  case arrow::Type::type::INTERVAL_MONTHS:
    type = GARROW_TYPE_MONTH_INTERVAL_SCALAR;
    break;
  case arrow::Type::type::INTERVAL_DAY_TIME:
    type = GARROW_TYPE_DAY_TIME_INTERVAL_SCALAR;
    break;
  case arrow::Type::type::INTERVAL_MONTH_DAY_NANO:
    type = GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_SCALAR;
    break;
  case arrow::Type::type::DECIMAL128:
    type = GARROW_TYPE_DECIMAL128_SCALAR;
    break;
  case arrow::Type::type::DECIMAL256:
    type = GARROW_TYPE_DECIMAL256_SCALAR;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_SCALAR;
    break;
  case arrow::Type::type::LARGE_LIST:
    type = GARROW_TYPE_LARGE_LIST_SCALAR;
    break;
/*
  case arrow::Type::type::FIXED_SIZE_LIST:
    type = GARROW_TYPE_FIXED_SIZE_LIST_SCALAR;
    break;
*/
  case arrow::Type::type::MAP:
    type = GARROW_TYPE_MAP_SCALAR;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_SCALAR;
    break;
  case arrow::Type::type::SPARSE_UNION:
    type = GARROW_TYPE_SPARSE_UNION_SCALAR;
    break;
  case arrow::Type::type::DENSE_UNION:
    type = GARROW_TYPE_DENSE_UNION_SCALAR;
    break;
  case arrow::Type::type::EXTENSION:
    type = GARROW_TYPE_EXTENSION_SCALAR;
    break;
  default:
    type = GARROW_TYPE_SCALAR;
    break;
  }
  scalar = GARROW_SCALAR(g_object_new_valist(type,
                                             first_property_name,
                                             args));
  return scalar;
}

std::shared_ptr<arrow::Scalar>
garrow_scalar_get_raw(GArrowScalar *scalar)
{
  auto priv = GARROW_SCALAR_GET_PRIVATE(scalar);
  return priv->scalar;
}
