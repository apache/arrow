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

#include <arrow-glib/array.hpp>
#include <arrow-glib/compute.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>

template <typename ArrowType, typename GArrowArrayType>
typename ArrowType::c_type
garrow_numeric_array_sum(GArrowArrayType array,
                         GError **error,
                         const gchar *tag,
                         typename ArrowType::c_type default_value)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum sum_datum;
  auto status = arrow::compute::Sum(&context,
                                    arrow_array,
                                    &sum_datum);
  if (garrow_error_check(error, status, tag)) {
    using ScalarType = typename arrow::TypeTraits<ArrowType>::ScalarType;
    auto arrow_numeric_scalar =
      std::dynamic_pointer_cast<ScalarType>(sum_datum.scalar());
    if (arrow_numeric_scalar->is_valid) {
      return arrow_numeric_scalar->value;
    } else {
      return default_value;
    }
  } else {
    return default_value;
  }
}

G_BEGIN_DECLS

/**
 * SECTION: compute
 * @section_id: compute
 * @title: Computation on array
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowCastOptions is a class to customize garrow_array_cast().
 *
 * #GArrowCountOptions is a class to customize garrow_array_count().
 *
 * There are many functions to compute data on an array.
 */

typedef struct GArrowCastOptionsPrivate_ {
  arrow::compute::CastOptions options;
} GArrowCastOptionsPrivate;

enum {
  PROP_ALLOW_INT_OVERFLOW = 1,
  PROP_ALLOW_TIME_TRUNCATE,
  PROP_ALLOW_FLOAT_TRUNCATE,
  PROP_ALLOW_INVALID_UTF8,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCastOptions,
                           garrow_cast_options,
                           G_TYPE_OBJECT)

#define GARROW_CAST_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowCastOptionsPrivate *>(      \
    garrow_cast_options_get_instance_private(   \
      GARROW_CAST_OPTIONS(object)))

static void
garrow_cast_options_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ALLOW_INT_OVERFLOW:
    priv->options.allow_int_overflow = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    priv->options.allow_time_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    priv->options.allow_float_truncate = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    priv->options.allow_invalid_utf8 = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ALLOW_INT_OVERFLOW:
    g_value_set_boolean(value, priv->options.allow_int_overflow);
    break;
  case PROP_ALLOW_TIME_TRUNCATE:
    g_value_set_boolean(value, priv->options.allow_time_truncate);
    break;
  case PROP_ALLOW_FLOAT_TRUNCATE:
    g_value_set_boolean(value, priv->options.allow_float_truncate);
    break;
  case PROP_ALLOW_INVALID_UTF8:
    g_value_set_boolean(value, priv->options.allow_invalid_utf8);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_cast_options_init(GArrowCastOptions *object)
{
}

static void
garrow_cast_options_class_init(GArrowCastOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_cast_options_set_property;
  gobject_class->get_property = garrow_cast_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowCastOptions:allow-int-overflow:
   *
   * Whether integer overflow is allowed or not.
   *
   * Since: 0.7.0
   */
  spec = g_param_spec_boolean("allow-int-overflow",
                              "Allow int overflow",
                              "Whether integer overflow is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_INT_OVERFLOW, spec);

  /**
   * GArrowCastOptions:allow-time-truncate:
   *
   * Whether truncating time value is allowed or not.
   *
   * Since: 0.8.0
   */
  spec = g_param_spec_boolean("allow-time-truncate",
                              "Allow time truncate",
                              "Whether truncating time value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_TIME_TRUNCATE, spec);

  /**
   * GArrowCastOptions:allow-float-truncate:
   *
   * Whether truncating float value is allowed or not.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("allow-float-truncate",
                              "Allow float truncate",
                              "Whether truncating float value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_FLOAT_TRUNCATE, spec);

  /**
   * GArrowCastOptions:allow-invalid-utf8:
   *
   * Whether invalid UTF-8 string value is allowed or not.
   *
   * Since: 0.13.0
   */
  spec = g_param_spec_boolean("allow-invalid-utf8",
                              "Allow invalid UTF-8",
                              "Whether invalid UTF-8 string value is allowed or not",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_INVALID_UTF8, spec);
}

/**
 * garrow_cast_options_new:
 *
 * Returns: A newly created #GArrowCastOptions.
 *
 * Since: 0.7.0
 */
GArrowCastOptions *
garrow_cast_options_new(void)
{
  auto cast_options = g_object_new(GARROW_TYPE_CAST_OPTIONS, NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}


typedef struct GArrowCountOptionsPrivate_ {
  arrow::compute::CountOptions options;
} GArrowCountOptionsPrivate;

enum {
  PROP_MODE = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCountOptions,
                           garrow_count_options,
                           G_TYPE_OBJECT)

#define GARROW_COUNT_OPTIONS_GET_PRIVATE(object)        \
  static_cast<GArrowCountOptionsPrivate *>(             \
    garrow_count_options_get_instance_private(          \
      GARROW_COUNT_OPTIONS(object)))

static void
garrow_count_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MODE:
    priv->options.count_mode =
      static_cast<arrow::compute::CountOptions::mode>(g_value_get_enum(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_count_options_get_property(GObject *object,
                                 guint prop_id,
                                 GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_MODE:
    g_value_set_enum(value, priv->options.count_mode);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_count_options_init(GArrowCountOptions *object)
{
}

static void
garrow_count_options_class_init(GArrowCountOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_count_options_set_property;
  gobject_class->get_property = garrow_count_options_get_property;

  GParamSpec *spec;
  /**
   * GArrowCountOptions:mode:
   *
   * How to count values.
   *
   * Since: 0.13.0
   */
  spec = g_param_spec_enum("mode",
                           "Mode",
                           "How to count values",
                           GARROW_TYPE_COUNT_MODE,
                           GARROW_COUNT_ALL,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_MODE, spec);
}

/**
 * garrow_count_options_new:
 *
 * Returns: A newly created #GArrowCountOptions.
 *
 * Since: 0.13.0
 */
GArrowCountOptions *
garrow_count_options_new(void)
{
  auto count_options = g_object_new(GARROW_TYPE_COUNT_OPTIONS, NULL);
  return GARROW_COUNT_OPTIONS(count_options);
}


/**
 * garrow_array_cast:
 * @array: A #GArrowArray.
 * @target_data_type: A #GArrowDataType of cast target data.
 * @options: (nullable): A #GArrowCastOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created casted array on success, %NULL on error.
 *
 * Since: 0.7.0
 */
GArrowArray *
garrow_array_cast(GArrowArray *array,
                  GArrowDataType *target_data_type,
                  GArrowCastOptions *options,
                  GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  auto arrow_target_data_type = garrow_data_type_get_raw(target_data_type);
  std::shared_ptr<arrow::Array> arrow_casted_array;
  arrow::Status status;
  if (options) {
    auto arrow_options = garrow_cast_options_get_raw(options);
    status = arrow::compute::Cast(&context,
                                  *arrow_array_raw,
                                  arrow_target_data_type,
                                  *arrow_options,
                                  &arrow_casted_array);
  } else {
    arrow::compute::CastOptions arrow_options;
    status = arrow::compute::Cast(&context,
                                  *arrow_array_raw,
                                  arrow_target_data_type,
                                  arrow_options,
                                  &arrow_casted_array);
  }

  if (!status.ok()) {
    std::stringstream message;
    message << "[array][cast] <";
    message << arrow_array->type()->ToString();
    message << "> -> <";
    message << arrow_target_data_type->ToString();
    message << ">";
    garrow_error_check(error, status, message.str().c_str());
    return NULL;
  }

  return garrow_array_new_raw(&arrow_casted_array);
}

/**
 * garrow_array_unique:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created unique elements array on success, %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowArray *
garrow_array_unique(GArrowArray *array,
                    GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  std::shared_ptr<arrow::Array> arrow_unique_array;
  auto status = arrow::compute::Unique(&context,
                                       arrow::compute::Datum(arrow_array),
                                       &arrow_unique_array);
  if (!status.ok()) {
    std::stringstream message;
    message << "[array][unique] <";
    message << arrow_array->type()->ToString();
    message << ">";
    garrow_error_check(error, status, message.str().c_str());
    return NULL;
  }

  return garrow_array_new_raw(&arrow_unique_array);
}

/**
 * garrow_array_dictionary_encode:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A newly created #GArrowDictionaryArray for the @array on success,
 *   %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowDictionaryArray *
garrow_array_dictionary_encode(GArrowArray *array,
                               GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum dictionary_encoded_datum;
  auto status =
    arrow::compute::DictionaryEncode(&context,
                                     arrow::compute::Datum(arrow_array),
                                     &dictionary_encoded_datum);
  if (!status.ok()) {
    std::stringstream message;
    message << "[array][dictionary-encode] <";
    message << arrow_array->type()->ToString();
    message << ">";
    garrow_error_check(error, status, message.str().c_str());
    return NULL;
  }

  auto arrow_dictionary_encoded_array =
    arrow::MakeArray(dictionary_encoded_datum.array());
  auto dictionary_encoded_array =
    garrow_array_new_raw(&arrow_dictionary_encoded_array);
  return GARROW_DICTIONARY_ARRAY(dictionary_encoded_array);
}

/**
 * garrow_array_count:
 * @array: A #GArrowArray.
 * @options: (nullable): A #GArrowCountOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The number of target values on success. If an error is occurred,
 *   the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_array_count(GArrowArray *array,
                   GArrowCountOptions *options,
                   GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_array_raw = arrow_array.get();
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum counted_datum;
  arrow::Status status;
  if (options) {
    auto arrow_options = garrow_count_options_get_raw(options);
    status = arrow::compute::Count(&context,
                                   *arrow_options,
                                   *arrow_array_raw,
                                   &counted_datum);
  } else {
    arrow::compute::CountOptions arrow_options(arrow::compute::CountOptions::COUNT_ALL);
    status = arrow::compute::Count(&context,
                                   arrow_options,
                                   *arrow_array_raw,
                                   &counted_datum);
  }

  if (garrow_error_check(error, status, "[array][count]")) {
    using ScalarType = typename arrow::TypeTraits<arrow::Int64Type>::ScalarType;
    auto counted_scalar = std::dynamic_pointer_cast<ScalarType>(counted_datum.scalar());
    return counted_scalar->value;
  } else {
    return 0;
  }
}

/**
 * garrow_array_count_values:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   A #GArrowStructArray of `{input type "values", int64_t "counts"}`
 *   on success, %NULL on error.
 *
 * Since: 0.13.0
 */
GArrowStructArray *
garrow_array_count_values(GArrowArray *array,
                          GError **error)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  std::shared_ptr<arrow::Array> arrow_counted_values;
  auto status = arrow::compute::ValueCounts(&context,
                                            arrow::compute::Datum(arrow_array),
                                            &arrow_counted_values);
  if (garrow_error_check(error, status, "[array][count-values]")) {
    return GARROW_STRUCT_ARRAY(garrow_array_new_raw(&arrow_counted_values));
  } else {
    return NULL;
  }
}


/**
 * garrow_boolean_array_invert:
 * @array: A #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise inverted boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_invert(GArrowBooleanArray *array,
                            GError **error)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto datum = arrow::compute::Datum(arrow_array);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum inverted_datum;
  auto status = arrow::compute::Invert(&context, datum, &inverted_datum);
  if (garrow_error_check(error, status, "[boolean-array][invert]")) {
    auto arrow_inverted_array = inverted_datum.make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_inverted_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_and:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise AND operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_and(GArrowBooleanArray *left,
                         GArrowBooleanArray *right,
                         GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto left_datum = arrow::compute::Datum(arrow_left);
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto right_datum = arrow::compute::Datum(arrow_right);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum operated_datum;
  auto status = arrow::compute::And(&context,
                                    left_datum,
                                    right_datum,
                                    &operated_datum);
  if (garrow_error_check(error, status, "[boolean-array][and]")) {
    auto arrow_operated_array = operated_datum.make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_or:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise OR operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_or(GArrowBooleanArray *left,
                        GArrowBooleanArray *right,
                        GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto left_datum = arrow::compute::Datum(arrow_left);
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto right_datum = arrow::compute::Datum(arrow_right);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum operated_datum;
  auto status = arrow::compute::Or(&context,
                                   left_datum,
                                   right_datum,
                                   &operated_datum);
  if (garrow_error_check(error, status, "[boolean-array][or]")) {
    auto arrow_operated_array = operated_datum.make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_boolean_array_xor:
 * @left: A left hand side #GArrowBooleanArray.
 * @right: A right hand side #GArrowBooleanArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The element-wise XOR operated boolean array.
 *
 *   It should be freed with g_object_unref() when no longer needed.
 *
 * Since: 0.13.0
 */
GArrowBooleanArray *
garrow_boolean_array_xor(GArrowBooleanArray *left,
                         GArrowBooleanArray *right,
                         GError **error)
{
  auto arrow_left = garrow_array_get_raw(GARROW_ARRAY(left));
  auto left_datum = arrow::compute::Datum(arrow_left);
  auto arrow_right = garrow_array_get_raw(GARROW_ARRAY(right));
  auto right_datum = arrow::compute::Datum(arrow_right);
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum operated_datum;
  auto status = arrow::compute::Xor(&context,
                                    left_datum,
                                    right_datum,
                                    &operated_datum);
  if (garrow_error_check(error, status, "[boolean-array][xor]")) {
    auto arrow_operated_array = operated_datum.make_array();
    return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_operated_array));
  } else {
    return NULL;
  }
}


/**
 * garrow_numeric_array_mean:
 * @array: A #GArrowNumericArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed mean.
 *
 * Since: 0.13.0
 */
gdouble
garrow_numeric_array_mean(GArrowNumericArray *array,
                          GError **error)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto memory_pool = arrow::default_memory_pool();
  arrow::compute::FunctionContext context(memory_pool);
  arrow::compute::Datum mean_datum;
  auto status = arrow::compute::Mean(&context, arrow_array, &mean_datum);
  if (garrow_error_check(error, status, "[numeric-array][mean]")) {
    using ScalarType = typename arrow::TypeTraits<arrow::DoubleType>::ScalarType;
    auto arrow_numeric_scalar =
      std::dynamic_pointer_cast<ScalarType>(mean_datum.scalar());
    if (arrow_numeric_scalar->is_valid) {
      return arrow_numeric_scalar->value;
    } else {
      return 0.0;
    }
  } else {
    return 0.0;
  }
}


/**
 * garrow_int8_array_sum:
 * @array: A #GArrowInt8Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int8_array_sum(GArrowInt8Array *array,
                      GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int8-array][sum]",
                                                    0);
}

/**
 * garrow_uint8_array_sum:
 * @array: A #GArrowUInt8Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint8_array_sum(GArrowUInt8Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                     error,
                                                     "[uint8-array][sum]",
                                                     0);
}

/**
 * garrow_int16_array_sum:
 * @array: A #GArrowInt16Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int16_array_sum(GArrowInt16Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int16-array][sum]",
                                                    0);
}

/**
 * garrow_uint16_array_sum:
 * @array: A #GArrowUInt16Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint16_array_sum(GArrowUInt16Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                     error,
                                                     "[uint16-array][sum]",
                                                     0);
}

/**
 * garrow_int32_array_sum:
 * @array: A #GArrowInt32Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int32_array_sum(GArrowInt32Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int32-array][sum]",
                                                    0);
}

/**
 * garrow_uint32_array_sum:
 * @array: A #GArrowUInt32Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint32_array_sum(GArrowUInt32Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                    error,
                                                    "[uint32-array][sum]",
                                                    0);
}

/**
 * garrow_int64_array_sum:
 * @array: A #GArrowInt64Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gint64
garrow_int64_array_sum(GArrowInt64Array *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::Int64Type>(array,
                                                    error,
                                                    "[int64-array][sum]",
                                                    0);
}

/**
 * garrow_uint64_array_sum:
 * @array: A #GArrowUInt64Array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
guint64
garrow_uint64_array_sum(GArrowUInt64Array *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::UInt64Type>(array,
                                                    error,
                                                    "[uint64-array][sum]",
                                                    0);
}

/**
 * garrow_float_array_sum:
 * @array: A #GArrowFloatArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gdouble
garrow_float_array_sum(GArrowFloatArray *array,
                       GError **error)
{
  return garrow_numeric_array_sum<arrow::DoubleType>(array,
                                                     error,
                                                     "[float-array][sum]",
                                                     0);
}

/**
 * garrow_double_array_sum:
 * @array: A #GArrowDoubleArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The value of the computed sum on success,
 *   If an error is occurred, the returned value is untrustful value.
 *
 * Since: 0.13.0
 */
gdouble
garrow_double_array_sum(GArrowDoubleArray *array,
                        GError **error)
{
  return garrow_numeric_array_sum<arrow::DoubleType>(array,
                                                     error,
                                                     "[double-array][sum]",
                                                     0);
}


G_END_DECLS

GArrowCastOptions *
garrow_cast_options_new_raw(arrow::compute::CastOptions *arrow_cast_options)
{
  auto cast_options =
    g_object_new(GARROW_TYPE_CAST_OPTIONS,
                 "allow-int-overflow", arrow_cast_options->allow_int_overflow,
                 "allow-time-truncate", arrow_cast_options->allow_time_truncate,
                 "allow-float-truncate", arrow_cast_options->allow_float_truncate,
                 "allow-invalid-utf8", arrow_cast_options->allow_invalid_utf8,
                 NULL);
  return GARROW_CAST_OPTIONS(cast_options);
}

arrow::compute::CastOptions *
garrow_cast_options_get_raw(GArrowCastOptions *cast_options)
{
  auto priv = GARROW_CAST_OPTIONS_GET_PRIVATE(cast_options);
  return &(priv->options);
}

GArrowCountOptions *
garrow_count_options_new_raw(arrow::compute::CountOptions *arrow_count_options)
{
  auto count_options =
    g_object_new(GARROW_TYPE_COUNT_OPTIONS,
                 "mode", arrow_count_options->count_mode,
                 NULL);
  return GARROW_COUNT_OPTIONS(count_options);
}

arrow::compute::CountOptions *
garrow_count_options_get_raw(GArrowCountOptions *count_options)
{
  auto priv = GARROW_COUNT_OPTIONS_GET_PRIVATE(count_options);
  return &(priv->options);
}
