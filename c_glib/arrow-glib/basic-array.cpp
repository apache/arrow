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
#include <arrow-glib/basic-data-type.hpp>
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/decimal.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/interval.hpp>
#include <arrow-glib/type.hpp>

#include <arrow/c/bridge.h>

#include <sstream>

G_BEGIN_DECLS

/**
 * SECTION: basic-array
 * @section_id: basic-array-classes
 * @title: Basic array classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowArray is a base class for all array classes such as
 * #GArrowBooleanArray.
 *
 * All array classes are immutable. You need to use binary data or
 * array builder to create a new array except #GArrowNullArray. If you
 * have binary data that uses Arrow format data, you can create a new
 * array with the binary data as #GArrowBuffer object. If you don't
 * have binary data, you can use array builder class such as
 * #GArrowBooleanArrayBuilder that creates Arrow format data
 * internally and a new array from the data.
 *
 * #GArrowNullArray is a class for null array. It can store zero or
 * more null values. You need to specify an array length to create a
 * new array.
 *
 * #GArrowBooleanArray is a class for boolean array. It can store zero
 * or more boolean data. If you don't have Arrow format data, you need
 * to use #GArrowBooleanArrayBuilder to create a new array.
 *
 * #GArrowInt8Array is a class for 8-bit integer array. It can store
 * zero or more 8-bit integer data. If you don't have Arrow format
 * data, you need to use #GArrowInt8ArrayBuilder to create a new
 * array.
 *
 * #GArrowUInt8Array is a class for 8-bit unsigned integer array. It
 * can store zero or more 8-bit unsigned integer data. If you don't
 * have Arrow format data, you need to use #GArrowUInt8ArrayBuilder to
 * create a new array.
 *
 * #GArrowInt16Array is a class for 16-bit integer array. It can store
 * zero or more 16-bit integer data. If you don't have Arrow format
 * data, you need to use #GArrowInt16ArrayBuilder to create a new
 * array.
 *
 * #GArrowUInt16Array is a class for 16-bit unsigned integer array. It
 * can store zero or more 16-bit unsigned integer data. If you don't
 * have Arrow format data, you need to use #GArrowUInt16ArrayBuilder
 * to create a new array.
 *
 * #GArrowInt32Array is a class for 32-bit integer array. It can store
 * zero or more 32-bit integer data. If you don't have Arrow format
 * data, you need to use #GArrowInt32ArrayBuilder to create a new
 * array.
 *
 * #GArrowUInt32Array is a class for 32-bit unsigned integer array. It
 * can store zero or more 32-bit unsigned integer data. If you don't
 * have Arrow format data, you need to use #GArrowUInt32ArrayBuilder
 * to create a new array.
 *
 * #GArrowInt64Array is a class for 64-bit integer array. It can store
 * zero or more 64-bit integer data. If you don't have Arrow format
 * data, you need to use #GArrowInt64ArrayBuilder to create a new
 * array.
 *
 * #GArrowUInt64Array is a class for 64-bit unsigned integer array. It
 * can store zero or more 64-bit unsigned integer data. If you don't
 * have Arrow format data, you need to use #GArrowUInt64ArrayBuilder
 * to create a new array.
 *
 * #GArrowFloatArray is a class for 32-bit floating point array. It
 * can store zero or more 32-bit floating data. If you don't have
 * Arrow format data, you need to use #GArrowFloatArrayBuilder to
 * create a new array.
 *
 * #GArrowDoubleArray is a class for 64-bit floating point array. It
 * can store zero or more 64-bit floating data. If you don't have
 * Arrow format data, you need to use #GArrowDoubleArrayBuilder to
 * create a new array.
 *
 * #GArrowBinaryArray is a class for binary array. It can store zero
 * or more binary data. If you don't have Arrow format data, you need
 * to use #GArrowBinaryArrayBuilder to create a new array.
 *
 * #GArrowLargeBinaryArray is a class for 64-bit offsets binary array.
 * It can store zero or more binary data. If you don't have Arrow
 * format data, you need to use #GArrowLargeBinaryArrayBuilder to
 * create a new array.
 *
 * #GArrowStringArray is a class for UTF-8 encoded string array. It
 * can store zero or more UTF-8 encoded string data. If you don't have
 * Arrow format data, you need to use #GArrowStringArrayBuilder to
 * create a new array.
 *
 * #GArrowLargeStringArray is a class for 64-bit offsets UTF-8
 * encoded string array. It can store zero or more UTF-8 encoded
 * string data. If you don't have Arrow format data, you need to
 * use #GArrowLargeStringArrayBuilder to create a new array.
 *
 * #GArrowFixedSizeBinaryArray is a class for fixed size binary array.
 * It can store zero or more fixed size binary data. If you don't have
 * Arrow format data, you need to use
 * #GArrowFixedSizeBinaryArrayBuilder to create a new array.
 *
 * #GArrowDate32Array is a class for the number of days since UNIX
 * epoch in 32-bit signed integer array. It can store zero or more
 * date data. If you don't have Arrow format data, you need to use
 * #GArrowDate32ArrayBuilder to create a new array.
 *
 * #GArrowDate64Array is a class for the number of milliseconds since
 * UNIX epoch in 64-bit signed integer array. It can store zero or
 * more date data. If you don't have Arrow format data, you need to
 * use #GArrowDate64ArrayBuilder to create a new array.
 *
 * #GArrowTimestampArray is a class for the number of
 * seconds/milliseconds/microseconds/nanoseconds since UNIX epoch in
 * 64-bit signed integer array. It can store zero or more timestamp
 * data. If you don't have Arrow format data, you need to use
 * #GArrowTimestampArrayBuilder to create a new array.
 *
 * #GArrowTime32Array is a class for the number of seconds or
 * milliseconds since midnight in 32-bit signed integer array. It can
 * store zero or more time data. If you don't have Arrow format data,
 * you need to use #GArrowTime32ArrayBuilder to create a new array.
 *
 * #GArrowTime64Array is a class for the number of microseconds or
 * nanoseconds since midnight in 64-bit signed integer array. It can
 * store zero or more time data. If you don't have Arrow format data,
 * you need to use #GArrowTime64ArrayBuilder to create a new array.
 *
 * #GArrowMonthIntervalArray is a class for the month intarval array.
 * It can store zero or more date data. If you don't have Arrow format 
 * data, you need to use #GArrowMonthIntervalArrayBuilder to create a 
 * new array.
 *
 * #GArrowDayTimeIntervalArray is a class for the day time intarval array.
 * It can store zero or more date data. If you don't have Arrow format 
 * data, you need to use #GArrowDayTimeIntervalArrayBuilder to create a 
 * new array.
 *
 * #GArrowMonthDayNanoIntervalArray is a class for the month day nano 
 * intarval array. It can store zero or more date data. If you don't
 * have Arrow format data, you need to use #GArrowMonthDayNanoIntervalArray
 * to create a new array.
 *
 * #GArrowDecimal128Array is a class for 128-bit decimal array. It can
 * store zero or more 128-bit decimal data. If you don't have Arrow
 * format data, you need to use #GArrowDecimal128ArrayBuilder to
 * create a new array.
 *
 * #GArrowDecimal256Array is a class for 256-bit decimal array. It can
 * store zero or more 256-bit decimal data. If you don't have Arrow
 * format data, you need to use #GArrowDecimal256ArrayBuilder to
 * create a new array.
 *
 * #GArrowExtensionArray is a base class for array of user-defined
 * extension types.
 */

typedef struct GArrowEqualOptionsPrivate_ {
  gboolean approx;
  arrow::EqualOptions options;
} GArrowEqualOptionsPrivate;

enum {
  PROP_APPROX = 1,
  PROP_NANS_EQUAL,
  PROP_ABSOLUTE_TOLERANCE,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowEqualOptions,
                           garrow_equal_options,
                           G_TYPE_OBJECT)

#define GARROW_EQUAL_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowEqualOptionsPrivate *>(      \
    garrow_equal_options_get_instance_private(   \
      GARROW_EQUAL_OPTIONS(object)))

static void
garrow_equal_options_finalize(GObject *object)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(object);
  priv->options.~EqualOptions();
  G_OBJECT_CLASS(garrow_equal_options_parent_class)->finalize(object);
}

static void
garrow_equal_options_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_APPROX:
    priv->approx = g_value_get_boolean(value);
    break;
  case PROP_NANS_EQUAL:
    priv->options = priv->options.nans_equal(g_value_get_boolean(value));
    break;
  case PROP_ABSOLUTE_TOLERANCE:
    priv->options = priv->options.atol(g_value_get_double(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_equal_options_get_property(GObject *object,
                                  guint prop_id,
                                  GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_APPROX:
    g_value_set_boolean(value, priv->approx);
    break;
  case PROP_NANS_EQUAL:
    g_value_set_boolean(value, priv->options.nans_equal());
    break;
  case PROP_ABSOLUTE_TOLERANCE:
    g_value_set_double(value, priv->options.atol());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_equal_options_init(GArrowEqualOptions *object)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(object);
  priv->approx = FALSE;
  new(&priv->options) arrow::EqualOptions;
  priv->options = arrow::EqualOptions::Defaults();
}

static void
garrow_equal_options_class_init(GArrowEqualOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize     = garrow_equal_options_finalize;
  gobject_class->set_property = garrow_equal_options_set_property;
  gobject_class->get_property = garrow_equal_options_get_property;

  auto options = arrow::EqualOptions::Defaults();
  GParamSpec *spec;
  /**
   * GArrowEqualOptions:approx:
   *
   * Whether or not approximate comparison is used.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_boolean("approx",
                              "Approx",
                              "Whether or not approximate comparison is used",
                              FALSE,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_APPROX, spec);

  /**
   * GArrowEqualOptions:nans-equal:
   *
   * Whether or not NaNs are considered equal.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_boolean("nans-equal",
                              "NaNs equal",
                              "Whether or not NaNs are considered equal",
                              options.nans_equal(),
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_NANS_EQUAL, spec);

  /**
   * GArrowEqualOptions:absolute-tolerance:
   *
   * The absolute tolerance for approximate comparison of
   * floating-point values.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_double("absolute-tolerance",
                             "Absolute tolerance",
                             "The absolute tolerance for approximate comparison "
                             "of floating-point values",
                             -G_MAXDOUBLE,
                             G_MAXDOUBLE,
                             options.atol(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ABSOLUTE_TOLERANCE, spec);
}

/**
 * garrow_equal_options_new:
 *
 * Returns: A newly created #GArrowEqualOptions.
 *
 * Since: 5.0.0
 */
GArrowEqualOptions *
garrow_equal_options_new(void)
{
  auto equal_options = g_object_new(GARROW_TYPE_EQUAL_OPTIONS, NULL);
  return GARROW_EQUAL_OPTIONS(equal_options);
}

/**
 * garrow_equal_options_is_approx:
 * @options: A #GArrowEqualOptions.
 *
 * Returns: %TRUE if approximate comparison is used, %FALSE otherwise.
 *
 * Since: 5.0.0
 */
gboolean
garrow_equal_options_is_approx(GArrowEqualOptions *options)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(options);
  return priv->approx;
}


typedef struct GArrowArrayPrivate_ {
  std::shared_ptr<arrow::Array> array;
  GArrowDataType *value_data_type;
  GArrowBuffer *null_bitmap;
  // Data for primitive array, value offsets for list array, type
  // codes for union array and so on.
  GArrowBuffer *buffer1;
  // Data for binary array, value offsets for dense union array and so
  // on.
  GArrowBuffer *buffer2;
  GArrowArray *parent;
} GArrowArrayPrivate;

enum {
  PROP_ARRAY = 1,
  PROP_VALUE_DATA_TYPE,
  PROP_NULL_BITMAP,
  PROP_BUFFER1,
  PROP_BUFFER2,
  PROP_PARENT,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowArray,
                                    garrow_array,
                                    G_TYPE_OBJECT)

#define GARROW_ARRAY_GET_PRIVATE(obj)         \
  static_cast<GArrowArrayPrivate *>(          \
    garrow_array_get_instance_private(        \
      GARROW_ARRAY(obj)))

G_END_DECLS
template <typename T>
const typename T::c_type *
garrow_array_get_values_raw(std::shared_ptr<arrow::Array> arrow_array,
                            gint64 *length)
{
  auto arrow_specific_array =
    std::static_pointer_cast<typename arrow::TypeTraits<T>::ArrayType>(arrow_array);
  *length = arrow_specific_array->length();
  return arrow_specific_array->raw_values();
};
G_BEGIN_DECLS

static void
garrow_array_dispose(GObject *object)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  if (priv->value_data_type) {
    g_object_unref(priv->value_data_type);
    priv->value_data_type = NULL;
  }

  if (priv->null_bitmap) {
    g_object_unref(priv->null_bitmap);
    priv->null_bitmap = NULL;
  }

  if (priv->buffer1) {
    g_object_unref(priv->buffer1);
    priv->buffer1 = NULL;
  }

  if (priv->buffer2) {
    g_object_unref(priv->buffer2);
    priv->buffer2 = NULL;
  }

  if (priv->parent) {
    g_object_unref(priv->parent);
    priv->parent = NULL;
  }

  G_OBJECT_CLASS(garrow_array_parent_class)->dispose(object);
}

static void
garrow_array_finalize(GObject *object)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  priv->array.~shared_ptr();

  G_OBJECT_CLASS(garrow_array_parent_class)->finalize(object);
}

static void
garrow_array_set_property(GObject *object,
                          guint prop_id,
                          const GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_ARRAY:
    priv->array =
      *static_cast<std::shared_ptr<arrow::Array> *>(g_value_get_pointer(value));
    break;
  case PROP_VALUE_DATA_TYPE:
    priv->value_data_type = GARROW_DATA_TYPE(g_value_dup_object(value));
    break;
  case PROP_NULL_BITMAP:
    priv->null_bitmap = GARROW_BUFFER(g_value_dup_object(value));
    break;
  case PROP_BUFFER1:
    priv->buffer1 = GARROW_BUFFER(g_value_dup_object(value));
    break;
  case PROP_BUFFER2:
    priv->buffer2 = GARROW_BUFFER(g_value_dup_object(value));
    break;
  case PROP_PARENT:
    priv->parent = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_get_property(GObject *object,
                          guint prop_id,
                          GValue *value,
                          GParamSpec *pspec)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE_DATA_TYPE:
    g_value_set_object(value, priv->value_data_type);
    break;
  case PROP_NULL_BITMAP:
    g_value_set_object(value, priv->null_bitmap);
    break;
  case PROP_BUFFER1:
    g_value_set_object(value, priv->buffer1);
    break;
  case PROP_BUFFER2:
    g_value_set_object(value, priv->buffer2);
    break;
  case PROP_PARENT:
    g_value_set_object(value, priv->parent);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_init(GArrowArray *object)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);
  new(&priv->array) std::shared_ptr<arrow::Array>;
}

static void
garrow_array_class_init(GArrowArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_array_dispose;
  gobject_class->finalize     = garrow_array_finalize;
  gobject_class->set_property = garrow_array_set_property;
  gobject_class->get_property = garrow_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("array",
                              "Array",
                              "The raw std::shared<arrow::Array> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARRAY, spec);

  spec = g_param_spec_object("value-data-type",
                             "Value data type",
                             "The data type of each value",
                             GARROW_TYPE_DATA_TYPE,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE_DATA_TYPE, spec);

  spec = g_param_spec_object("null-bitmap",
                             "NULL bitmap",
                             "The NULL bitmap",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_NULL_BITMAP, spec);

  spec = g_param_spec_object("buffer1",
                             "Buffer1",
                             "The first buffer",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BUFFER1, spec);

  spec = g_param_spec_object("buffer2",
                             "Buffer2",
                             "The second buffer",
                             GARROW_TYPE_BUFFER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_BUFFER2, spec);

  spec = g_param_spec_object("parent",
                             "Parent",
                             "The parent array",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_PARENT, spec);
}

/**
 * garrow_array_import:
 * @c_abi_array: (not nullable): A `struct ArrowArray *`.
 * @data_type: A #GArrowDataType of the C ABI array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An imported #GArrowArray
 *   on success, %NULL on error.
 *
 *   You don't need to release the passed `struct ArrowArray *`,
 *   even if this function reports an error.
 *
 * Since: 6.0.0
 */
GArrowArray *
garrow_array_import(gpointer c_abi_array,
                    GArrowDataType *data_type,
                    GError **error)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_array_result =
    arrow::ImportArray(static_cast<ArrowArray *>(c_abi_array),
                       arrow_data_type);
  if (garrow::check(error, arrow_array_result, "[array][import]")) {
    return garrow_array_new_raw(&(*arrow_array_result));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_export:
 * @array: A #GArrowArray.
 * @c_abi_array: (out): Return location for a `struct ArrowArray *`.
 *   It should be freed with the `ArrowArray::release` callback then
 *   g_free() when no longer needed.
 * @c_abi_schema: (out) (nullable): Return location for a
 *   `struct ArrowSchema *` or %NULL.
 *   It should be freed with the `ArrowSchema::release` callback then
 *   g_free() when no longer needed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 6.0.0
 */
gboolean
garrow_array_export(GArrowArray *array,
                    gpointer *c_abi_array,
                    gpointer *c_abi_schema,
                    GError **error)
{
  const auto arrow_array = garrow_array_get_raw(array);
  *c_abi_array = g_new(ArrowArray, 1);
  arrow::Status status;
  if (c_abi_schema) {
    *c_abi_schema = g_new(ArrowSchema, 1);
    status = arrow::ExportArray(*arrow_array,
                                static_cast<ArrowArray *>(*c_abi_array),
                                static_cast<ArrowSchema *>(*c_abi_schema));
  } else {
    status = arrow::ExportArray(*arrow_array,
                                static_cast<ArrowArray *>(*c_abi_array));
  }
  if (garrow::check(error, status, "[array][export]")) {
    return true;
  } else {
    g_free(*c_abi_array);
    *c_abi_array = nullptr;
    if (c_abi_schema) {
      g_free(*c_abi_schema);
      *c_abi_schema = nullptr;
    }
    return false;
  }
}

/**
 * garrow_array_equal:
 * @array: A #GArrowArray.
 * @other_array: A #GArrowArray to be compared.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_array_equal(GArrowArray *array, GArrowArray *other_array)
{
  return garrow_array_equal_options(array, other_array, NULL);
}

/**
 * garrow_array_equal_options:
 * @array: A #GArrowArray.
 * @other_array: A #GArrowArray to be compared.
 * @options: (nullable): A #GArrowEqualOptions to custom how to compare.
 *
 * Returns: %TRUE if both of them have the same data, %FALSE
 *   otherwise.
 *
 * Since: 5.0.0
 */
gboolean
garrow_array_equal_options(GArrowArray *array,
                           GArrowArray *other_array,
                           GArrowEqualOptions *options)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  if (options) {
    auto is_approx = garrow_equal_options_is_approx(options);
    const auto arrow_options = garrow_equal_options_get_raw(options);
    if (is_approx) {
      return arrow_array->ApproxEquals(arrow_other_array, *arrow_options);
    } else {
      return arrow_array->Equals(arrow_other_array, *arrow_options);
    }
  } else {
    return arrow_array->Equals(arrow_other_array);
  }
}

/**
 * garrow_array_equal_approx:
 * @array: A #GArrowArray.
 * @other_array: A #GArrowArray to be compared.
 *
 * Returns: %TRUE if both of them have the approx same data, %FALSE
 *   otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_array_equal_approx(GArrowArray *array, GArrowArray *other_array)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  return arrow_array->ApproxEquals(arrow_other_array);
}

/**
 * garrow_array_equal_range:
 * @array: A #GArrowArray.
 * @start_index: The start index of @array to be used.
 * @other_array: A #GArrowArray to be compared.
 * @other_start_index: The start index of @other_array to be used.
 * @end_index: The end index of @array to be used. The end index of
 *   @other_array is "@other_start_index + (@end_index -
 *   @start_index)".
 * @options: (nullable): A #GArrowEqualOptions to custom how to compare.
 *
 * Returns: %TRUE if both of them have the same data in the range,
 *   %FALSE otherwise.
 *
 * Since: 0.4.0
 */
gboolean
garrow_array_equal_range(GArrowArray *array,
                         gint64 start_index,
                         GArrowArray *other_array,
                         gint64 other_start_index,
                         gint64 end_index,
                         GArrowEqualOptions *options)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  if (options) {
    const auto arrow_options = garrow_equal_options_get_raw(options);
    return arrow_array->RangeEquals(arrow_other_array,
                                    start_index,
                                    end_index,
                                    other_start_index,
                                    *arrow_options);
  } else {
    return arrow_array->RangeEquals(arrow_other_array,
                                    start_index,
                                    end_index,
                                    other_start_index);
  }
}

/**
 * garrow_array_is_null:
 * @array: A #GArrowArray.
 * @i: The index of the target value.
 *
 * Returns: Whether the @i-th value is null or not.
 *
 * Since: 0.3.0
 */
gboolean
garrow_array_is_null(GArrowArray *array, gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->IsNull(i);
}

/**
 * garrow_array_is_valid:
 * @array: A #GArrowArray.
 * @i: The index of the target value.
 *
 * Returns: Whether the @i-th value is valid (not null) or not.
 *
 * Since: 0.8.0
 */
gboolean
garrow_array_is_valid(GArrowArray *array, gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->IsValid(i);
}

/**
 * garrow_array_get_length:
 * @array: A #GArrowArray.
 *
 * Returns: The number of rows in the array.
 */
gint64
garrow_array_get_length(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->length();
}

/**
 * garrow_array_get_offset:
 * @array: A #GArrowArray.
 *
 * Returns: The number of values in the array.
 */
gint64
garrow_array_get_offset(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->offset();
}

/**
 * garrow_array_get_n_nulls:
 * @array: A #GArrowArray.
 *
 * Returns: The number of NULLs in the array.
 */
gint64
garrow_array_get_n_nulls(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return arrow_array->null_count();
}

/**
 * garrow_array_get_null_bitmap:
 * @array: A #GArrowArray.
 *
 * Returns: (transfer full) (nullable): The bitmap that indicates null
 *   value indices for the array as #GArrowBuffer or %NULL when
 *   garrow_array_get_n_nulls() returns 0.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_array_get_null_bitmap(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  if (priv->null_bitmap) {
    g_object_ref(priv->null_bitmap);
    return priv->null_bitmap;
  }

  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_null_bitmap = arrow_array->null_bitmap();
  return garrow_buffer_new_raw(&arrow_null_bitmap);
}

/**
 * garrow_array_get_value_data_type:
 * @array: A #GArrowArray.
 *
 * Returns: (transfer full): The #GArrowDataType for each value of the
 *   array.
 *
 * Since: 0.3.0
 */
GArrowDataType *
garrow_array_get_value_data_type(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  if (priv->value_data_type) {
    g_object_ref(priv->value_data_type);
    return priv->value_data_type;
  }

  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_data_type = arrow_array->type();
  return garrow_data_type_new_raw(&arrow_data_type);
}

/**
 * garrow_array_get_value_type:
 * @array: A #GArrowArray.
 *
 * Returns: The #GArrowType for each value of the array.
 *
 * Since: 0.3.0
 */
GArrowType
garrow_array_get_value_type(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  return garrow_type_from_raw(arrow_array->type_id());
}

/**
 * garrow_array_slice:
 * @array: A #GArrowArray.
 * @offset: The offset of sub #GArrowArray.
 * @length: The length of sub #GArrowArray.
 *
 * Returns: (transfer full): The sub #GArrowArray. It covers only from
 *   `offset` to `offset + length` range. The sub #GArrowArray shares
 *   values with the base #GArrowArray.
 */
GArrowArray *
garrow_array_slice(GArrowArray *array,
                   gint64 offset,
                   gint64 length)
{
  const auto arrow_array = garrow_array_get_raw(array);
  auto arrow_sub_array = arrow_array->Slice(offset, length);
  return garrow_array_new_raw(&arrow_sub_array,
                              "array", &arrow_sub_array,
                              "parent", array,
                              NULL);
}

/**
 * garrow_array_to_string:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable):
 *   The formatted array content or %NULL on error.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_array_to_string(GArrowArray *array, GError **error)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto string = arrow_array->ToString();
  return g_strdup(string.c_str());
}

/**
 * garrow_array_view:
 * @array: A #GArrowArray.
 * @return_type: A #GArrowDataType of the returned view.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A zero-copy view of this array
 *   with the given type. This method checks if the `return_type` are
 *   layout-compatible.
 *
 * Since: 0.15.0
 */
GArrowArray *
garrow_array_view(GArrowArray *array,
                  GArrowDataType *return_type,
                  GError **error)
{
  auto arrow_array_raw = garrow_array_get_raw(array);
  auto arrow_return_type = garrow_data_type_get_raw(return_type);
  auto arrow_array = arrow_array_raw->View(arrow_return_type);
  if (garrow::check(error, arrow_array, "[array][view]")) {
    return garrow_array_new_raw(&(*arrow_array));
  } else {
    return NULL;
  }
}

/**
 * garrow_array_diff_unified:
 * @array: A #GArrowArray.
 * @other_array: A #GArrowArray to be compared.
 *
 * Returns: (nullable) (transfer full): The string representation of
 *   the difference between two arrays as unified format. If there is
 *   no difference, the return value is %NULL.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.15.0
 */
gchar *
garrow_array_diff_unified(GArrowArray *array, GArrowArray *other_array)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  std::stringstream diff;
  arrow_array->Equals(arrow_other_array,
                      arrow::EqualOptions().diff_sink(&diff));
  auto string = diff.str();
  if (string.empty()) {
    return NULL;
  } else {
    return g_strndup(string.data(), string.size());
  }
}

/**
 * garrow_array_concatenate:
 * @array: A #GArrowArray.
 * @other_arrays: (element-type GArrowArray): A #GArrowArray to be
 *   concatenated.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The concatenated array.
 *
 * Since: 4.0.0
 */
GArrowArray *
garrow_array_concatenate(GArrowArray *array,
                         GList *other_arrays,
                         GError **error)
{
  if (!other_arrays) {
    g_object_ref(array);
    return array;
  }
  arrow::ArrayVector arrow_arrays;
  arrow_arrays.push_back(garrow_array_get_raw(array));
  for (auto node = other_arrays; node; node = node->next) {
    auto other_array = GARROW_ARRAY(node->data);
    arrow_arrays.push_back(garrow_array_get_raw(other_array));
  }
  auto arrow_concatenated_array = arrow::Concatenate(arrow_arrays);
  if (garrow::check(error,
                    arrow_concatenated_array,
                    "[array][concatenate]")) {
    return garrow_array_new_raw(&(*arrow_concatenated_array));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowNullArray,
              garrow_null_array,
              GARROW_TYPE_ARRAY)

static void
garrow_null_array_init(GArrowNullArray *object)
{
}

static void
garrow_null_array_class_init(GArrowNullArrayClass *klass)
{
}

/**
 * garrow_null_array_new:
 * @length: An array length.
 *
 * Returns: A newly created #GArrowNullArray.
 */
GArrowNullArray *
garrow_null_array_new(gint64 length)
{
  auto arrow_null_array = std::make_shared<arrow::NullArray>(length);
  std::shared_ptr<arrow::Array> arrow_array = arrow_null_array;
  auto array = garrow_array_new_raw(&arrow_array);
  return GARROW_NULL_ARRAY(array);
}


G_DEFINE_TYPE(GArrowPrimitiveArray,
              garrow_primitive_array,
              GARROW_TYPE_ARRAY)

G_END_DECLS
template <typename T>
GArrowArray *
garrow_primitive_array_new(gint64 length,
                           GArrowBuffer *data,
                           GArrowBuffer *null_bitmap,
                           gint64 n_nulls)
{
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_specific_array =
    std::make_shared<typename arrow::TypeTraits<T>::ArrayType>(length,
                                                               arrow_data,
                                                               arrow_bitmap,
                                                               n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_specific_array);
  return garrow_array_new_raw(&arrow_array,
                              "array", &arrow_array,
                              "null-bitmap", null_bitmap,
                              "buffer1", data,
                              NULL);
};

template <typename T>
GArrowArray *
garrow_primitive_array_new(GArrowDataType *data_type,
                           gint64 length,
                           GArrowBuffer *data,
                           GArrowBuffer *null_bitmap,
                           gint64 n_nulls)
{
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_specific_array =
    std::make_shared<typename arrow::TypeTraits<T>::ArrayType>(arrow_data_type,
                                                               length,
                                                               arrow_data,
                                                               arrow_bitmap,
                                                               n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_specific_array);
  return garrow_array_new_raw(&arrow_array,
                              "array", &arrow_array,
                              "null-bitmap", null_bitmap,
                              "buffer1", data,
                              NULL);
};
G_BEGIN_DECLS

static void
garrow_primitive_array_init(GArrowPrimitiveArray *object)
{
}

static void
garrow_primitive_array_class_init(GArrowPrimitiveArrayClass *klass)
{
}

/**
 * garrow_primitive_array_get_buffer:
 * @array: A #GArrowPrimitiveArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Deprecated: 1.0.0: Use garrow_primitive_array_get_data_buffer() instead.
 */
GArrowBuffer *
garrow_primitive_array_get_buffer(GArrowPrimitiveArray *array)
{
  return garrow_primitive_array_get_data_buffer(array);
}

/**
 * garrow_primitive_array_get_data_buffer:
 * @array: A #GArrowPrimitiveArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Since: 1.0.0
 */
GArrowBuffer *
garrow_primitive_array_get_data_buffer(GArrowPrimitiveArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  if (priv->buffer1) {
    g_object_ref(priv->buffer1);
    return priv->buffer1;
  }

  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_primitive_array =
    std::static_pointer_cast<arrow::PrimitiveArray>(arrow_array);
  auto arrow_data = arrow_primitive_array->values();
  return garrow_buffer_new_raw(&arrow_data);
}


G_DEFINE_TYPE(GArrowBooleanArray,
              garrow_boolean_array,
              GARROW_TYPE_PRIMITIVE_ARRAY)

static void
garrow_boolean_array_init(GArrowBooleanArray *object)
{
}

static void
garrow_boolean_array_class_init(GArrowBooleanArrayClass *klass)
{
}

/**
 * garrow_boolean_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowBooleanArray.
 *
 * Since: 0.4.0
 */
GArrowBooleanArray *
garrow_boolean_array_new(gint64 length,
                         GArrowBuffer *data,
                         GArrowBuffer *null_bitmap,
                         gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::BooleanType>(length,
                                                              data,
                                                              null_bitmap,
                                                              n_nulls);
  return GARROW_BOOLEAN_ARRAY(array);
}

/**
 * garrow_boolean_array_get_value:
 * @array: A #GArrowBooleanArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gboolean
garrow_boolean_array_get_value(GArrowBooleanArray *array,
                               gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::BooleanArray *>(arrow_array.get())->Value(i);
}

/**
 * garrow_boolean_array_get_values:
 * @array: A #GArrowBooleanArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length) (transfer full):
 *   The raw boolean values.
 *
 *   It should be freed with g_free() when no longer needed.
 */
gboolean *
garrow_boolean_array_get_values(GArrowBooleanArray *array,
                                gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_boolean_array =
    std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
  *length = arrow_boolean_array->length();
  auto values = static_cast<gboolean *>(g_new(gboolean, *length));
  for (gint64 i = 0; i < *length; ++i) {
    values[i] = arrow_boolean_array->Value(i);
  }
  return values;
}


G_DEFINE_TYPE(GArrowNumericArray,
              garrow_numeric_array,
              GARROW_TYPE_PRIMITIVE_ARRAY)

static void
garrow_numeric_array_init(GArrowNumericArray *object)
{
}

static void
garrow_numeric_array_class_init(GArrowNumericArrayClass *klass)
{
}


G_DEFINE_TYPE(GArrowInt8Array,
              garrow_int8_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_int8_array_init(GArrowInt8Array *object)
{
}

static void
garrow_int8_array_class_init(GArrowInt8ArrayClass *klass)
{
}

/**
 * garrow_int8_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowInt8Array.
 *
 * Since: 0.4.0
 */
GArrowInt8Array *
garrow_int8_array_new(gint64 length,
                      GArrowBuffer *data,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Int8Type>(length,
                                                           data,
                                                           null_bitmap,
                                                           n_nulls);
  return GARROW_INT8_ARRAY(array);
}

/**
 * garrow_int8_array_get_value:
 * @array: A #GArrowInt8Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gint8
garrow_int8_array_get_value(GArrowInt8Array *array,
                            gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int8Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_int8_array_get_values:
 * @array: A #GArrowInt8Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gint8 *
garrow_int8_array_get_values(GArrowInt8Array *array,
                             gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::Int8Type>(arrow_array, length);
}

G_DEFINE_TYPE(GArrowUInt8Array,
              garrow_uint8_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_uint8_array_init(GArrowUInt8Array *object)
{
}

static void
garrow_uint8_array_class_init(GArrowUInt8ArrayClass *klass)
{
}

/**
 * garrow_uint8_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowUInt8Array.
 *
 * Since: 0.4.0
 */
GArrowUInt8Array *
garrow_uint8_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::UInt8Type>(length,
                                                            data,
                                                            null_bitmap,
                                                            n_nulls);
  return GARROW_UINT8_ARRAY(array);
}

/**
 * garrow_uint8_array_get_value:
 * @array: A #GArrowUInt8Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
guint8
garrow_uint8_array_get_value(GArrowUInt8Array *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::UInt8Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_uint8_array_get_values:
 * @array: A #GArrowUInt8Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const guint8 *
garrow_uint8_array_get_values(GArrowUInt8Array *array,
                              gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::UInt8Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowInt16Array,
              garrow_int16_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_int16_array_init(GArrowInt16Array *object)
{
}

static void
garrow_int16_array_class_init(GArrowInt16ArrayClass *klass)
{
}

/**
 * garrow_int16_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowInt16Array.
 *
 * Since: 0.4.0
 */
GArrowInt16Array *
garrow_int16_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Int16Type>(length,
                                                            data,
                                                            null_bitmap,
                                                            n_nulls);
  return GARROW_INT16_ARRAY(array);
}

/**
 * garrow_int16_array_get_value:
 * @array: A #GArrowInt16Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gint16
garrow_int16_array_get_value(GArrowInt16Array *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int16Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_int16_array_get_values:
 * @array: A #GArrowInt16Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gint16 *
garrow_int16_array_get_values(GArrowInt16Array *array,
                              gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::Int16Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowUInt16Array,
              garrow_uint16_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_uint16_array_init(GArrowUInt16Array *object)
{
}

static void
garrow_uint16_array_class_init(GArrowUInt16ArrayClass *klass)
{
}

/**
 * garrow_uint16_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowUInt16Array.
 *
 * Since: 0.4.0
 */
GArrowUInt16Array *
garrow_uint16_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::UInt16Type>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_UINT16_ARRAY(array);
}

/**
 * garrow_uint16_array_get_value:
 * @array: A #GArrowUInt16Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
guint16
garrow_uint16_array_get_value(GArrowUInt16Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::UInt16Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_uint16_array_get_values:
 * @array: A #GArrowUInt16Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const guint16 *
garrow_uint16_array_get_values(GArrowUInt16Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::UInt16Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowInt32Array,
              garrow_int32_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_int32_array_init(GArrowInt32Array *object)
{
}

static void
garrow_int32_array_class_init(GArrowInt32ArrayClass *klass)
{
}

/**
 * garrow_int32_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowInt32Array.
 *
 * Since: 0.4.0
 */
GArrowInt32Array *
garrow_int32_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Int32Type>(length,
                                                            data,
                                                            null_bitmap,
                                                            n_nulls);
  return GARROW_INT32_ARRAY(array);
}

/**
 * garrow_int32_array_get_value:
 * @array: A #GArrowInt32Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gint32
garrow_int32_array_get_value(GArrowInt32Array *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int32Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_int32_array_get_values:
 * @array: A #GArrowInt32Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gint32 *
garrow_int32_array_get_values(GArrowInt32Array *array,
                              gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::Int32Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowUInt32Array,
              garrow_uint32_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_uint32_array_init(GArrowUInt32Array *object)
{
}

static void
garrow_uint32_array_class_init(GArrowUInt32ArrayClass *klass)
{
}

/**
 * garrow_uint32_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowUInt32Array.
 *
 * Since: 0.4.0
 */
GArrowUInt32Array *
garrow_uint32_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::UInt32Type>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_UINT32_ARRAY(array);
}

/**
 * garrow_uint32_array_get_value:
 * @array: A #GArrowUInt32Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
guint32
garrow_uint32_array_get_value(GArrowUInt32Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::UInt32Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_uint32_array_get_values:
 * @array: A #GArrowUInt32Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const guint32 *
garrow_uint32_array_get_values(GArrowUInt32Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::UInt32Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowInt64Array,
              garrow_int64_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_int64_array_init(GArrowInt64Array *object)
{
}

static void
garrow_int64_array_class_init(GArrowInt64ArrayClass *klass)
{
}

/**
 * garrow_int64_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowInt64Array.
 *
 * Since: 0.4.0
 */
GArrowInt64Array *
garrow_int64_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Int64Type>(length,
                                                            data,
                                                            null_bitmap,
                                                            n_nulls);
  return GARROW_INT64_ARRAY(array);
}

/**
 * garrow_int64_array_get_value:
 * @array: A #GArrowInt64Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gint64
garrow_int64_array_get_value(GArrowInt64Array *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Int64Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_int64_array_get_values:
 * @array: A #GArrowInt64Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gint64 *
garrow_int64_array_get_values(GArrowInt64Array *array,
                              gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto values =
    garrow_array_get_values_raw<arrow::Int64Type>(arrow_array, length);
  return reinterpret_cast<const gint64 *>(values);
}


G_DEFINE_TYPE(GArrowUInt64Array,
              garrow_uint64_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_uint64_array_init(GArrowUInt64Array *object)
{
}

static void
garrow_uint64_array_class_init(GArrowUInt64ArrayClass *klass)
{
}

/**
 * garrow_uint64_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowUInt64Array.
 *
 * Since: 0.4.0
 */
GArrowUInt64Array *
garrow_uint64_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::UInt64Type>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_UINT64_ARRAY(array);
}

/**
 * garrow_uint64_array_get_value:
 * @array: A #GArrowUInt64Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
guint64
garrow_uint64_array_get_value(GArrowUInt64Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::UInt64Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_uint64_array_get_values:
 * @array: A #GArrowUInt64Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const guint64 *
garrow_uint64_array_get_values(GArrowUInt64Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto values =
    garrow_array_get_values_raw<arrow::UInt64Type>(arrow_array, length);
  return reinterpret_cast<const guint64 *>(values);
}


G_DEFINE_TYPE(GArrowHalfFloatArray,
              garrow_half_float_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_half_float_array_init(GArrowHalfFloatArray *object)
{
}

static void
garrow_half_float_array_class_init(GArrowHalfFloatArrayClass *klass)
{
}

/**
 * garrow_half_float_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowHalfFloatArray.
 *
 * Since: 11.0.0
 */
GArrowHalfFloatArray *
garrow_half_float_array_new(gint64 length,
                            GArrowBuffer *data,
                            GArrowBuffer *null_bitmap,
                            gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::HalfFloatType>(length,
                                                                data,
                                                                null_bitmap,
                                                                n_nulls);
  return GARROW_HALF_FLOAT_ARRAY(array);
}

/**
 * garrow_half_float_array_get_value:
 * @array: A #GArrowHalfFloatArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 11.0.0
 */
guint16
garrow_half_float_array_get_value(GArrowHalfFloatArray *array,
                                  gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return std::static_pointer_cast<arrow::HalfFloatArray>(arrow_array)->Value(i);
}

/**
 * garrow_half_float_array_get_values:
 * @array: A #GArrowHalfFloatArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 11.0.0
 */
const guint16 *
garrow_half_float_array_get_values(GArrowHalfFloatArray *array,
                                   gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::HalfFloatType>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowFloatArray,
              garrow_float_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_float_array_init(GArrowFloatArray *object)
{
}

static void
garrow_float_array_class_init(GArrowFloatArrayClass *klass)
{
}

/**
 * garrow_float_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowFloatArray.
 *
 * Since: 0.4.0
 */
GArrowFloatArray *
garrow_float_array_new(gint64 length,
                       GArrowBuffer *data,
                       GArrowBuffer *null_bitmap,
                       gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::FloatType>(length,
                                                            data,
                                                            null_bitmap,
                                                            n_nulls);
  return GARROW_FLOAT_ARRAY(array);
}

/**
 * garrow_float_array_get_value:
 * @array: A #GArrowFloatArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gfloat
garrow_float_array_get_value(GArrowFloatArray *array,
                             gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::FloatArray *>(arrow_array.get())->Value(i);
}

/**
 * garrow_float_array_get_values:
 * @array: A #GArrowFloatArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gfloat *
garrow_float_array_get_values(GArrowFloatArray *array,
                              gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::FloatType>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowDoubleArray,
              garrow_double_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_double_array_init(GArrowDoubleArray *object)
{
}

static void
garrow_double_array_class_init(GArrowDoubleArrayClass *klass)
{
}

/**
 * garrow_double_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowDoubleArray.
 *
 * Since: 0.4.0
 */
GArrowDoubleArray *
garrow_double_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::DoubleType>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_DOUBLE_ARRAY(array);
}

/**
 * garrow_double_array_get_value:
 * @array: A #GArrowDoubleArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 */
gdouble
garrow_double_array_get_value(GArrowDoubleArray *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::DoubleArray *>(arrow_array.get())->Value(i);
}

/**
 * garrow_double_array_get_values:
 * @array: A #GArrowDoubleArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 */
const gdouble *
garrow_double_array_get_values(GArrowDoubleArray *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::DoubleType>(arrow_array, length);
}


G_END_DECLS
template <typename BINARY_ARRAY_CLASS>
GArrowArray *
garrow_base_binary_array_new(gint64 length,
                             GArrowBuffer *value_offsets,
                             GArrowBuffer *value_data,
                             GArrowBuffer *null_bitmap,
                             gint64 n_nulls)
{
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_value_data = garrow_buffer_get_raw(value_data);
  const auto arrow_null_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_binary_array =
    std::make_shared<BINARY_ARRAY_CLASS>(length,
                                         arrow_value_offsets,
                                         arrow_value_data,
                                         arrow_null_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_binary_array);
  return garrow_array_new_raw(&arrow_array,
                              "array", &arrow_array,
                              "null-bitmap", null_bitmap,
                              "buffer1", value_offsets,
                              "buffer2", value_data,
                              NULL);
};

template <typename BINARY_ARRAY_CLASS>
GBytes *
garrow_base_binary_array_get_value(GArrowArray *array,
                                   gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_binary_array =
    std::static_pointer_cast<BINARY_ARRAY_CLASS>(arrow_array);
  auto view = arrow_binary_array->GetView(i);
  return g_bytes_new_static(view.data(), view.length());
};

template <typename BINARY_ARRAY_CLASS>
GArrowBuffer *
garrow_base_binary_array_get_data_buffer(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  if (priv->buffer2) {
    g_object_ref(priv->buffer2);
    return priv->buffer2;
  }

  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_binary_array =
    std::static_pointer_cast<BINARY_ARRAY_CLASS>(arrow_array);
  auto arrow_data = arrow_binary_array->value_data();
  return garrow_buffer_new_raw(&arrow_data);
};

template <typename BINARY_ARRAY_CLASS>
GArrowBuffer *
garrow_base_binary_array_get_offsets_buffer(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  if (priv->buffer1) {
    g_object_ref(priv->buffer1);
    return priv->buffer1;
  }

  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_binary_array =
    std::static_pointer_cast<BINARY_ARRAY_CLASS>(arrow_array);
  auto arrow_offsets = arrow_binary_array->value_offsets();
  return garrow_buffer_new_raw(&arrow_offsets);
};
G_BEGIN_DECLS

G_DEFINE_TYPE(GArrowBinaryArray,
              garrow_binary_array,
              GARROW_TYPE_ARRAY)

static void
garrow_binary_array_init(GArrowBinaryArray *object)
{
}

static void
garrow_binary_array_class_init(GArrowBinaryArrayClass *klass)
{
}

/**
 * garrow_binary_array_new:
 * @length: The number of elements.
 * @value_offsets: The value offsets of @data in Arrow format.
 * @value_data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowBinaryArray.
 *
 * Since: 0.4.0
 */
GArrowBinaryArray *
garrow_binary_array_new(gint64 length,
                        GArrowBuffer *value_offsets,
                        GArrowBuffer *value_data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto binary_array =
    garrow_base_binary_array_new<arrow::BinaryArray>(length,
                                                     value_offsets,
                                                     value_data,
                                                     null_bitmap,
                                                     n_nulls);
  return GARROW_BINARY_ARRAY(binary_array);
}

/**
 * garrow_binary_array_get_value:
 * @array: A #GArrowBinaryArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 */
GBytes *
garrow_binary_array_get_value(GArrowBinaryArray *array,
                              gint64 i)
{
  return garrow_base_binary_array_get_value<arrow::BinaryArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_binary_array_get_buffer:
 * @array: A #GArrowBinaryArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Deprecated: 1.0.0: Use garrow_binary_array_get_data_buffer() instead.
 */
GArrowBuffer *
garrow_binary_array_get_buffer(GArrowBinaryArray *array)
{
  return garrow_binary_array_get_data_buffer(array);
}

/**
 * garrow_binary_array_get_data_buffer:
 * @array: A #GArrowBinaryArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Since: 1.0.0
 */
GArrowBuffer *
garrow_binary_array_get_data_buffer(GArrowBinaryArray *array)
{
  return garrow_base_binary_array_get_data_buffer<arrow::BinaryArray>(
    GARROW_ARRAY(array));
}

/**
 * garrow_binary_array_get_offsets_buffer:
 * @array: A #GArrowBinaryArray.
 *
 * Returns: (transfer full): The offsets of the array as #GArrowBuffer.
 */
GArrowBuffer *
garrow_binary_array_get_offsets_buffer(GArrowBinaryArray *array)
{
  return garrow_base_binary_array_get_offsets_buffer<arrow::BinaryArray>(
    GARROW_ARRAY(array));
}


G_DEFINE_TYPE(GArrowLargeBinaryArray,
              garrow_large_binary_array,
              GARROW_TYPE_ARRAY)

static void
garrow_large_binary_array_init(GArrowLargeBinaryArray *object)
{
}

static void
garrow_large_binary_array_class_init(GArrowLargeBinaryArrayClass *klass)
{
}

/**
 * garrow_large_binary_array_new:
 * @length: The number of elements.
 * @value_offsets: The value offsets of @data in Arrow format.
 * @value_data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowLargeBinaryArray.
 *
 * Since: 0.16.0
 */
GArrowLargeBinaryArray *
garrow_large_binary_array_new(gint64 length,
                              GArrowBuffer *value_offsets,
                              GArrowBuffer *value_data,
                              GArrowBuffer *null_bitmap,
                              gint64 n_nulls)
{
  auto large_binary_array =
    garrow_base_binary_array_new<arrow::LargeBinaryArray>(length,
                                                          value_offsets,
                                                          value_data,
                                                          null_bitmap,
                                                          n_nulls);
  return GARROW_LARGE_BINARY_ARRAY(large_binary_array);
}

/**
 * garrow_large_binary_array_get_value:
 * @array: A #GArrowLargeBinaryArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 0.16.0
 */
GBytes *
garrow_large_binary_array_get_value(GArrowLargeBinaryArray *array,
                                    gint64 i)
{
  return garrow_base_binary_array_get_value<arrow::LargeBinaryArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_large_binary_array_get_buffer:
 * @array: A #GArrowLargeBinaryArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Since: 0.16.0
 *
 * Deprecated: 1.0.0: Use garrow_large_binary_array_get_data_buffer() instead.
 */
GArrowBuffer *
garrow_large_binary_array_get_buffer(GArrowLargeBinaryArray *array)
{
  return garrow_large_binary_array_get_data_buffer(array);
}

/**
 * garrow_large_binary_array_get_data_buffer:
 * @array: A #GArrowLargeBinaryArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 *
 * Since: 1.0.0
 */
GArrowBuffer *
garrow_large_binary_array_get_data_buffer(GArrowLargeBinaryArray *array)
{
  return garrow_base_binary_array_get_data_buffer<arrow::LargeBinaryArray>(
    GARROW_ARRAY(array));
}

/**
 * garrow_large_binary_array_get_offsets_buffer:
 * @array: A #GArrowLargeBinaryArray.
 *
 * Returns: (transfer full): The offsets of the array as #GArrowBuffer.
 *
 * Since: 0.16.0
 */
GArrowBuffer *
garrow_large_binary_array_get_offsets_buffer(GArrowLargeBinaryArray *array)
{
  return garrow_base_binary_array_get_offsets_buffer<arrow::LargeBinaryArray>(
    GARROW_ARRAY(array));
}


G_END_DECLS
template <typename STRING_ARRAY_CLASS>
gchar *
garrow_base_string_array_get_value(GArrowArray *array,
                                   gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_string_array =
    std::static_pointer_cast<STRING_ARRAY_CLASS>(arrow_array);
  auto view = arrow_string_array->GetView(i);
  return g_strndup(view.data(), view.length());
};
G_BEGIN_DECLS

G_DEFINE_TYPE(GArrowStringArray,
              garrow_string_array,
              GARROW_TYPE_BINARY_ARRAY)

static void
garrow_string_array_init(GArrowStringArray *object)
{
}

static void
garrow_string_array_class_init(GArrowStringArrayClass *klass)
{
}

/**
 * garrow_string_array_new:
 * @length: The number of elements.
 * @value_offsets: The value offsets of @data in Arrow format.
 * @value_data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowStringArray.
 *
 * Since: 0.4.0
 */
GArrowStringArray *
garrow_string_array_new(gint64 length,
                        GArrowBuffer *value_offsets,
                        GArrowBuffer *value_data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto string_array =
    garrow_base_binary_array_new<arrow::StringArray>(length,
                                                     value_offsets,
                                                     value_data,
                                                     null_bitmap,
                                                     n_nulls);
  return GARROW_STRING_ARRAY(string_array);
}

/**
 * garrow_string_array_get_string:
 * @array: A #GArrowStringArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th UTF-8 encoded string.
 */
gchar *
garrow_string_array_get_string(GArrowStringArray *array,
                               gint64 i)
{
  return garrow_base_string_array_get_value<arrow::StringArray>(
    GARROW_ARRAY(array), i);
}


G_DEFINE_TYPE(GArrowLargeStringArray,
              garrow_large_string_array,
              GARROW_TYPE_LARGE_BINARY_ARRAY)

static void
garrow_large_string_array_init(GArrowLargeStringArray *object)
{
}

static void
garrow_large_string_array_class_init(GArrowLargeStringArrayClass *klass)
{
}

/**
 * garrow_large_string_array_new:
 * @length: The number of elements.
 * @value_offsets: The value offsets of @data in Arrow format.
 * @value_data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowLargeStringArray.
 *
 * Since: 0.16.0
 */
GArrowLargeStringArray *
garrow_large_string_array_new(gint64 length,
                              GArrowBuffer *value_offsets,
                              GArrowBuffer *value_data,
                              GArrowBuffer *null_bitmap,
                              gint64 n_nulls)
{
  auto large_string_array =
    garrow_base_binary_array_new<arrow::LargeStringArray>(length,
                                                          value_offsets,
                                                          value_data,
                                                          null_bitmap,
                                                          n_nulls);
  return GARROW_LARGE_STRING_ARRAY(large_string_array);
}

/**
 * garrow_large_string_array_get_string:
 * @array: A #GArrowLargeStringArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th UTF-8 encoded string.
 *
 * Since: 0.16.0
 */
gchar *
garrow_large_string_array_get_string(GArrowLargeStringArray *array,
                                     gint64 i)
{
  return garrow_base_string_array_get_value<arrow::LargeStringArray>(
    GARROW_ARRAY(array), i);
}


G_DEFINE_TYPE(GArrowDate32Array,
              garrow_date32_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_date32_array_init(GArrowDate32Array *object)
{
}

static void
garrow_date32_array_class_init(GArrowDate32ArrayClass *klass)
{
}

/**
 * garrow_date32_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowDate32Array.
 *
 * Since: 0.7.0
 */
GArrowDate32Array *
garrow_date32_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Date32Type>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_DATE32_ARRAY(array);
}

/**
 * garrow_date32_array_get_value:
 * @array: A #GArrowDate32Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 0.7.0
 */
gint32
garrow_date32_array_get_value(GArrowDate32Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Date32Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_date32_array_get_values:
 * @array: A #GArrowDate32Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 0.7.0
 */
const gint32 *
garrow_date32_array_get_values(GArrowDate32Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::Date32Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowDate64Array,
              garrow_date64_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_date64_array_init(GArrowDate64Array *object)
{
}

static void
garrow_date64_array_class_init(GArrowDate64ArrayClass *klass)
{
}

/**
 * garrow_date64_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowDate64Array.
 *
 * Since: 0.7.0
 */
GArrowDate64Array *
garrow_date64_array_new(gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::Date64Type>(length,
                                                             data,
                                                             null_bitmap,
                                                             n_nulls);
  return GARROW_DATE64_ARRAY(array);
}

/**
 * garrow_date64_array_get_value:
 * @array: A #GArrowDate64Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 0.7.0
 */
gint64
garrow_date64_array_get_value(GArrowDate64Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Date64Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_date64_array_get_values:
 * @array: A #GArrowDate64Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 0.7.0
 */
const gint64 *
garrow_date64_array_get_values(GArrowDate64Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto values =
    garrow_array_get_values_raw<arrow::Date64Type>(arrow_array, length);
  return reinterpret_cast<const gint64 *>(values);
}


G_DEFINE_TYPE(GArrowTimestampArray,
              garrow_timestamp_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_timestamp_array_init(GArrowTimestampArray *object)
{
}

static void
garrow_timestamp_array_class_init(GArrowTimestampArrayClass *klass)
{
}

/**
 * garrow_timestamp_array_new:
 * @data_type: The #GArrowTimestampDataType.
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowTimestampArray.
 *
 * Since: 0.7.0
 */
GArrowTimestampArray *
garrow_timestamp_array_new(GArrowTimestampDataType *data_type,
                           gint64 length,
                           GArrowBuffer *data,
                           GArrowBuffer *null_bitmap,
                           gint64 n_nulls)
{
  auto array =
    garrow_primitive_array_new<arrow::TimestampType>(GARROW_DATA_TYPE(data_type),
                                                     length,
                                                     data,
                                                     null_bitmap,
                                                     n_nulls);
  return GARROW_TIMESTAMP_ARRAY(array);
}

/**
 * garrow_timestamp_array_get_value:
 * @array: A #GArrowTimestampArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 0.7.0
 */
gint64
garrow_timestamp_array_get_value(GArrowTimestampArray *array,
                                 gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::TimestampArray *>(arrow_array.get())->Value(i);
}

/**
 * garrow_timestamp_array_get_values:
 * @array: A #GArrowTimestampArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 0.7.0
 */
const gint64 *
garrow_timestamp_array_get_values(GArrowTimestampArray *array,
                                  gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto values =
    garrow_array_get_values_raw<arrow::TimestampType>(arrow_array, length);
  return reinterpret_cast<const gint64 *>(values);
}


G_DEFINE_TYPE(GArrowTime32Array,
              garrow_time32_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_time32_array_init(GArrowTime32Array *object)
{
}

static void
garrow_time32_array_class_init(GArrowTime32ArrayClass *klass)
{
}

/**
 * garrow_time32_array_new:
 * @data_type: The #GArrowTime32DataType.
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowTime32Array.
 *
 * Since: 0.7.0
 */
GArrowTime32Array *
garrow_time32_array_new(GArrowTime32DataType *data_type,
                        gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array =
    garrow_primitive_array_new<arrow::Time32Type>(GARROW_DATA_TYPE(data_type),
                                                  length,
                                                  data,
                                                  null_bitmap,
                                                  n_nulls);
  return GARROW_TIME32_ARRAY(array);
}

/**
 * garrow_time32_array_get_value:
 * @array: A #GArrowTime32Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 0.7.0
 */
gint32
garrow_time32_array_get_value(GArrowTime32Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Time32Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_time32_array_get_values:
 * @array: A #GArrowTime32Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 0.7.0
 */
const gint32 *
garrow_time32_array_get_values(GArrowTime32Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::Time32Type>(arrow_array, length);
}


G_DEFINE_TYPE(GArrowTime64Array,
              garrow_time64_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_time64_array_init(GArrowTime64Array *object)
{
}

static void
garrow_time64_array_class_init(GArrowTime64ArrayClass *klass)
{
}

/**
 * garrow_time64_array_new:
 * @data_type: The #GArrowTime64DataType.
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowTime64Array.
 *
 * Since: 0.7.0
 */
GArrowTime64Array *
garrow_time64_array_new(GArrowTime64DataType *data_type,
                        gint64 length,
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  auto array =
    garrow_primitive_array_new<arrow::Time64Type>(GARROW_DATA_TYPE(data_type),
                                                  length,
                                                  data,
                                                  null_bitmap,
                                                  n_nulls);
  return GARROW_TIME64_ARRAY(array);
}

/**
 * garrow_time64_array_get_value:
 * @array: A #GArrowTime64Array.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 0.7.0
 */
gint64
garrow_time64_array_get_value(GArrowTime64Array *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::Time64Array *>(arrow_array.get())->Value(i);
}

/**
 * garrow_time64_array_get_values:
 * @array: A #GArrowTime64Array.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 0.7.0
 */
const gint64 *
garrow_time64_array_get_values(GArrowTime64Array *array,
                               gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto values =
    garrow_array_get_values_raw<arrow::Time64Type>(arrow_array, length);
  return reinterpret_cast<const gint64 *>(values);
}


G_DEFINE_TYPE(GArrowMonthIntervalArray,
              garrow_month_interval_array,
              GARROW_TYPE_NUMERIC_ARRAY)

static void
garrow_month_interval_array_init(GArrowMonthIntervalArray *object)
{
}

static void
garrow_month_interval_array_class_init(GArrowMonthIntervalArrayClass *klass)
{
}

/**
 * garrow_month_interval_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowMonthIntervalArray.
 *
 * Since: 8.0.0
 */
GArrowMonthIntervalArray *
garrow_month_interval_array_new(gint64 length,
                                GArrowBuffer *data,
                                GArrowBuffer *null_bitmap,
                                gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::MonthIntervalType>(length,
                                                                    data,
                                                                    null_bitmap,
                                                                    n_nulls);
  return GARROW_MONTH_INTERVAL_ARRAY(array);
}

/**
 * garrow_month_interval_array_get_value:
 * @array: A #GArrowMonthIntervalArray.
 * @i: The index of the target value.
 *
 * Returns: The @i-th value.
 *
 * Since: 8.0.0
 */
gint32
garrow_month_interval_array_get_value(GArrowMonthIntervalArray *array,
                                      gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return static_cast<arrow::MonthIntervalArray *>(arrow_array.get())->Value(i);
}

/**
 * garrow_month_interval_array_get_values:
 * @array: A #GArrowMonthIntervalArray.
 * @length: (out): The number of values.
 *
 * Returns: (array length=length): The raw values.
 *
 * Since: 8.0.0
 */
const gint32 *
garrow_month_interval_array_get_values(GArrowMonthIntervalArray *array,
                                       gint64 *length)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  return garrow_array_get_values_raw<arrow::MonthIntervalType>(
    arrow_array, length);
}


G_DEFINE_TYPE(GArrowDayTimeIntervalArray,
              garrow_day_time_interval_array,
              GARROW_TYPE_PRIMITIVE_ARRAY)

static void
garrow_day_time_interval_array_init(GArrowDayTimeIntervalArray *object)
{
}

static void
garrow_day_time_interval_array_class_init(GArrowDayTimeIntervalArrayClass *klass)
{
}

/**
 * garrow_day_time_interval_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowDayTimeIntervalArray.
 *
 * Since: 8.0.0
 */
GArrowDayTimeIntervalArray *
garrow_day_time_interval_array_new(gint64 length,
                                   GArrowBuffer *data,
                                   GArrowBuffer *null_bitmap,
                                   gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::DayTimeIntervalType>(
    length,
    data,
    null_bitmap,
    n_nulls);
  return GARROW_DAY_TIME_INTERVAL_ARRAY(array);
}

/**
 * garrow_day_time_interval_array_get_value:
 * @array: A #GArrowDayTimeIntervalArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 8.0.0
 */
GArrowDayMillisecond *
garrow_day_time_interval_array_get_value(GArrowDayTimeIntervalArray *array,
                                         gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_day_time_interval_array =
    std::static_pointer_cast<arrow::DayTimeIntervalArray>(arrow_array);
  auto arrow_day_time_interval = arrow_day_time_interval_array->GetValue(i);
  return garrow_day_millisecond_new_raw(&arrow_day_time_interval);
}

/**
 * garrow_day_time_interval_array_get_values:
 * @array: A #GArrowDayTimeIntervalArray.
 *
 * Returns: (nullable) (element-type GArrowDayMillisecond) (transfer full):
 *   The list of #GArrowDayMillisecond.
 *
 * Since: 8.0.0
 */
GList *
garrow_day_time_interval_array_get_values(GArrowDayTimeIntervalArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_day_time_interval_array =
    std::static_pointer_cast<arrow::DayTimeIntervalArray>(arrow_array);
  auto length = arrow_day_time_interval_array->length();
  GList *values = NULL;
  for (gint64 i = 0; i < length; ++i) {
    if (arrow_day_time_interval_array->IsValid(i)) {
      auto arrow_value = arrow_day_time_interval_array->GetValue(i);
      auto value = garrow_day_millisecond_new_raw(&arrow_value);
      values = g_list_prepend(values, value);
    } else {
      values = g_list_prepend(values, NULL);
    }
  }
  return g_list_reverse(values);
}


G_DEFINE_TYPE(GArrowMonthDayNanoIntervalArray,
              garrow_month_day_nano_interval_array,
              GARROW_TYPE_PRIMITIVE_ARRAY)

static void
garrow_month_day_nano_interval_array_init(
  GArrowMonthDayNanoIntervalArray *object)
{
}

static void
garrow_month_day_nano_interval_array_class_init(
  GArrowMonthDayNanoIntervalArrayClass *klass)
{
}

/**
 * garrow_month_day_nano_interval_array_new:
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowMonthDayNanoIntervalArray.
 *
 * Since: 8.0.0
 */
GArrowMonthDayNanoIntervalArray *
garrow_month_day_nano_interval_array_new(gint64 length,
                                         GArrowBuffer *data,
                                         GArrowBuffer *null_bitmap,
                                         gint64 n_nulls)
{
  auto array = garrow_primitive_array_new<arrow::MonthDayNanoIntervalType>(
    length,
    data,
    null_bitmap,
    n_nulls);
  return GARROW_MONTH_DAY_NANO_INTERVAL_ARRAY(array);
}

/**
 * garrow_month_day_nano_interval_array_get_value:
 * @array: A #GArrowMonthDayNanoIntervalArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 8.0.0
 */
GArrowMonthDayNano *
garrow_month_day_nano_interval_array_get_value(
  GArrowMonthDayNanoIntervalArray *array,
  gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_month_day_nano_interval_array =
    std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(arrow_array);
  auto arrow_value = arrow_month_day_nano_interval_array->GetValue(i);
  return garrow_month_day_nano_new_raw(&arrow_value);
}

/**
 * garrow_month_day_nano_interval_array_get_values:
 * @array: A #GArrowMonthDayNanoIntervalArray.
 *
 * Returns: (nullable) (element-type GArrowMonthDayNano) (transfer full):
 *   The list of #GArrowMonthDayNano.
 *
 * Since: 8.0.0
 */
GList *
garrow_month_day_nano_interval_array_get_values(
  GArrowMonthDayNanoIntervalArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_month_day_nano_interval_array =
    std::static_pointer_cast<arrow::MonthDayNanoIntervalArray>(arrow_array);
  auto length = arrow_month_day_nano_interval_array->length();
  GList *values = NULL;
  for (gint64 i = 0; i < length; ++i) {
    if (arrow_month_day_nano_interval_array->IsValid(i)) {
      auto arrow_value = arrow_month_day_nano_interval_array->GetValue(i);
      auto value = garrow_month_day_nano_new_raw(&arrow_value);
      values = g_list_prepend(values, value);
    } else {
      values = g_list_prepend(values, NULL);
    }
  }
  return g_list_reverse(values);
}


G_DEFINE_TYPE(GArrowFixedSizeBinaryArray,
              garrow_fixed_size_binary_array,
              GARROW_TYPE_PRIMITIVE_ARRAY)
static void
garrow_fixed_size_binary_array_init(GArrowFixedSizeBinaryArray *object)
{
}

static void
garrow_fixed_size_binary_array_class_init(GArrowFixedSizeBinaryArrayClass *klass)
{
}

/**
 * garrow_fixed_size_binary_array_new:
 * @data_type: A #GArrowFixedSizeBinaryDataType for the array.
 * @length: The number of elements.
 * @data: The binary data in Arrow format of the array.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowFixedSizeBinaryArray.
 *
 * Since: 3.0.0
 */
GArrowFixedSizeBinaryArray *
garrow_fixed_size_binary_array_new(GArrowFixedSizeBinaryDataType *data_type,
                                   gint64 length,
                                   GArrowBuffer *data,
                                   GArrowBuffer *null_bitmap,
                                   gint64 n_nulls)
{
  auto array =
    garrow_primitive_array_new<arrow::FixedSizeBinaryType>(
      GARROW_DATA_TYPE(data_type),
      length,
      data,
      null_bitmap,
      n_nulls);
  return GARROW_FIXED_SIZE_BINARY_ARRAY(array);
}

/**
 * garrow_fixed_size_binary_array_get_byte_width:
 * @array: A #GArrowFixedSizeBinaryArray.
 *
 * Returns: The number of bytes of each value.
 *
 * Since: 3.0.0
 */
gint32
garrow_fixed_size_binary_array_get_byte_width(GArrowFixedSizeBinaryArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
  return arrow_binary_array->byte_width();
}

/**
 * garrow_fixed_size_binary_array_get_value:
 * @array: A #GArrowFixedSizeBinaryArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 3.0.0
 */
GBytes *
garrow_fixed_size_binary_array_get_value(GArrowFixedSizeBinaryArray *array,
                                         gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
  auto value = arrow_binary_array->GetValue(i);
  return g_bytes_new_static(value,
                            arrow_binary_array->byte_width());
}

/**
 * garrow_fixed_size_binary_array_get_values_bytes:
 * @array: A #GArrowFixedSizeBinaryArray.
 *
 * Returns: (transfer full): All values as a #GBytes.
 *
 * Since: 3.0.0
 */
GBytes *
garrow_fixed_size_binary_array_get_values_bytes(GArrowFixedSizeBinaryArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    std::static_pointer_cast<arrow::FixedSizeBinaryArray>(arrow_array);
  auto value = arrow_binary_array->raw_values();
  return g_bytes_new_static(value,
                            arrow_binary_array->byte_width() *
                            arrow_array->length());
}


G_DEFINE_TYPE(GArrowDecimal128Array,
              garrow_decimal128_array,
              GARROW_TYPE_FIXED_SIZE_BINARY_ARRAY)
static void
garrow_decimal128_array_init(GArrowDecimal128Array *object)
{
}

static void
garrow_decimal128_array_class_init(GArrowDecimal128ArrayClass *klass)
{
}

/**
 * garrow_decimal128_array_format_value:
 * @array: A #GArrowDecimal128Array.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The formatted @i-th value.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.10.0
 */
gchar *
garrow_decimal128_array_format_value(GArrowDecimal128Array *array,
                                     gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_decimal128_array =
    std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
  auto value = arrow_decimal128_array->FormatValue(i);
  return g_strndup(value.data(), value.size());
}

/**
 * garrow_decimal128_array_get_value:
 * @array: A #GArrowDecimal128Array.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 0.10.0
 */
GArrowDecimal128 *
garrow_decimal128_array_get_value(GArrowDecimal128Array *array,
                                  gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_decimal128_array =
    std::static_pointer_cast<arrow::Decimal128Array>(arrow_array);
  auto arrow_decimal128 =
    std::make_shared<arrow::Decimal128>(arrow_decimal128_array->GetValue(i));
  return garrow_decimal128_new_raw(&arrow_decimal128);
}


G_DEFINE_TYPE(GArrowDecimal256Array,
              garrow_decimal256_array,
              GARROW_TYPE_FIXED_SIZE_BINARY_ARRAY)
static void
garrow_decimal256_array_init(GArrowDecimal256Array *object)
{
}

static void
garrow_decimal256_array_class_init(GArrowDecimal256ArrayClass *klass)
{
}

/**
 * garrow_decimal256_array_format_value:
 * @array: A #GArrowDecimal256Array.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The formatted @i-th value.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 3.0.0
 */
gchar *
garrow_decimal256_array_format_value(GArrowDecimal256Array *array,
                                     gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_decimal256_array =
    std::static_pointer_cast<arrow::Decimal256Array>(arrow_array);
  auto value = arrow_decimal256_array->FormatValue(i);
  return g_strndup(value.data(), value.size());
}

/**
 * garrow_decimal256_array_get_value:
 * @array: A #GArrowDecimal256Array.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th value.
 *
 * Since: 3.0.0
 */
GArrowDecimal256 *
garrow_decimal256_array_get_value(GArrowDecimal256Array *array,
                                  gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_decimal256_array =
    std::static_pointer_cast<arrow::Decimal256Array>(arrow_array);
  auto arrow_decimal256 =
    std::make_shared<arrow::Decimal256>(arrow_decimal256_array->GetValue(i));
  return garrow_decimal256_new_raw(&arrow_decimal256);
}


typedef struct GArrowExtensionArrayPrivate_ {
  GArrowArray *storage;
} GArrowExtensionArrayPrivate;

enum {
  PROP_STORAGE = 1
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowExtensionArray,
                           garrow_extension_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_EXTENSION_ARRAY_GET_PRIVATE(obj)         \
  static_cast<GArrowExtensionArrayPrivate *>(           \
    garrow_extension_array_get_instance_private(        \
      GARROW_EXTENSION_ARRAY(obj)))

static void
garrow_extension_array_dispose(GObject *object)
{
  auto priv = GARROW_EXTENSION_ARRAY_GET_PRIVATE(object);

  if (priv->storage) {
    g_object_unref(priv->storage);
    priv->storage = NULL;
  }

  G_OBJECT_CLASS(garrow_extension_array_parent_class)->dispose(object);
}

static void
garrow_extension_array_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_EXTENSION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STORAGE:
    priv->storage = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extension_array_get_property(GObject *object,
                                    guint prop_id,
                                    GValue *value,
                                    GParamSpec *pspec)
{
  auto priv = GARROW_EXTENSION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STORAGE:
    g_value_set_object(value, priv->storage);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_extension_array_init(GArrowExtensionArray *object)
{
}

static void
garrow_extension_array_class_init(GArrowExtensionArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_extension_array_dispose;
  gobject_class->set_property = garrow_extension_array_set_property;
  gobject_class->get_property = garrow_extension_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("storage",
                             "storage",
                             "The storage array",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STORAGE, spec);
}

/**
 * garrow_extension_array_get_storage:
 * @array: A #GArrowExtensionArray.
 *
 * Returns: (transfer full): The underlying storage of the array.
 *
 * Since: 3.0.0
 */
GArrowArray *
garrow_extension_array_get_storage(GArrowExtensionArray *array)
{
  auto priv = GARROW_EXTENSION_ARRAY_GET_PRIVATE(array);
  if (priv->storage) {
    g_object_ref(priv->storage);
    return priv->storage;
  }

  auto array_priv = GARROW_ARRAY_GET_PRIVATE(array);
  return garrow_array_new_raw(&(array_priv->array));
}


G_END_DECLS

arrow::EqualOptions *
garrow_equal_options_get_raw(GArrowEqualOptions *equal_options)
{
  auto priv = GARROW_EQUAL_OPTIONS_GET_PRIVATE(equal_options);
  return &(priv->options);
}

GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array)
{
  return garrow_array_new_raw(arrow_array,
                              "array", arrow_array,
                              NULL);
}

GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array,
                     const gchar *first_property_name,
                     ...)
{
  va_list args;
  va_start(args, first_property_name);
  auto array = garrow_array_new_raw_valist(arrow_array,
                                           first_property_name,
                                           args);
  va_end(args);
  return array;
}

GArrowArray *
garrow_array_new_raw_valist(std::shared_ptr<arrow::Array> *arrow_array,
                            const gchar *first_property_name,
                            va_list args)
{
  GType type;

  switch ((*arrow_array)->type_id()) {
  case arrow::Type::type::NA:
    type = GARROW_TYPE_NULL_ARRAY;
    break;
  case arrow::Type::type::BOOL:
    type = GARROW_TYPE_BOOLEAN_ARRAY;
    break;
  case arrow::Type::type::UINT8:
    type = GARROW_TYPE_UINT8_ARRAY;
    break;
  case arrow::Type::type::INT8:
    type = GARROW_TYPE_INT8_ARRAY;
    break;
  case arrow::Type::type::UINT16:
    type = GARROW_TYPE_UINT16_ARRAY;
    break;
  case arrow::Type::type::INT16:
    type = GARROW_TYPE_INT16_ARRAY;
    break;
  case arrow::Type::type::UINT32:
    type = GARROW_TYPE_UINT32_ARRAY;
    break;
  case arrow::Type::type::INT32:
    type = GARROW_TYPE_INT32_ARRAY;
    break;
  case arrow::Type::type::UINT64:
    type = GARROW_TYPE_UINT64_ARRAY;
    break;
  case arrow::Type::type::INT64:
    type = GARROW_TYPE_INT64_ARRAY;
    break;
  case arrow::Type::type::HALF_FLOAT:
    type = GARROW_TYPE_HALF_FLOAT_ARRAY;
    break;
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_ARRAY;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_ARRAY;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_ARRAY;
    break;
  case arrow::Type::type::LARGE_BINARY:
    type = GARROW_TYPE_LARGE_BINARY_ARRAY;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_ARRAY;
    break;
  case arrow::Type::type::LARGE_STRING:
    type = GARROW_TYPE_LARGE_STRING_ARRAY;
    break;
  case arrow::Type::type::FIXED_SIZE_BINARY:
    type = GARROW_TYPE_FIXED_SIZE_BINARY_ARRAY;
    break;
  case arrow::Type::type::DATE32:
    type = GARROW_TYPE_DATE32_ARRAY;
    break;
  case arrow::Type::type::DATE64:
    type = GARROW_TYPE_DATE64_ARRAY;
    break;
  case arrow::Type::type::TIMESTAMP:
    type = GARROW_TYPE_TIMESTAMP_ARRAY;
    break;
  case arrow::Type::type::TIME32:
    type = GARROW_TYPE_TIME32_ARRAY;
    break;
  case arrow::Type::type::TIME64:
    type = GARROW_TYPE_TIME64_ARRAY;
    break;
  case arrow::Type::type::INTERVAL_MONTHS:
    type = GARROW_TYPE_MONTH_INTERVAL_ARRAY;
    break;
  case arrow::Type::type::INTERVAL_DAY_TIME:
    type = GARROW_TYPE_DAY_TIME_INTERVAL_ARRAY;
    break;
  case arrow::Type::type::INTERVAL_MONTH_DAY_NANO:
    type = GARROW_TYPE_MONTH_DAY_NANO_INTERVAL_ARRAY;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_ARRAY;
    break;
  case arrow::Type::type::LARGE_LIST:
    type = GARROW_TYPE_LARGE_LIST_ARRAY;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_ARRAY;
    break;
  case arrow::Type::type::MAP:
    type = GARROW_TYPE_MAP_ARRAY;
    break;
  case arrow::Type::type::SPARSE_UNION:
    type = GARROW_TYPE_SPARSE_UNION_ARRAY;
    break;
  case arrow::Type::type::DENSE_UNION:
    type = GARROW_TYPE_DENSE_UNION_ARRAY;
    break;
  case arrow::Type::type::DICTIONARY:
    type = GARROW_TYPE_DICTIONARY_ARRAY;
    break;
  case arrow::Type::type::DECIMAL128:
    type = GARROW_TYPE_DECIMAL128_ARRAY;
    break;
  case arrow::Type::type::DECIMAL256:
    type = GARROW_TYPE_DECIMAL256_ARRAY;
    break;
  case arrow::Type::type::EXTENSION:
    {
      auto arrow_data_type = (*arrow_array)->type();
      auto arrow_gextension_data_type =
        std::static_pointer_cast<garrow::GExtensionType>(arrow_data_type);
      if (arrow_gextension_data_type) {
        type = arrow_gextension_data_type->array_gtype();
      } else {
        type = GARROW_TYPE_EXTENSION_ARRAY;
      }
    }
    break;
  default:
    type = GARROW_TYPE_ARRAY;
    break;
  }
  return GARROW_ARRAY(g_object_new_valist(type,
                                          first_property_name,
                                          args));
}

GArrowExtensionArray *
garrow_extension_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array,
                               GArrowArray *storage)
{
  auto array = garrow_array_new_raw(arrow_array,
                                    "array", arrow_array,
                                    "storage", storage,
                                    NULL);
  return GARROW_EXTENSION_ARRAY(array);
}

std::shared_ptr<arrow::Array>
garrow_array_get_raw(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  return priv->array;
}
