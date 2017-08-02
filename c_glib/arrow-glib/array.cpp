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
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/type.hpp>

#include <iostream>
#include <sstream>

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

/**
 * SECTION: array
 * @section_id: array-classes
 * @title: Array classes
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
 * #GArrowBooleanArray is a class for binary array. It can store zero
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
 * #GArrowStringArray is a class for UTF-8 encoded string array. It
 * can store zero or more UTF-8 encoded string data. If you don't have
 * Arrow format data, you need to use #GArrowStringArrayBuilder to
 * create a new array.
 *
 * #GArrowListArray is a class for list array. It can store zero or
 * more list data. If you don't have Arrow format data, you need to
 * use #GArrowListArrayBuilder to create a new array.
 *
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has zero or more fields. If you don't
 * have Arrow format data, you need to use #GArrowStructArrayBuilder
 * to create a new array.
 */

typedef struct GArrowArrayPrivate_ {
  std::shared_ptr<arrow::Array> array;
} GArrowArrayPrivate;

enum {
  PROP_0,
  PROP_ARRAY
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowArray, garrow_array, G_TYPE_OBJECT)

#define GARROW_ARRAY_GET_PRIVATE(obj)                                   \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj), GARROW_TYPE_ARRAY, GArrowArrayPrivate))

static void
garrow_array_finalize(GObject *object)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(object);

  priv->array = nullptr;

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
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_array_init(GArrowArray *object)
{
}

static void
garrow_array_class_init(GArrowArrayClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_array_finalize;
  gobject_class->set_property = garrow_array_set_property;
  gobject_class->get_property = garrow_array_get_property;

  spec = g_param_spec_pointer("array",
                              "Array",
                              "The raw std::shared<arrow::Array> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ARRAY, spec);
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
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  return arrow_array->Equals(arrow_other_array);
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
                         gint64 end_index)
{
  const auto arrow_array = garrow_array_get_raw(array);
  const auto arrow_other_array = garrow_array_get_raw(other_array);
  return arrow_array->RangeEquals(*arrow_other_array,
                                  start_index,
                                  end_index,
                                  other_start_index);
}

/**
 * garrow_array_is_null:
 * @array: A #GArrowArray.
 * @i: The index of the target value.
 *
 * Returns: Whether the i-th value is null or not.
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
 *   value indexes for the array as #GArrowBuffer or %NULL when
 *   garrow_array_get_n_nulls() returns 0.
 *
 * Since: 0.3.0
 */
GArrowBuffer *
garrow_array_get_null_bitmap(GArrowArray *array)
{
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
  return garrow_array_new_raw(&arrow_sub_array);
}

/**
 * garrow_array_to_string:
 * @array: A #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The formatted array content or %NULL on error.
 *
 *   The returned string should be freed when with g_free() when no
 *   longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_array_to_string(GArrowArray *array, GError **error)
{
  const auto arrow_array = garrow_array_get_raw(array);
  std::stringstream sink;
  auto status = arrow::PrettyPrint(*arrow_array, 0, &sink);
  if (garrow_error_check(error, status, "[array][to-string]")) {
    return g_strdup(sink.str().c_str());
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowNullArray,               \
              garrow_null_array,             \
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


G_DEFINE_TYPE(GArrowPrimitiveArray,             \
              garrow_primitive_array,           \
              GARROW_TYPE_ARRAY)

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
 */
GArrowBuffer *
garrow_primitive_array_get_buffer(GArrowPrimitiveArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_primitive_array =
    static_cast<arrow::PrimitiveArray *>(arrow_array.get());
  auto arrow_data = arrow_primitive_array->values();
  return garrow_buffer_new_raw(&arrow_data);
}


G_DEFINE_TYPE(GArrowBooleanArray,               \
              garrow_boolean_array,             \
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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_boolean_array =
    std::make_shared<arrow::BooleanArray>(length,
                                          arrow_data,
                                          arrow_bitmap,
                                          n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_boolean_array);
  return GARROW_BOOLEAN_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_boolean_array_get_value:
 * @array: A #GArrowBooleanArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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
 * Returns: (array length=length): The raw boolean values.
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


G_DEFINE_TYPE(GArrowInt8Array,               \
              garrow_int8_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_int8_array =
    std::make_shared<arrow::Int8Array>(length,
                                       arrow_data,
                                       arrow_bitmap,
                                       n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_int8_array);
  return GARROW_INT8_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_int8_array_get_value:
 * @array: A #GArrowInt8Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowUInt8Array,               \
              garrow_uint8_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_uint8_array =
    std::make_shared<arrow::UInt8Array>(length,
                                        arrow_data,
                                        arrow_bitmap,
                                        n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_uint8_array);
  return GARROW_UINT8_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_uint8_array_get_value:
 * @array: A #GArrowUInt8Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowInt16Array,               \
              garrow_int16_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_int16_array =
    std::make_shared<arrow::Int16Array>(length,
                                        arrow_data,
                                        arrow_bitmap,
                                        n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_int16_array);
  return GARROW_INT16_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_int16_array_get_value:
 * @array: A #GArrowInt16Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowUInt16Array,               \
              garrow_uint16_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_uint16_array =
    std::make_shared<arrow::UInt16Array>(length,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_uint16_array);
  return GARROW_UINT16_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_uint16_array_get_value:
 * @array: A #GArrowUInt16Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowInt32Array,               \
              garrow_int32_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_int32_array =
    std::make_shared<arrow::Int32Array>(length,
                                        arrow_data,
                                        arrow_bitmap,
                                        n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_int32_array);
  return GARROW_INT32_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_int32_array_get_value:
 * @array: A #GArrowInt32Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowUInt32Array,               \
              garrow_uint32_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_uint32_array =
    std::make_shared<arrow::UInt32Array>(length,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_uint32_array);
  return GARROW_UINT32_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_uint32_array_get_value:
 * @array: A #GArrowUInt32Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowInt64Array,               \
              garrow_int64_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_int64_array =
    std::make_shared<arrow::Int64Array>(length,
                                        arrow_data,
                                        arrow_bitmap,
                                        n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_int64_array);
  return GARROW_INT64_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_int64_array_get_value:
 * @array: A #GArrowInt64Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowUInt64Array,               \
              garrow_uint64_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_uint64_array =
    std::make_shared<arrow::UInt64Array>(length,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_uint64_array);
  return GARROW_UINT64_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_uint64_array_get_value:
 * @array: A #GArrowUInt64Array.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowFloatArray,               \
              garrow_float_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_float_array =
    std::make_shared<arrow::FloatArray>(length,
                                        arrow_data,
                                        arrow_bitmap,
                                        n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_float_array);
  return GARROW_FLOAT_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_float_array_get_value:
 * @array: A #GArrowFloatArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowDoubleArray,               \
              garrow_double_array,             \
              GARROW_TYPE_PRIMITIVE_ARRAY)

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
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_double_array =
    std::make_shared<arrow::DoubleArray>(length,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_double_array);
  return GARROW_DOUBLE_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_double_array_get_value:
 * @array: A #GArrowDoubleArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th value.
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


G_DEFINE_TYPE(GArrowBinaryArray,               \
              garrow_binary_array,             \
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
 * @data: The binary data in Arrow format of the array.
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
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_binary_array =
    std::make_shared<arrow::BinaryArray>(length,
                                         arrow_value_offsets,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_binary_array);
  return GARROW_BINARY_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_binary_array_get_value:
 * @array: A #GArrowBinaryArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The i-th value.
 */
GBytes *
garrow_binary_array_get_value(GArrowBinaryArray *array,
                              gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    static_cast<arrow::BinaryArray *>(arrow_array.get());

  int32_t length;
  auto value = arrow_binary_array->GetValue(i, &length);
  return g_bytes_new_static(value, length);
}

/**
 * garrow_binary_array_get_buffer:
 * @array: A #GArrowBinaryArray.
 *
 * Returns: (transfer full): The data of the array as #GArrowBuffer.
 */
GArrowBuffer *
garrow_binary_array_get_buffer(GArrowBinaryArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    static_cast<arrow::BinaryArray *>(arrow_array.get());
  auto arrow_data = arrow_binary_array->value_data();
  return garrow_buffer_new_raw(&arrow_data);
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
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_binary_array =
    static_cast<arrow::BinaryArray *>(arrow_array.get());
  auto arrow_offsets = arrow_binary_array->value_offsets();
  return garrow_buffer_new_raw(&arrow_offsets);
}


G_DEFINE_TYPE(GArrowStringArray,               \
              garrow_string_array,             \
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
 * @data: The binary data in Arrow format of the array.
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
                        GArrowBuffer *data,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_data = garrow_buffer_get_raw(data);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_string_array =
    std::make_shared<arrow::StringArray>(length,
                                         arrow_value_offsets,
                                         arrow_data,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_string_array);
  return GARROW_STRING_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_string_array_get_string:
 * @array: A #GArrowStringArray.
 * @i: The index of the target value.
 *
 * Returns: The i-th UTF-8 encoded string.
 */
gchar *
garrow_string_array_get_string(GArrowStringArray *array,
                               gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_string_array =
    static_cast<arrow::StringArray *>(arrow_array.get());
  gint32 length;
  auto value =
    reinterpret_cast<const gchar *>(arrow_string_array->GetValue(i, &length));
  return g_strndup(value, length);
}


G_DEFINE_TYPE(GArrowListArray,               \
              garrow_list_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_list_array_init(GArrowListArray *object)
{
}

static void
garrow_list_array_class_init(GArrowListArrayClass *klass)
{
}

/**
 * garrow_list_array_new:
 * @length: The number of elements.
 * @value_offsets: The offsets of @values in Arrow format.
 * @values: The values as #GArrowArray.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowListArray.
 *
 * Since: 0.4.0
 */
GArrowListArray *
garrow_list_array_new(gint64 length,
                      GArrowBuffer *value_offsets,
                      GArrowArray *values,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls)
{
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_values = garrow_array_get_raw(values);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_data_type = arrow::list(arrow_values->type());
  auto arrow_list_array =
    std::make_shared<arrow::ListArray>(arrow_data_type,
                                       length,
                                       arrow_value_offsets,
                                       arrow_values,
                                       arrow_bitmap,
                                       n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_list_array);
  return GARROW_LIST_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_list_array_get_value_type:
 * @array: A #GArrowListArray.
 *
 * Returns: (transfer full): The data type of value in each list.
 */
GArrowDataType *
garrow_list_array_get_value_type(GArrowListArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_list_array =
    static_cast<arrow::ListArray *>(arrow_array.get());
  auto arrow_value_type = arrow_list_array->value_type();
  return garrow_data_type_new_raw(&arrow_value_type);
}

/**
 * garrow_list_array_get_value:
 * @array: A #GArrowListArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The i-th list.
 */
GArrowArray *
garrow_list_array_get_value(GArrowListArray *array,
                            gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_list_array =
    static_cast<arrow::ListArray *>(arrow_array.get());
  auto arrow_list =
    arrow_list_array->values()->Slice(arrow_list_array->value_offset(i),
                                      arrow_list_array->value_length(i));
  return garrow_array_new_raw(&arrow_list);
}


G_DEFINE_TYPE(GArrowStructArray,               \
              garrow_struct_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_struct_array_init(GArrowStructArray *object)
{
}

static void
garrow_struct_array_class_init(GArrowStructArrayClass *klass)
{
}

/**
 * garrow_struct_array_new:
 * @data_type: The data type of the struct.
 * @length: The number of elements.
 * @children: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @null_bitmap: (nullable): The bitmap that shows null elements. The
 *   N-th element is null when the N-th bit is 0, not null otherwise.
 *   If the array has no null elements, the bitmap must be %NULL and
 *   @n_nulls is 0.
 * @n_nulls: The number of null elements. If -1 is specified, the
 *   number of nulls are computed from @null_bitmap.
 *
 * Returns: A newly created #GArrowStructArray.
 *
 * Since: 0.4.0
 */
GArrowStructArray *
garrow_struct_array_new(GArrowDataType *data_type,
                        gint64 length,
                        GList *children,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  std::vector<std::shared_ptr<arrow::Array>> arrow_children;
  for (GList *node = children; node; node = node->next) {
    GArrowArray *child = GARROW_ARRAY(node->data);
    arrow_children.push_back(garrow_array_get_raw(child));
  }
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_struct_array =
    std::make_shared<arrow::StructArray>(arrow_data_type,
                                         length,
                                         arrow_children,
                                         arrow_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_struct_array);
  return GARROW_STRUCT_ARRAY(garrow_array_new_raw(&arrow_array));
}

/**
 * garrow_struct_array_get_field
 * @array: A #GArrowStructArray.
 * @i: The index of the field in the struct.
 *
 * Returns: (transfer full): The i-th field.
 */
GArrowArray *
garrow_struct_array_get_field(GArrowStructArray *array,
                              gint i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_struct_array =
    static_cast<arrow::StructArray *>(arrow_array.get());
  auto arrow_field = arrow_struct_array->field(i);
  return garrow_array_new_raw(&arrow_field);
}

/**
 * garrow_struct_array_get_fields
 * @array: A #GArrowStructArray.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The fields in the struct.
 */
GList *
garrow_struct_array_get_fields(GArrowStructArray *array)
{
  const auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  const auto arrow_struct_array =
    static_cast<const arrow::StructArray *>(arrow_array.get());

  GList *fields = NULL;
  for (int i = 0; i < arrow_struct_array->num_fields(); ++i) {
    auto arrow_field = arrow_struct_array->field(i);
    GArrowArray *field = garrow_array_new_raw(&arrow_field);
    fields = g_list_prepend(fields, field);
  }

  return g_list_reverse(fields);
}

G_END_DECLS

GArrowArray *
garrow_array_new_raw(std::shared_ptr<arrow::Array> *arrow_array)
{
  GType type;
  GArrowArray *array;

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
  case arrow::Type::type::FLOAT:
    type = GARROW_TYPE_FLOAT_ARRAY;
    break;
  case arrow::Type::type::DOUBLE:
    type = GARROW_TYPE_DOUBLE_ARRAY;
    break;
  case arrow::Type::type::BINARY:
    type = GARROW_TYPE_BINARY_ARRAY;
    break;
  case arrow::Type::type::STRING:
    type = GARROW_TYPE_STRING_ARRAY;
    break;
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_ARRAY;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_ARRAY;
    break;
  default:
    type = GARROW_TYPE_ARRAY;
    break;
  }
  array = GARROW_ARRAY(g_object_new(type,
                                    "array", arrow_array,
                                    NULL));
  return array;
}

std::shared_ptr<arrow::Array>
garrow_array_get_raw(GArrowArray *array)
{
  auto priv = GARROW_ARRAY_GET_PRIVATE(array);
  return priv->array;
}
