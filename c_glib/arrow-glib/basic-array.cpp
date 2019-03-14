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
#include <arrow-glib/basic-data-type.hpp>
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/compute.hpp>
#include <arrow-glib/decimal128.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/type.hpp>

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
  return garrow_array_new_raw(&arrow_array);
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
  return garrow_array_new_raw(&arrow_array);
};

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
 * #GArrowDecimal128Array is a class for 128-bit decimal array. It can store zero
 * or more 128-bit decimal data. If you don't have Arrow format data, you need
 * to use #GArrowDecimal128ArrayBuilder to create a new array.
 */

typedef struct GArrowArrayPrivate_ {
  std::shared_ptr<arrow::Array> array;
} GArrowArrayPrivate;

enum {
  PROP_0,
  PROP_ARRAY
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowArray,
                                    garrow_array,
                                    G_TYPE_OBJECT)

#define GARROW_ARRAY_GET_PRIVATE(obj)         \
  static_cast<GArrowArrayPrivate *>(          \
     garrow_array_get_instance_private(       \
       GARROW_ARRAY(obj)))

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
 * garrow_array_is_valid:
 * @array: A #GArrowArray.
 * @i: The index of the target value.
 *
 * Returns: Whether the i-th value is valid (not null) or not.
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
 * Returns: (nullable) (transfer full):
 *   The formatted array content or %NULL on error.
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
GArrowArray *
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

  return garrow_array_new_raw(&arrow_dictionary_encoded_array);
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
 * Returns: The i-th value.
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
 * Returns: The i-th value.
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
 * Returns: The i-th value.
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
 * Returns: The i-th value.
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
 * Returns: The i-th value.
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
 * Returns: (transfer full): The formatted i-th value.
 *
 *   The returned string should be freed with g_free() when no longer
 *   needed.
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
 * Returns: (transfer full): The i-th value.
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
  auto arrow_decimal =
    std::make_shared<arrow::Decimal128>(arrow_decimal128_array->GetValue(i));
  return garrow_decimal128_new_raw(&arrow_decimal);
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
  case arrow::Type::type::LIST:
    type = GARROW_TYPE_LIST_ARRAY;
    break;
  case arrow::Type::type::STRUCT:
    type = GARROW_TYPE_STRUCT_ARRAY;
    break;
  case arrow::Type::type::UNION:
    {
      auto arrow_union_array =
        std::static_pointer_cast<arrow::UnionArray>(*arrow_array);
      if (arrow_union_array->mode() == arrow::UnionMode::SPARSE) {
        type = GARROW_TYPE_SPARSE_UNION_ARRAY;
      } else {
        type = GARROW_TYPE_DENSE_UNION_ARRAY;
      }
    }
    break;
  case arrow::Type::type::DICTIONARY:
    type = GARROW_TYPE_DICTIONARY_ARRAY;
    break;
  case arrow::Type::type::DECIMAL:
    type = GARROW_TYPE_DECIMAL128_ARRAY;
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
