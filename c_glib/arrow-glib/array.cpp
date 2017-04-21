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
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/type.hpp>

#include <iostream>

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
 * All array classes are immutable. You need to use array builder
 * class such as #GArrowBooleanArrayBuilder to create a new array
 * except #GArrowNullArray.
 *
 * #GArrowNullArray is a class for null array. It can store zero or
 * more null values. You need to specify an array length to create a
 * new array.
 *
 * #GArrowBooleanArray is a class for binary array. It can store zero
 * or more boolean data. You need to use #GArrowBooleanArrayBuilder to
 * create a new array.
 *
 * #GArrowInt8Array is a class for 8-bit integer array. It can store
 * zero or more 8-bit integer data. You need to use
 * #GArrowInt8ArrayBuilder to create a new array.
 *
 * #GArrowUInt8Array is a class for 8-bit unsigned integer array. It
 * can store zero or more 8-bit unsigned integer data. You need to use
 * #GArrowUInt8ArrayBuilder to create a new array.
 *
 * #GArrowInt16Array is a class for 16-bit integer array. It can store
 * zero or more 16-bit integer data. You need to use
 * #GArrowInt16ArrayBuilder to create a new array.
 *
 * #GArrowUInt16Array is a class for 16-bit unsigned integer array. It
 * can store zero or more 16-bit unsigned integer data. You need to use
 * #GArrowUInt16ArrayBuilder to create a new array.
 *
 * #GArrowInt32Array is a class for 32-bit integer array. It can store
 * zero or more 32-bit integer data. You need to use
 * #GArrowInt32ArrayBuilder to create a new array.
 *
 * #GArrowUInt32Array is a class for 32-bit unsigned integer array. It
 * can store zero or more 32-bit unsigned integer data. You need to use
 * #GArrowUInt32ArrayBuilder to create a new array.
 *
 * #GArrowInt64Array is a class for 64-bit integer array. It can store
 * zero or more 64-bit integer data. You need to use
 * #GArrowInt64ArrayBuilder to create a new array.
 *
 * #GArrowUInt64Array is a class for 64-bit unsigned integer array. It
 * can store zero or more 64-bit unsigned integer data. You need to
 * use #GArrowUInt64ArrayBuilder to create a new array.
 *
 * #GArrowFloatArray is a class for 32-bit floating point array. It
 * can store zero or more 32-bit floating data. You need to use
 * #GArrowFloatArrayBuilder to create a new array.
 *
 * #GArrowDoubleArray is a class for 64-bit floating point array. It
 * can store zero or more 64-bit floating data. You need to use
 * #GArrowDoubleArrayBuilder to create a new array.
 *
 * #GArrowBinaryArray is a class for binary array. It can store zero
 * or more binary data. You need to use #GArrowBinaryArrayBuilder to
 * create a new array.
 *
 * #GArrowStringArray is a class for UTF-8 encoded string array. It
 * can store zero or more UTF-8 encoded string data. You need to use
 * #GArrowStringArrayBuilder to create a new array.
 *
 * #GArrowListArray is a class for list array. It can store zero or
 * more list data. You need to use #GArrowListArrayBuilder to create a
 * new array.
 *
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has zero or more fields. You need to
 * use #GArrowStructArrayBuilder to create a new array.
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


G_DEFINE_TYPE(GArrowBooleanArray,               \
              garrow_boolean_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_boolean_array_init(GArrowBooleanArray *object)
{
}

static void
garrow_boolean_array_class_init(GArrowBooleanArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowInt8Array,               \
              garrow_int8_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int8_array_init(GArrowInt8Array *object)
{
}

static void
garrow_int8_array_class_init(GArrowInt8ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowUInt8Array,               \
              garrow_uint8_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_uint8_array_init(GArrowUInt8Array *object)
{
}

static void
garrow_uint8_array_class_init(GArrowUInt8ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowInt16Array,               \
              garrow_int16_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int16_array_init(GArrowInt16Array *object)
{
}

static void
garrow_int16_array_class_init(GArrowInt16ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowUInt16Array,               \
              garrow_uint16_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_uint16_array_init(GArrowUInt16Array *object)
{
}

static void
garrow_uint16_array_class_init(GArrowUInt16ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowInt32Array,               \
              garrow_int32_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int32_array_init(GArrowInt32Array *object)
{
}

static void
garrow_int32_array_class_init(GArrowInt32ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowUInt32Array,               \
              garrow_uint32_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_uint32_array_init(GArrowUInt32Array *object)
{
}

static void
garrow_uint32_array_class_init(GArrowUInt32ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowInt64Array,               \
              garrow_int64_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_int64_array_init(GArrowInt64Array *object)
{
}

static void
garrow_int64_array_class_init(GArrowInt64ArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowUInt64Array,               \
              garrow_uint64_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_uint64_array_init(GArrowUInt64Array *object)
{
}

static void
garrow_uint64_array_class_init(GArrowUInt64ArrayClass *klass)
{
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

G_DEFINE_TYPE(GArrowFloatArray,               \
              garrow_float_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_float_array_init(GArrowFloatArray *object)
{
}

static void
garrow_float_array_class_init(GArrowFloatArrayClass *klass)
{
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


G_DEFINE_TYPE(GArrowDoubleArray,               \
              garrow_double_array,             \
              GARROW_TYPE_ARRAY)

static void
garrow_double_array_init(GArrowDoubleArray *object)
{
}

static void
garrow_double_array_class_init(GArrowDoubleArrayClass *klass)
{
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
  for (auto arrow_field : arrow_struct_array->fields()) {
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
