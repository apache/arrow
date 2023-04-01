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
#include <arrow-glib/buffer.hpp>
#include <arrow-glib/compute.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: composite-array
 * @section_id: composite-array-classes
 * @title: Composite array classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowListArray is a class for list array. It can store zero or
 * more list data. If you don't have Arrow format data, you need to
 * use #GArrowListArrayBuilder to create a new array.
 *
 * #GArrowLargeListArray is a class for 64-bit offsets list array.
 * It can store zero or more list data. If you don't have Arrow format data,
 * you need to use #GArrowLargeListArrayBuilder to create a new array.
 *
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has one or more fields. If you don't
 * have Arrow format data, you need to use #GArrowStructArrayBuilder
 * to create a new array.
 *
 * #GArrowMapArray is a class for map array. It can store
 * data with keys and items.
 *
 * #GArrowUnionArray is a base class for union array. It can store
 * zero or more unions. One union has one or more fields but one union
 * can store only one field value.
 *
 * #GArrowDenseUnionArray is a class for dense union array.
 *
 * #GArrowSparseUnionArray is a class for sparse union array.
 *
 * #GArrowDictionaryArray is a class for dictionary array. It can
 * store data with dictionary and indices. It's space effective than
 * normal array when the array has many same values. You can convert a
 * normal array to dictionary array by garrow_array_dictionary_encode().
 */

typedef struct GArrowListArrayPrivate_ {
  GArrowArray *raw_values;
} GArrowListArrayPrivate;

enum {
  PROP_RAW_VALUES = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowListArray,
                           garrow_list_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_LIST_ARRAY_GET_PRIVATE(obj)      \
  static_cast<GArrowListArrayPrivate *>(        \
    garrow_list_array_get_instance_private(     \
      GARROW_LIST_ARRAY(obj)))

G_END_DECLS
template <typename LIST_ARRAY_CLASS>
GArrowArray *
garrow_base_list_array_new(GArrowDataType *data_type,
                           gint64 length,
                           GArrowBuffer *value_offsets,
                           GArrowArray *values,
                           GArrowBuffer *null_bitmap,
                           gint64 n_nulls)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_values = garrow_array_get_raw(values);
  const auto arrow_null_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_list_array =
    std::make_shared<LIST_ARRAY_CLASS>(arrow_data_type,
                                       length,
                                       arrow_value_offsets,
                                       arrow_values,
                                       arrow_null_bitmap,
                                       n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_list_array);
  return garrow_array_new_raw(&arrow_array,
                              "array", &arrow_array,
                              "value-data-type", data_type,
                              "null-bitmap", null_bitmap,
                              "buffer1", value_offsets,
                              "raw-values", values,
                              NULL);
};

template <typename LIST_ARRAY_CLASS>
GArrowDataType *
garrow_base_list_array_get_value_type(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  auto arrow_value_type = arrow_list_array->value_type();
  return garrow_data_type_new_raw(&arrow_value_type);
};

template <typename LIST_ARRAY_CLASS>
GArrowArray *
garrow_base_list_array_get_value(GArrowArray *array,
                                 gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  auto arrow_list = arrow_list_array->value_slice(i);
  return garrow_array_new_raw(&arrow_list,
                              "array", &arrow_list,
                              "parent", array,
                              NULL);
};

template <typename LIST_ARRAY_CLASS>
GArrowArray *
garrow_base_list_array_get_values(GArrowArray *array)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  auto arrow_values = arrow_list_array->values();
  return garrow_array_new_raw(&arrow_values,
                              "array", &arrow_values,
                              "parent", array,
                              NULL);
};

template <typename LIST_ARRAY_CLASS>
typename LIST_ARRAY_CLASS::offset_type
garrow_base_list_array_get_value_offset(GArrowArray *array, gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  return arrow_list_array->value_offset(i);
};

template <typename LIST_ARRAY_CLASS>
typename LIST_ARRAY_CLASS::offset_type
garrow_base_list_array_get_value_length(GArrowArray *array, gint64 i)
{
  auto arrow_array = garrow_array_get_raw(array);
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  return arrow_list_array->value_length(i);
};

template <typename LIST_ARRAY_CLASS>
const typename LIST_ARRAY_CLASS::offset_type *
garrow_base_list_array_get_value_offsets(GArrowArray *array, gint64 *n_offsets)
{
  auto arrow_array = garrow_array_get_raw(array);
  *n_offsets = arrow_array->length() + 1;
  auto arrow_list_array =
    std::static_pointer_cast<LIST_ARRAY_CLASS>(arrow_array);
  return arrow_list_array->raw_value_offsets();
};


G_BEGIN_DECLS

static void
garrow_list_array_dispose(GObject *object)
{
  auto priv = GARROW_LIST_ARRAY_GET_PRIVATE(object);

  if (priv->raw_values) {
    g_object_unref(priv->raw_values);
    priv->raw_values = NULL;
  }


  G_OBJECT_CLASS(garrow_list_array_parent_class)->dispose(object);
}

static void
garrow_list_array_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_LIST_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RAW_VALUES:
    priv->raw_values = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_array_get_property(GObject *object,
                               guint prop_id,
                               GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_LIST_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RAW_VALUES:
    g_value_set_object(value, priv->raw_values);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_list_array_init(GArrowListArray *object)
{
}

static void
garrow_list_array_class_init(GArrowListArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_list_array_dispose;
  gobject_class->set_property = garrow_list_array_set_property;
  gobject_class->get_property = garrow_list_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("raw-values",
                             "Raw values",
                             "The raw values",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RAW_VALUES, spec);
}

/**
 * garrow_list_array_new:
 * @data_type: The data type of the list.
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
garrow_list_array_new(GArrowDataType *data_type,
                      gint64 length,
                      GArrowBuffer *value_offsets,
                      GArrowArray *values,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls)
{
  auto list_array = garrow_base_list_array_new<arrow::ListArray>(
    data_type,
    length,
    value_offsets,
    values,
    null_bitmap,
    n_nulls);
  return GARROW_LIST_ARRAY(list_array);
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
  return garrow_base_list_array_get_value_type<arrow::ListArray>(
    GARROW_ARRAY(array));
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
  return garrow_base_list_array_get_value<arrow::ListArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_list_array_get_values:
 * @array: A #GArrowListArray.
 *
 * Returns: (transfer full): The array containing the list's values.
 *
 * Since: 2.0.0
 */
GArrowArray *
garrow_list_array_get_values(GArrowListArray *array)
{
  return garrow_base_list_array_get_values<arrow::ListArray>(
    GARROW_ARRAY(array));
}

/**
 * garrow_list_array_get_offset:
 * @array: A #GArrowListArray.
 * @i: The index of the offset of the target value.
 *
 * Returns: The target offset in the array containing the list's values.
 *
 * Since: 2.0.0
 */
gint32
garrow_list_array_get_value_offset(GArrowListArray *array, gint64 i)
{
  return garrow_base_list_array_get_value_offset<arrow::ListArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_list_array_get_value_length:
 * @array: A #GArrowListArray.
 * @i: The index of the length of the target value.
 *
 * Returns: The target length in the array containing the list's values.
 *
 * Since: 2.0.0
 */
gint32
garrow_list_array_get_value_length(GArrowListArray *array, gint64 i)
{
  return garrow_base_list_array_get_value_length<arrow::ListArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_list_array_get_value_offsets:
 * @array: A #GArrowListArray.
 * @n_offsets: The number of offsets to be returned.
 *
 * Returns: (array length=n_offsets): The target offsets in the array
 * containing the list's values.
 *
 * Since: 2.0.0
 */
const gint32 *
garrow_list_array_get_value_offsets(GArrowListArray *array, gint64 *n_offsets)
{
  return garrow_base_list_array_get_value_offsets<arrow::ListArray>(
    GARROW_ARRAY(array), n_offsets);
}


typedef struct GArrowLargeListArrayPrivate_ {
  GArrowArray *raw_values;
} GArrowLargeListArrayPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowLargeListArray,
                           garrow_large_list_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_LARGE_LIST_ARRAY_GET_PRIVATE(obj)        \
  static_cast<GArrowLargeListArrayPrivate *>(           \
    garrow_large_list_array_get_instance_private(       \
      GARROW_LARGE_LIST_ARRAY(obj)))

static void
garrow_large_list_array_dispose(GObject *object)
{
  auto priv = GARROW_LARGE_LIST_ARRAY_GET_PRIVATE(object);

  if (priv->raw_values) {
    g_object_unref(priv->raw_values);
    priv->raw_values = NULL;
  }

  G_OBJECT_CLASS(garrow_large_list_array_parent_class)->dispose(object);
}

static void
garrow_large_list_array_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_LARGE_LIST_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RAW_VALUES:
    priv->raw_values = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_large_list_array_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_LARGE_LIST_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RAW_VALUES:
    g_value_set_object(value, priv->raw_values);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_large_list_array_init(GArrowLargeListArray *object)
{
}

static void
garrow_large_list_array_class_init(GArrowLargeListArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_large_list_array_dispose;
  gobject_class->set_property = garrow_large_list_array_set_property;
  gobject_class->get_property = garrow_large_list_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("raw-values",
                             "Raw values",
                             "The raw values",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RAW_VALUES, spec);
}

/**
 * garrow_large_list_array_new:
 * @data_type: The data type of the list.
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
 * Returns: A newly created #GArrowLargeListArray.
 *
 * Since: 0.16.0
 */
GArrowLargeListArray *
garrow_large_list_array_new(GArrowDataType *data_type,
                            gint64 length,
                            GArrowBuffer *value_offsets,
                            GArrowArray *values,
                            GArrowBuffer *null_bitmap,
                            gint64 n_nulls)
{
  auto large_list_array = garrow_base_list_array_new<arrow::LargeListArray>(
    data_type,
    length,
    value_offsets,
    values,
    null_bitmap,
    n_nulls);
  return GARROW_LARGE_LIST_ARRAY(large_list_array);
}

/**
 * garrow_large_list_array_get_value_type:
 * @array: A #GArrowLargeListArray.
 *
 * Returns: (transfer full): The data type of value in each list.
 *
 * Since: 0.16.0
 */
GArrowDataType *
garrow_large_list_array_get_value_type(GArrowLargeListArray *array)
{
  return garrow_base_list_array_get_value_type<arrow::LargeListArray>(
    GARROW_ARRAY(array));
}

/**
 * garrow_large_list_array_get_value:
 * @array: A #GArrowLargeListArray.
 * @i: The index of the target value.
 *
 * Returns: (transfer full): The @i-th list.
 *
 * Since: 0.16.0
 */
GArrowArray *
garrow_large_list_array_get_value(GArrowLargeListArray *array,
                                  gint64 i)
{
  return garrow_base_list_array_get_value<arrow::LargeListArray>(
    GARROW_ARRAY(array),
    i);
}

/**
 * garrow_large_list_array_get_values:
 * @array: A #GArrowLargeListArray.
 *
 * Returns: (transfer full): The array containing the list's values.
 *
 * Since: 2.0.0
 */
GArrowArray *
garrow_large_list_array_get_values(GArrowLargeListArray *array)
{
  return garrow_base_list_array_get_values<arrow::LargeListArray>(
    GARROW_ARRAY(array));
}

/**
 * garrow_large_list_array_get_value_offset:
 * @array: A #GArrowLargeListArray.
 * @i: The index of the offset of the target value.
 *
 * Returns: The target offset in the array containing the list's values.
 *
 * Since: 2.0.0
 */
gint64
garrow_large_list_array_get_value_offset(GArrowLargeListArray *array, gint64 i)
{
  return garrow_base_list_array_get_value_offset<arrow::LargeListArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_large_list_array_get_length:
 * @array: A #GArrowLargeListArray.
 * @i: The index of the length of the target value.
 *
 * Returns: The target length in the array containing the list's values.
 *
 * Since: 2.0.0
 */
gint64
garrow_large_list_array_get_value_length(GArrowLargeListArray *array, gint64 i)
{
  return garrow_base_list_array_get_value_length<arrow::LargeListArray>(
    GARROW_ARRAY(array), i);
}

/**
 * garrow_large_list_array_get_value_offsets:
 * @array: A #GArrowLargeListArray.
 * @n_offsets: The number of offsets to be returned.
 *
 * Returns: (array length=n_offsets): The target offsets in the array
 * containing the list's values.
 *
 * Since: 2.0.0
 */
const gint64 *
garrow_large_list_array_get_value_offsets(GArrowLargeListArray *array,
                                          gint64 *n_offsets)
{
  return garrow_base_list_array_get_value_offsets<arrow::LargeListArray>(
    GARROW_ARRAY(array), n_offsets);
}


typedef struct GArrowStructArrayPrivate_ {
  GPtrArray *fields;
} GArrowStructArrayPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GArrowStructArray,
                           garrow_struct_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_STRUCT_ARRAY_GET_PRIVATE(obj)    \
  static_cast<GArrowStructArrayPrivate *>(      \
    garrow_struct_array_get_instance_private(   \
      GARROW_STRUCT_ARRAY(obj)))

static void
garrow_struct_array_dispose(GObject *object)
{
  auto priv = GARROW_STRUCT_ARRAY_GET_PRIVATE(object);

  if (priv->fields) {
    g_ptr_array_free(priv->fields, TRUE);
    priv->fields = NULL;
  }

  G_OBJECT_CLASS(garrow_struct_array_parent_class)->dispose(object);
}

static void
garrow_struct_array_init(GArrowStructArray *object)
{
}

static void
garrow_struct_array_class_init(GArrowStructArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->dispose = garrow_struct_array_dispose;
}

/**
 * garrow_struct_array_new:
 * @data_type: The data type of the struct.
 * @length: The number of elements.
 * @fields: (element-type GArrowArray): The arrays for each field
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
                        GList *fields,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  const auto arrow_null_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_struct_array =
    std::make_shared<arrow::StructArray>(arrow_data_type,
                                         length,
                                         arrow_fields,
                                         arrow_null_bitmap,
                                         n_nulls);
  auto arrow_array =
    std::static_pointer_cast<arrow::Array>(arrow_struct_array);
  auto struct_array =
    garrow_array_new_raw(&arrow_array,
                         "array", &arrow_array,
                         "null-bitmap", null_bitmap,
                         NULL);
  auto priv = GARROW_STRUCT_ARRAY_GET_PRIVATE(struct_array);
  priv->fields = g_ptr_array_sized_new(arrow_fields.size());
  g_ptr_array_set_free_func(priv->fields, g_object_unref);
  for (auto node = fields; node; node = node->next) {
    auto field = GARROW_ARRAY(node->data);
    g_ptr_array_add(priv->fields, g_object_ref(field));
  }
  return GARROW_STRUCT_ARRAY(struct_array);
}

static GPtrArray *
garrow_struct_array_get_fields_internal(GArrowStructArray *array)
{
  auto priv = GARROW_STRUCT_ARRAY_GET_PRIVATE(array);
  if (!priv->fields) {
    auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
    auto arrow_struct_array =
      std::static_pointer_cast<arrow::StructArray>(arrow_array);
    auto arrow_fields = arrow_struct_array->fields();
    priv->fields = g_ptr_array_sized_new(arrow_fields.size());
    g_ptr_array_set_free_func(priv->fields, g_object_unref);
    for (auto &arrow_field : arrow_fields) {
      g_ptr_array_add(priv->fields, garrow_array_new_raw(&arrow_field));
    }
  }
  return priv->fields;
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
  auto fields = garrow_struct_array_get_fields_internal(array);
  if (i < 0) {
    i += fields->len;
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= static_cast<gint>(fields->len)) {
    return NULL;
  }
  auto field = static_cast<GArrowArray *>(g_ptr_array_index(fields, i));
  g_object_ref(field);
  return field;
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
  auto fields = garrow_struct_array_get_fields_internal(array);

  GList *field_list = NULL;
  for (guint i = 0; i < fields->len; ++i) {
    auto field = static_cast<GArrowArray *>(g_ptr_array_index(fields, i));
    field_list = g_list_prepend(field_list, g_object_ref(field));
  }
  return g_list_reverse(field_list);
}

/**
 * garrow_struct_array_flatten
 * @array: A #GArrowStructArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GArrowArray) (transfer full):
 *   The fields in the struct.
 *
 * Since: 0.10.0
 */
GList *
garrow_struct_array_flatten(GArrowStructArray *array, GError **error)
{
  const auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_struct_array =
    std::static_pointer_cast<arrow::StructArray>(arrow_array);

  auto memory_pool = arrow::default_memory_pool();
  auto arrow_arrays = arrow_struct_array->Flatten(memory_pool);
  if (!garrow::check(error, arrow_arrays, "[struct-array][flatten]")) {
    return NULL;
  }

  GList *arrays = NULL;
  for (auto arrow_array : *arrow_arrays) {
    auto array = garrow_array_new_raw(&arrow_array);
    arrays = g_list_prepend(arrays, array);
  }

  return g_list_reverse(arrays);
}


typedef struct GArrowMapArrayPrivate_ {
  GArrowArray *offsets;
  GArrowArray *keys;
  GArrowArray *items;
} GArrowMapArrayPrivate;

enum {
  PROP_OFFSETS = 1,
  PROP_KEYS,
  PROP_ITEMS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowMapArray,
                           garrow_map_array,
                           GARROW_TYPE_LIST_ARRAY)

#define GARROW_MAP_ARRAY_GET_PRIVATE(obj)       \
  static_cast<GArrowMapArrayPrivate *>(         \
    garrow_map_array_get_instance_private(      \
      GARROW_MAP_ARRAY(obj)))

static void
garrow_map_array_dispose(GObject *object)
{
  auto priv = GARROW_MAP_ARRAY_GET_PRIVATE(object);

  if (priv->offsets) {
    g_object_unref(priv->offsets);
    priv->offsets = NULL;
  }

  if (priv->keys) {
    g_object_unref(priv->keys);
    priv->keys = NULL;
  }

  if (priv->items) {
    g_object_unref(priv->items);
    priv->items = NULL;
  }

  G_OBJECT_CLASS(garrow_map_array_parent_class)->dispose(object);
}

static void
garrow_map_array_set_property(GObject *object,
                              guint prop_id,
                              const GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GARROW_MAP_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OFFSETS:
    priv->offsets = GARROW_ARRAY(g_value_dup_object(value));
    break;
  case PROP_KEYS:
    priv->keys = GARROW_ARRAY(g_value_dup_object(value));
    break;
  case PROP_ITEMS:
    priv->items = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_map_array_get_property(GObject *object,
                              guint prop_id,
                              GValue *value,
                              GParamSpec *pspec)
{
  auto priv = GARROW_MAP_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_OFFSETS:
    g_value_set_object(value, priv->offsets);
    break;
  case PROP_KEYS:
    g_value_set_object(value, priv->keys);
    break;
  case PROP_ITEMS:
    g_value_set_object(value, priv->items);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_map_array_init(GArrowMapArray *object)
{
}

static void
garrow_map_array_class_init(GArrowMapArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_map_array_dispose;
  gobject_class->set_property = garrow_map_array_set_property;
  gobject_class->get_property = garrow_map_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("offsets",
                             "Offsets",
                             "The GArrowArray for offsets",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_OFFSETS, spec);

  spec = g_param_spec_object("keys",
                             "Keys",
                             "The GArrowArray for keys",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_KEYS, spec);

  spec = g_param_spec_object("items",
                             "Items",
                             "The GArrowArray for items",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_ITEMS, spec);
}

/**
 * garrow_map_array_new:
 * @offsets: The offsets Array containing n + 1 offsets encoding length and size.
 * @keys: The Array containing key values.
 * @items: The items Array containing item values.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowMapArray
 *   or %NULL on error.
 *
 * Since: 0.17.0
 */
GArrowMapArray *
garrow_map_array_new(GArrowArray *offsets,
                     GArrowArray *keys,
                     GArrowArray *items,
                     GError **error)
{
  const auto arrow_offsets = garrow_array_get_raw(offsets);
  const auto arrow_keys = garrow_array_get_raw(keys);
  const auto arrow_items = garrow_array_get_raw(items);
  auto arrow_memory_pool = arrow::default_memory_pool();
  auto arrow_array_result = arrow::MapArray::FromArrays(arrow_offsets,
                                                        arrow_keys,
                                                        arrow_items,
                                                        arrow_memory_pool);
  if (garrow::check(error, arrow_array_result, "[map-array][new]")) {
    auto arrow_array = *arrow_array_result;
    return GARROW_MAP_ARRAY(garrow_array_new_raw(&arrow_array,
                                                 "array", &arrow_array,
                                                 "offsets", offsets,
                                                 "keys", keys,
                                                 "items", items,
                                                 NULL));
  } else {
    return NULL;
  }
}

/**
 * garrow_map_array_get_keys:
 * @array: A #GArrowMapArray.
 *
 * Returns: (transfer full): The Array containing key values.
 *
 * Since: 0.17.0
 */
GArrowArray *
garrow_map_array_get_keys(GArrowMapArray *array)
{
  auto priv = GARROW_MAP_ARRAY_GET_PRIVATE(array);
  if (priv->keys) {
    g_object_ref(priv->keys);
    return priv->keys;
  }

  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_map_array =
    std::static_pointer_cast<arrow::MapArray>(arrow_array);
  auto arrow_keys = arrow_map_array->keys();
  return garrow_array_new_raw(&arrow_keys);
}

/**
 * garrow_map_array_get_items:
 * @array: A #GArrowMapArray.
 *
 * Returns: (transfer full): The items Array containing item values.
 *
 * Since: 0.17.0
 */
GArrowArray *
garrow_map_array_get_items(GArrowMapArray *array)
{
  auto priv = GARROW_MAP_ARRAY_GET_PRIVATE(array);
  if (priv->items) {
    g_object_ref(priv->items);
    return priv->items;
  }

  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_map_array =
    std::static_pointer_cast<arrow::MapArray>(arrow_array);
  auto arrow_items = arrow_map_array->items();
  return garrow_array_new_raw(&arrow_items);
}


typedef struct GArrowUnionArrayPrivate_ {
  GArrowInt8Array *type_ids;
  GPtrArray *fields;
} GArrowUnionArrayPrivate;

enum {
  PROP_TYPE_IDS = 1,
  PROP_FIELDS,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowUnionArray,
                           garrow_union_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_UNION_ARRAY_GET_PRIVATE(obj)     \
  static_cast<GArrowUnionArrayPrivate *>(       \
    garrow_union_array_get_instance_private(    \
      GARROW_UNION_ARRAY(obj)))

static void
garrow_union_array_dispose(GObject *object)
{
  auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(object);

  if (priv->type_ids) {
    g_object_unref(priv->type_ids);
    priv->type_ids = NULL;
  }

  if (priv->fields) {
    g_ptr_array_free(priv->fields, TRUE);
    priv->fields = NULL;
  }

  G_OBJECT_CLASS(garrow_union_array_parent_class)->dispose(object);
}

static void
garrow_union_array_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TYPE_IDS:
    priv->type_ids = GARROW_INT8_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_union_array_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TYPE_IDS:
    g_value_set_object(value, priv->type_ids);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_union_array_init(GArrowUnionArray *object)
{
}

static void
garrow_union_array_class_init(GArrowUnionArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_union_array_dispose;
  gobject_class->set_property = garrow_union_array_set_property;
  gobject_class->get_property = garrow_union_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("type-ids",
                             "Type IDs",
                             "The GArrowInt8Array for type IDs",
                             GARROW_TYPE_INT8_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_TYPE_IDS, spec);
}

/**
 * garrow_union_array_get_type_code:
 * @array: A #GArrowUnionArray.
 * @i: The index of the logical type code of the value in the union.
 *
 * Returns: The i-th logical type code of the value.
 *
 * Since: 12.0.0
 */
gint8
garrow_union_array_get_type_code(GArrowUnionArray *array,
                                 gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_union_array =
    std::static_pointer_cast<arrow::UnionArray>(arrow_array);
  return arrow_union_array->type_code(i);
}

/**
 * garrow_union_array_get_child_id:
 * @array: A #GArrowUnionArray.
 * @i: The index of the physical child ID containing value in the union.
 *
 * Returns: The physical child ID containing the i-th value.
 *
 * Since: 12.0.0
 */
gint
garrow_union_array_get_child_id(GArrowUnionArray *array,
                                gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_union_array =
    std::static_pointer_cast<arrow::UnionArray>(arrow_array);
  return arrow_union_array->child_id(i);
}

/**
 * garrow_union_array_get_field
 * @array: A #GArrowUnionArray.
 * @i: The index of the field in the union.
 *
 * Returns: (nullable) (transfer full): The i-th field values as a
 *   #GArrowArray or %NULL on out of range.
 */
GArrowArray *
garrow_union_array_get_field(GArrowUnionArray *array,
                             gint i)
{
  auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(array);
  if (!priv->fields) {
    auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
    auto arrow_union_array =
      std::static_pointer_cast<arrow::UnionArray>(arrow_array);
    auto n_fields = arrow_union_array->num_fields();
    priv->fields = g_ptr_array_sized_new(n_fields);
    g_ptr_array_set_free_func(priv->fields, g_object_unref);
    for (int i = 0; i < n_fields; ++i) {
      auto arrow_field = arrow_union_array->field(i);
      g_ptr_array_add(priv->fields, garrow_array_new_raw(&arrow_field));
    }
  }

  if (i < 0) {
    i += priv->fields->len;
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= static_cast<gint>(priv->fields->len)) {
    return NULL;
  }
  auto field = static_cast<GArrowArray *>(g_ptr_array_index(priv->fields, i));
  g_object_ref(field);
  return field;
}


G_DEFINE_TYPE(GArrowSparseUnionArray,
              garrow_sparse_union_array,
              GARROW_TYPE_UNION_ARRAY)

static void
garrow_sparse_union_array_init(GArrowSparseUnionArray *object)
{
}

static void
garrow_sparse_union_array_class_init(GArrowSparseUnionArrayClass *klass)
{
}

static GArrowSparseUnionArray *
garrow_sparse_union_array_new_internal(GArrowSparseUnionDataType *data_type,
                                       GArrowInt8Array *type_ids,
                                       GList *fields,
                                       GError **error,
                                       const char *context)
{
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  arrow::Result<std::shared_ptr<arrow::Array>> arrow_sparse_union_array_result;
  if (data_type) {
    auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
    auto arrow_union_data_type =
      std::static_pointer_cast<arrow::UnionType>(arrow_data_type);
    std::vector<std::string> arrow_field_names;
    for (const auto &arrow_field : arrow_union_data_type->fields()) {
      arrow_field_names.push_back(arrow_field->name());
    }
    arrow_sparse_union_array_result =
      arrow::SparseUnionArray::Make(*arrow_type_ids,
                                    arrow_fields,
                                    arrow_field_names,
                                    arrow_union_data_type->type_codes());
  } else {
    arrow_sparse_union_array_result =
      arrow::SparseUnionArray::Make(*arrow_type_ids, arrow_fields);
  }
  if (garrow::check(error,
                    arrow_sparse_union_array_result,
                    context)) {
    auto arrow_sparse_union_array = *arrow_sparse_union_array_result;
    auto sparse_union_array =
      garrow_array_new_raw(&arrow_sparse_union_array,
                           "array", &arrow_sparse_union_array,
                           "value-data-type", data_type,
                           "type-ids", type_ids,
                           NULL);
    auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(sparse_union_array);
    priv->fields = g_ptr_array_sized_new(arrow_fields.size());
    g_ptr_array_set_free_func(priv->fields, g_object_unref);
    for (auto node = fields; node; node = node->next) {
      auto field = GARROW_ARRAY(node->data);
      g_ptr_array_add(priv->fields, g_object_ref(field));
    }
    return GARROW_SPARSE_UNION_ARRAY(sparse_union_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_sparse_union_array_new:
 * @type_ids: The field type IDs for each value as #GArrowInt8Array.
 * @fields: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowSparseUnionArray
 *   or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowSparseUnionArray *
garrow_sparse_union_array_new(GArrowInt8Array *type_ids,
                              GList *fields,
                              GError **error)
{
  return garrow_sparse_union_array_new_internal(NULL,
                                                type_ids,
                                                fields,
                                                error,
                                                "[sparse-union-array][new]");
}

/**
 * garrow_sparse_union_array_new_data_type:
 * @data_type: The data type for the sparse array.
 * @type_ids: The field type IDs for each value as #GArrowInt8Array.
 * @fields: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowSparseUnionArray
 *   or %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowSparseUnionArray *
garrow_sparse_union_array_new_data_type(GArrowSparseUnionDataType *data_type,
                                        GArrowInt8Array *type_ids,
                                        GList *fields,
                                        GError **error)
{
  return garrow_sparse_union_array_new_internal(
    data_type,
    type_ids,
    fields,
    error,
    "[sparse-union-array][new][data-type]");
}


typedef struct GArrowDenseUnionArrayPrivate_ {
  GArrowInt32Array *value_offsets;
} GArrowDenseUnionArrayPrivate;

enum {
  PROP_VALUE_OFFSETS = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDenseUnionArray,
                           garrow_dense_union_array,
                           GARROW_TYPE_UNION_ARRAY)

#define GARROW_DENSE_UNION_ARRAY_GET_PRIVATE(obj)       \
  static_cast<GArrowDenseUnionArrayPrivate *>(          \
    garrow_dense_union_array_get_instance_private(      \
      GARROW_DENSE_UNION_ARRAY(obj)))

static void
garrow_dense_union_array_dispose(GObject *object)
{
  auto priv = GARROW_DENSE_UNION_ARRAY_GET_PRIVATE(object);

  if (priv->value_offsets) {
    g_object_unref(priv->value_offsets);
    priv->value_offsets = NULL;
  }

  G_OBJECT_CLASS(garrow_dense_union_array_parent_class)->dispose(object);
}

static void
garrow_dense_union_array_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_DENSE_UNION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE_OFFSETS:
    priv->value_offsets = GARROW_INT32_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dense_union_array_get_property(GObject *object,
                                      guint prop_id,
                                      GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_DENSE_UNION_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_VALUE_OFFSETS:
    g_value_set_object(value, priv->value_offsets);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dense_union_array_init(GArrowDenseUnionArray *object)
{
}

static void
garrow_dense_union_array_class_init(GArrowDenseUnionArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_dense_union_array_dispose;
  gobject_class->set_property = garrow_dense_union_array_set_property;
  gobject_class->get_property = garrow_dense_union_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("value-offsets",
                             "Value offsets",
                             "The GArrowInt32Array for value offsets",
                             GARROW_TYPE_INT32_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_VALUE_OFFSETS, spec);
}

static GArrowDenseUnionArray *
garrow_dense_union_array_new_internal(GArrowDenseUnionDataType *data_type,
                                      GArrowInt8Array *type_ids,
                                      GArrowInt32Array *value_offsets,
                                      GList *fields,
                                      GError **error,
                                      const gchar *context)
{
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  auto arrow_value_offsets = garrow_array_get_raw(GARROW_ARRAY(value_offsets));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  arrow::Result<std::shared_ptr<arrow::Array>> arrow_dense_union_array_result;
  if (data_type) {
    auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
    auto arrow_union_data_type =
      std::static_pointer_cast<arrow::UnionType>(arrow_data_type);
    std::vector<std::string> arrow_field_names;
    for (const auto &arrow_field : arrow_union_data_type->fields()) {
      arrow_field_names.push_back(arrow_field->name());
    }
    arrow_dense_union_array_result =
      arrow::DenseUnionArray::Make(*arrow_type_ids,
                                   *arrow_value_offsets,
                                   arrow_fields,
                                   arrow_field_names,
                                   arrow_union_data_type->type_codes());
  } else {
    arrow_dense_union_array_result =
      arrow::DenseUnionArray::Make(*arrow_type_ids,
                                   *arrow_value_offsets,
                                   arrow_fields);
  }
  if (garrow::check(error,
                    arrow_dense_union_array_result,
                    context)) {
    auto arrow_dense_union_array = *arrow_dense_union_array_result;
    auto dense_union_array =
      garrow_array_new_raw(&arrow_dense_union_array,
                           "array", &arrow_dense_union_array,
                           "value-data-type", data_type,
                           "type-ids", type_ids,
                           "value-offsets", value_offsets,
                           NULL);
    auto priv = GARROW_UNION_ARRAY_GET_PRIVATE(dense_union_array);
    priv->fields = g_ptr_array_sized_new(arrow_fields.size());
    g_ptr_array_set_free_func(priv->fields, g_object_unref);
    for (auto node = fields; node; node = node->next) {
      auto field = GARROW_ARRAY(node->data);
      g_ptr_array_add(priv->fields, g_object_ref(field));
    }
    return GARROW_DENSE_UNION_ARRAY(dense_union_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_dense_union_array_new:
 * @type_ids: The field type IDs for each value as #GArrowInt8Array.
 * @value_offsets: The value offsets for each value as #GArrowInt32Array.
 *   Each offset is counted for each type.
 * @fields: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowDenseUnionArray
 *   or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowDenseUnionArray *
garrow_dense_union_array_new(GArrowInt8Array *type_ids,
                             GArrowInt32Array *value_offsets,
                             GList *fields,
                             GError **error)
{
  return garrow_dense_union_array_new_internal(NULL,
                                               type_ids,
                                               value_offsets,
                                               fields,
                                               error,
                                               "[dense-union-array][new]");
}

/**
 * garrow_dense_union_array_new_data_type:
 * @data_type: The data type for the dense array.
 * @type_ids: The field type IDs for each value as #GArrowInt8Array.
 * @value_offsets: The value offsets for each value as #GArrowInt32Array.
 *   Each offset is counted for each type.
 * @fields: (element-type GArrowArray): The arrays for each field
 *   as #GList of #GArrowArray.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowSparseUnionArray
 *   or %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowDenseUnionArray *
garrow_dense_union_array_new_data_type(GArrowDenseUnionDataType *data_type,
                                       GArrowInt8Array *type_ids,
                                       GArrowInt32Array *value_offsets,
                                       GList *fields,
                                       GError **error)
{
  return garrow_dense_union_array_new_internal(
    data_type,
    type_ids,
    value_offsets,
    fields,
    error,
    "[dense-union-array][new][data-type]");
}

/**
 * garrow_dense_union_array_get_value_offset:
 * @array: A #GArrowUnionArray.
 * @i: The index of the offset of the value in the union.
 *
 * Returns: The offset of the i-th value.
 *
 * Since: 12.0.0
 */
gint32
garrow_dense_union_array_get_value_offset(GArrowDenseUnionArray *array,
                                          gint64 i)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_dense_union_array =
    std::static_pointer_cast<arrow::DenseUnionArray>(arrow_array);
  return arrow_dense_union_array->value_offset(i);
}


typedef struct GArrowDictionaryArrayPrivate_ {
  GArrowArray *indices;
  GArrowArray *dictionary;
} GArrowDictionaryArrayPrivate;

enum {
  PROP_INDICES = 1,
  PROP_DICTIONARY,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowDictionaryArray,
                           garrow_dictionary_array,
                           GARROW_TYPE_ARRAY)

#define GARROW_DICTIONARY_ARRAY_GET_PRIVATE(obj)        \
  static_cast<GArrowDictionaryArrayPrivate *>(          \
    garrow_dictionary_array_get_instance_private(       \
      GARROW_DICTIONARY_ARRAY(obj)))

static void
garrow_dictionary_array_dispose(GObject *object)
{
  auto priv = GARROW_DICTIONARY_ARRAY_GET_PRIVATE(object);

  if (priv->indices) {
    g_object_unref(priv->indices);
    priv->indices = NULL;
  }

  if (priv->dictionary) {
    g_object_unref(priv->dictionary);
    priv->dictionary = NULL;
  }

  G_OBJECT_CLASS(garrow_dictionary_array_parent_class)->dispose(object);
}

static void
garrow_dictionary_array_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_DICTIONARY_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INDICES:
    priv->indices = GARROW_ARRAY(g_value_dup_object(value));
    break;
  case PROP_DICTIONARY:
    priv->dictionary = GARROW_ARRAY(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dictionary_array_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_DICTIONARY_ARRAY_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_INDICES:
    g_value_set_object(value, priv->indices);
    break;
  case PROP_DICTIONARY:
    g_value_set_object(value, priv->dictionary);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_dictionary_array_init(GArrowDictionaryArray *object)
{
}

static void
garrow_dictionary_array_class_init(GArrowDictionaryArrayClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = garrow_dictionary_array_dispose;
  gobject_class->set_property = garrow_dictionary_array_set_property;
  gobject_class->get_property = garrow_dictionary_array_get_property;

  GParamSpec *spec;
  spec = g_param_spec_object("indices",
                             "The indices",
                             "The GArrowArray for indices",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_INDICES, spec);

  spec = g_param_spec_object("dictionary",
                             "The dictionary",
                             "The GArrowArray for dictionary",
                             GARROW_TYPE_ARRAY,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DICTIONARY, spec);
}

/**
 * garrow_dictionary_array_new:
 * @data_type: The data type of the dictionary array.
 * @indices: The indices of values in dictionary.
 * @dictionary: The dictionary of the dictionary array.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowDictionaryArray
 *   or %NULL on error.
 *
 * Since: 0.8.0
 */
GArrowDictionaryArray *
garrow_dictionary_array_new(GArrowDataType *data_type,
                            GArrowArray *indices,
                            GArrowArray *dictionary,
                            GError **error)
{
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_indices = garrow_array_get_raw(indices);
  const auto arrow_dictionary = garrow_array_get_raw(dictionary);
  auto arrow_dictionary_array_result =
    arrow::DictionaryArray::FromArrays(
      arrow_data_type,
      arrow_indices,
      arrow_dictionary);
  if (garrow::check(error,
                    arrow_dictionary_array_result,
                    "[dictionary-array][new]")) {
    auto arrow_array =
      std::static_pointer_cast<arrow::Array>(*arrow_dictionary_array_result);
    auto dictionary_array = garrow_array_new_raw(&arrow_array,
                                                 "array", &arrow_array,
                                                 "value-data-type", data_type,
                                                 "indices", indices,
                                                 "dictionary", dictionary,
                                                 NULL);
    return GARROW_DICTIONARY_ARRAY(dictionary_array);
  } else {
    return NULL;
  }
}

/**
 * garrow_dictionary_array_get_indices:
 * @array: A #GArrowDictionaryArray.
 *
 * Returns: (transfer full): The indices of values in dictionary.
 *
 * Since: 0.8.0
 */
GArrowArray *
garrow_dictionary_array_get_indices(GArrowDictionaryArray *array)
{
  auto priv = GARROW_DICTIONARY_ARRAY_GET_PRIVATE(array);
  if (priv->indices) {
    g_object_ref(priv->indices);
    return priv->indices;
  }

  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_dictionary_array =
    std::static_pointer_cast<arrow::DictionaryArray>(arrow_array);
  auto arrow_indices = arrow_dictionary_array->indices();
  return garrow_array_new_raw(&arrow_indices);
}

/**
 * garrow_dictionary_array_get_dictionary:
 * @array: A #GArrowDictionaryArray.
 *
 * Returns: (transfer full): The dictionary of this array.
 *
 * Since: 0.8.0
 */
GArrowArray *
garrow_dictionary_array_get_dictionary(GArrowDictionaryArray *array)
{
  auto priv = GARROW_DICTIONARY_ARRAY_GET_PRIVATE(array);
  if (priv->dictionary) {
    g_object_ref(priv->dictionary);
    return priv->dictionary;
  }

  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_dictionary_array =
    std::static_pointer_cast<arrow::DictionaryArray>(arrow_array);
  auto arrow_dictionary = arrow_dictionary_array->dictionary();
  return garrow_array_new_raw(&arrow_dictionary);
}

/**
 * garrow_dictionary_array_get_dictionary_data_type:
 * @array: A #GArrowDictionaryArray.
 *
 * Returns: (transfer full): The dictionary data type of this array.
 *
 * Since: 0.8.0
 *
 * Deprecated: 1.0.0: Use garrow_array_get_value_data_type() instead.
 */
GArrowDictionaryDataType *
garrow_dictionary_array_get_dictionary_data_type(GArrowDictionaryArray *array)
{
  auto data_type = garrow_array_get_value_data_type(GARROW_ARRAY(array));
  return GARROW_DICTIONARY_DATA_TYPE(data_type);
}

G_END_DECLS
