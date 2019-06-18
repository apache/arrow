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
 * #GArrowStructArray is a class for struct array. It can store zero
 * or more structs. One struct has one or more fields. If you don't
 * have Arrow format data, you need to use #GArrowStructArrayBuilder
 * to create a new array.
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

G_DEFINE_TYPE(GArrowListArray,
              garrow_list_array,
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
  const auto arrow_data_type = garrow_data_type_get_raw(data_type);
  const auto arrow_value_offsets = garrow_buffer_get_raw(value_offsets);
  const auto arrow_values = garrow_array_get_raw(values);
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
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


G_DEFINE_TYPE(GArrowStructArray,
              garrow_struct_array,
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
    auto child = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(child));
  }
  const auto arrow_bitmap = garrow_buffer_get_raw(null_bitmap);
  auto arrow_struct_array =
    std::make_shared<arrow::StructArray>(arrow_data_type,
                                         length,
                                         arrow_fields,
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
 *
 * Deprecated: 0.10.0. Use garrow_struct_array_flatten() instead.
 */
GList *
garrow_struct_array_get_fields(GArrowStructArray *array)
{
  return garrow_struct_array_flatten(array, NULL);
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
  arrow::ArrayVector arrow_arrays;
  auto status = arrow_struct_array->Flatten(memory_pool, &arrow_arrays);
  if (!garrow_error_check(error, status, "[struct-array][flatten]")) {
    return NULL;
  }

  GList *arrays = NULL;
  for (auto arrow_array : arrow_arrays) {
    auto array = garrow_array_new_raw(&arrow_array);
    arrays = g_list_prepend(arrays, array);
  }

  return g_list_reverse(arrays);
}


G_DEFINE_TYPE(GArrowUnionArray,
              garrow_union_array,
              GARROW_TYPE_ARRAY)

static void
garrow_union_array_init(GArrowUnionArray *object)
{
}

static void
garrow_union_array_class_init(GArrowUnionArrayClass *klass)
{
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
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_union_array =
    std::static_pointer_cast<arrow::UnionArray>(arrow_array);
  auto n_fields = arrow_array->num_fields();
  if (i < 0) {
    i += n_fields;
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= n_fields) {
    return NULL;
  }
  auto arrow_field_array = arrow_union_array->child(i);
  return garrow_array_new_raw(&arrow_field_array);
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
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  std::shared_ptr<arrow::Array> arrow_union_array;
  auto status = arrow::UnionArray::MakeSparse(*arrow_type_ids,
                                              arrow_fields,
                                              &arrow_union_array);
  if (garrow_error_check(error, status, "[sparse-union-array][new]")) {
    return GARROW_SPARSE_UNION_ARRAY(garrow_array_new_raw(&arrow_union_array));
  } else {
    return NULL;
  }
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
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_union_data_type =
    std::static_pointer_cast<arrow::UnionType>(arrow_data_type);
  std::vector<std::string> arrow_field_names;
  for (const auto &arrow_field : arrow_union_data_type->children()) {
    arrow_field_names.push_back(arrow_field->name());
  }
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  std::shared_ptr<arrow::Array> arrow_union_array;
  auto status = arrow::UnionArray::MakeSparse(*arrow_type_ids,
                                              arrow_fields,
                                              arrow_field_names,
                                              arrow_union_data_type->type_codes(),
                                              &arrow_union_array);
  if (garrow_error_check(error,
                         status,
                         "[sparse-union-array][new][data-type]")) {
    return GARROW_SPARSE_UNION_ARRAY(garrow_array_new_raw(&arrow_union_array));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowDenseUnionArray,
              garrow_dense_union_array,
              GARROW_TYPE_UNION_ARRAY)

static void
garrow_dense_union_array_init(GArrowDenseUnionArray *object)
{
}

static void
garrow_dense_union_array_class_init(GArrowDenseUnionArrayClass *klass)
{
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
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  auto arrow_value_offsets = garrow_array_get_raw(GARROW_ARRAY(value_offsets));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  std::shared_ptr<arrow::Array> arrow_union_array;
  auto status = arrow::UnionArray::MakeDense(*arrow_type_ids,
                                             *arrow_value_offsets,
                                             arrow_fields,
                                             &arrow_union_array);
  if (garrow_error_check(error, status, "[dense-union-array][new]")) {
    return GARROW_DENSE_UNION_ARRAY(garrow_array_new_raw(&arrow_union_array));
  } else {
    return NULL;
  }
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
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_union_data_type =
    std::static_pointer_cast<arrow::UnionType>(arrow_data_type);
  std::vector<std::string> arrow_field_names;
  for (const auto &arrow_field : arrow_union_data_type->children()) {
    arrow_field_names.push_back(arrow_field->name());
  }
  auto arrow_type_ids = garrow_array_get_raw(GARROW_ARRAY(type_ids));
  auto arrow_value_offsets = garrow_array_get_raw(GARROW_ARRAY(value_offsets));
  std::vector<std::shared_ptr<arrow::Array>> arrow_fields;
  for (auto node = fields; node; node = node->next) {
    auto *field = GARROW_ARRAY(node->data);
    arrow_fields.push_back(garrow_array_get_raw(field));
  }
  std::shared_ptr<arrow::Array> arrow_union_array;
  auto status = arrow::UnionArray::MakeDense(*arrow_type_ids,
                                             *arrow_value_offsets,
                                             arrow_fields,
                                             arrow_field_names,
                                             arrow_union_data_type->type_codes(),
                                             &arrow_union_array);
  if (garrow_error_check(error, status, "[dense-union-array][new][data-type]")) {
    return GARROW_DENSE_UNION_ARRAY(garrow_array_new_raw(&arrow_union_array));
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowDictionaryArray,
              garrow_dictionary_array,
              GARROW_TYPE_ARRAY)

static void
garrow_dictionary_array_init(GArrowDictionaryArray *object)
{
}

static void
garrow_dictionary_array_class_init(GArrowDictionaryArrayClass *klass)
{
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
  std::shared_ptr<arrow::Array> arrow_dictionary_array;
  auto status = arrow::DictionaryArray::FromArrays(arrow_data_type,
                                                   arrow_indices,
                                                   arrow_dictionary,
                                                   &arrow_dictionary_array);
  if (garrow_error_check(error, status, "[dictionary-array][new]")) {
    auto arrow_array =
      std::static_pointer_cast<arrow::Array>(arrow_dictionary_array);
    return GARROW_DICTIONARY_ARRAY(garrow_array_new_raw(&arrow_array));
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
 */
GArrowDictionaryDataType *
garrow_dictionary_array_get_dictionary_data_type(GArrowDictionaryArray *array)
{
  auto arrow_array = garrow_array_get_raw(GARROW_ARRAY(array));
  auto arrow_dictionary_array =
    std::static_pointer_cast<arrow::DictionaryArray>(arrow_array);
  auto arrow_dictionary_data_type = arrow_dictionary_array->dict_type();
  auto const_arrow_data_type =
    static_cast<const arrow::DataType *>(arrow_dictionary_data_type);
  auto arrow_data_type = const_cast<arrow::DataType *>(const_arrow_data_type);
  struct NullDeleter {
    void operator()(arrow::DataType *data_type) {
    }
  };
  std::shared_ptr<arrow::DataType>
    shared_arrow_data_type(arrow_data_type, NullDeleter());
  auto data_type = garrow_data_type_new_raw(&shared_arrow_data_type);
  return GARROW_DICTIONARY_DATA_TYPE(data_type);
}

G_END_DECLS
