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
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>
#include <arrow-glib/field.hpp>
#include <arrow-glib/type.hpp>

G_BEGIN_DECLS

/**
 * SECTION: composite-data-type
 * @section_id: composite-data-type-classes
 * @title: Composite data type classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowBaseListDataType is an abstract class for list data type.
 *
 * #GArrowListDataType is a class for list data type.
 *
 * #GArrowLargeListDataType is a class for 64-bit offsets list data type.
 *
 * #GArrowStructDataType is a class for struct data type.
 *
 * #GArrowMapDataType is a class for map data type.
 *
 * #GArrowUnionDataType is a base class for union data types.
 *
 * #GArrowSparseUnionDataType is a class for sparse union data type.
 *
 * #GArrowDenseUnionDataType is a class for dense union data type.
 *
 * #GArrowDictionaryDataType is a class for dictionary data type.
 *
 * #GArrowRunEndEncodedDataType is a class for run end encoded data type.
 *
 * #GArrowFixedSizeListDataType is a class for fixed size list data type.
 */

G_DEFINE_TYPE(GArrowBaseListDataType, garrow_base_list_data_type, GARROW_TYPE_DATA_TYPE)

static void
garrow_base_list_data_type_init(GArrowBaseListDataType *object)
{
}

static void
garrow_base_list_data_type_class_init(GArrowBaseListDataTypeClass *klass)
{
}

/**
 * garrow_base_list_data_type_get_field:
 * @base_list_data_type: A #GArrowBaseListDataType.
 *
 * Returns: (transfer full): The field of value.
 *
 * Since: 21.0.0
 */
GArrowField *
garrow_base_list_data_type_get_field(GArrowBaseListDataType *base_list_data_type)
{
  auto data_type = GARROW_DATA_TYPE(base_list_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_base_list_data_type =
    std::static_pointer_cast<arrow::BaseListType>(arrow_data_type);

  auto arrow_field = arrow_base_list_data_type->value_field();
  return garrow_field_new_raw(&arrow_field, nullptr);
}

G_DEFINE_TYPE(GArrowListDataType, garrow_list_data_type, GARROW_TYPE_BASE_LIST_DATA_TYPE)

static void
garrow_list_data_type_init(GArrowListDataType *object)
{
}

static void
garrow_list_data_type_class_init(GArrowListDataTypeClass *klass)
{
}

/**
 * garrow_list_data_type_new:
 * @field: The field of elements
 *
 * Returns: The newly created list data type.
 */
GArrowListDataType *
garrow_list_data_type_new(GArrowField *field)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto arrow_data_type = std::make_shared<arrow::ListType>(arrow_field);

  GArrowListDataType *data_type = GARROW_LIST_DATA_TYPE(
    g_object_new(GARROW_TYPE_LIST_DATA_TYPE, "data-type", &arrow_data_type, NULL));
  return data_type;
}

/**
 * garrow_list_data_type_get_value_field:
 * @list_data_type: A #GArrowListDataType.
 *
 * Returns: (transfer full): The field of value.
 *
 * Deprecated: 0.13.0:
 *   Use garrow_list_data_type_get_field() instead.
 */
GArrowField *
garrow_list_data_type_get_value_field(GArrowListDataType *list_data_type)
{
  return garrow_list_data_type_get_field(list_data_type);
}

/**
 * garrow_list_data_type_get_field:
 * @list_data_type: A #GArrowListDataType.
 *
 * Returns: (transfer full): The field of value.
 *
 * Since: 0.13.0
 *
 * Deprecated: 21.0.0:
 *   Use garrow_base_list_data_type_get_field() instead.
 */
GArrowField *
garrow_list_data_type_get_field(GArrowListDataType *list_data_type)
{
  return garrow_base_list_data_type_get_field(GARROW_BASE_LIST_DATA_TYPE(list_data_type));
}

G_DEFINE_TYPE(GArrowLargeListDataType, garrow_large_list_data_type, GARROW_TYPE_DATA_TYPE)

static void
garrow_large_list_data_type_init(GArrowLargeListDataType *object)
{
}

static void
garrow_large_list_data_type_class_init(GArrowLargeListDataTypeClass *klass)
{
}

/**
 * garrow_large_list_data_type_new:
 * @field: The field of elements
 *
 * Returns: The newly created large list data type.
 *
 * Since: 0.16.0
 */
GArrowLargeListDataType *
garrow_large_list_data_type_new(GArrowField *field)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto arrow_data_type = std::make_shared<arrow::LargeListType>(arrow_field);

  GArrowLargeListDataType *data_type = GARROW_LARGE_LIST_DATA_TYPE(
    g_object_new(GARROW_TYPE_LARGE_LIST_DATA_TYPE, "data-type", &arrow_data_type, NULL));
  return data_type;
}

/**
 * garrow_large_list_data_type_get_field:
 * @large_list_data_type: A #GArrowLargeListDataType.
 *
 * Returns: (transfer full): The field of value.
 *
 * Since: 0.16.0
 */
GArrowField *
garrow_large_list_data_type_get_field(GArrowLargeListDataType *large_list_data_type)
{
  auto data_type = GARROW_DATA_TYPE(large_list_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_large_list_data_type =
    static_cast<arrow::LargeListType *>(arrow_data_type.get());

  auto arrow_field = arrow_large_list_data_type->value_field();
  return garrow_field_new_raw(&arrow_field, nullptr);
}

G_DEFINE_TYPE(GArrowStructDataType, garrow_struct_data_type, GARROW_TYPE_DATA_TYPE)

static void
garrow_struct_data_type_init(GArrowStructDataType *object)
{
}

static void
garrow_struct_data_type_class_init(GArrowStructDataTypeClass *klass)
{
}

/**
 * garrow_struct_data_type_new:
 * @fields: (element-type GArrowField): The fields of the struct.
 *
 * Returns: The newly created struct data type.
 */
GArrowStructDataType *
garrow_struct_data_type_new(GList *fields)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto *node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  auto arrow_data_type = std::make_shared<arrow::StructType>(arrow_fields);
  auto data_type =
    g_object_new(GARROW_TYPE_STRUCT_DATA_TYPE, "data-type", &arrow_data_type, NULL);
  return GARROW_STRUCT_DATA_TYPE(data_type);
}

/**
 * garrow_struct_data_type_get_n_fields:
 * @struct_data_type: A #GArrowStructDataType.
 *
 * Returns: The number of fields of the struct data type.
 *
 * Since: 0.12.0
 */
gint
garrow_struct_data_type_get_n_fields(GArrowStructDataType *struct_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(struct_data_type));
  return arrow_data_type->num_fields();
}

/**
 * garrow_struct_data_type_get_fields:
 * @struct_data_type: A #GArrowStructDataType.
 *
 * Returns: (transfer full) (element-type GArrowField):
 *   The fields of the struct data type.
 *
 * Since: 0.12.0
 */
GList *
garrow_struct_data_type_get_fields(GArrowStructDataType *struct_data_type)
{
  auto data_type = GARROW_DATA_TYPE(struct_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_fields = arrow_data_type->fields();

  GList *fields = NULL;
  for (auto arrow_field : arrow_fields) {
    fields = g_list_prepend(fields, garrow_field_new_raw(&arrow_field, nullptr));
  }
  return g_list_reverse(fields);
}

/**
 * garrow_struct_data_type_get_field:
 * @struct_data_type: A #GArrowStructDataType.
 * @i: The index of the target field.
 *
 * Returns: (transfer full) (nullable):
 *   The field at the index in the struct data type or %NULL on not found.
 *
 * Since: 0.12.0
 */
GArrowField *
garrow_struct_data_type_get_field(GArrowStructDataType *struct_data_type, gint i)
{
  auto data_type = GARROW_DATA_TYPE(struct_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);

  if (i < 0) {
    i += arrow_data_type->num_fields();
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= arrow_data_type->num_fields()) {
    return NULL;
  }

  auto arrow_field = arrow_data_type->field(i);
  if (arrow_field) {
    return garrow_field_new_raw(&arrow_field, nullptr);
  } else {
    return NULL;
  }
}

/**
 * garrow_struct_data_type_get_field_by_name:
 * @struct_data_type: A #GArrowStructDataType.
 * @name: The name of the target field.
 *
 * Returns: (transfer full) (nullable):
 *   The field that has the name in the struct data type or %NULL on not found.
 *
 * Since: 0.12.0
 */
GArrowField *
garrow_struct_data_type_get_field_by_name(GArrowStructDataType *struct_data_type,
                                          const gchar *name)
{
  auto data_type = GARROW_DATA_TYPE(struct_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_struct_data_type =
    std::static_pointer_cast<arrow::StructType>(arrow_data_type);

  auto arrow_field = arrow_struct_data_type->GetFieldByName(name);
  if (arrow_field) {
    return garrow_field_new_raw(&arrow_field, nullptr);
  } else {
    return NULL;
  }
}

/**
 * garrow_struct_data_type_get_field_index:
 * @struct_data_type: A #GArrowStructDataType.
 * @name: The name of the target field.
 *
 * Returns: The index of the target index in the struct data type
 *   or `-1` on not found.
 *
 * Since: 0.12.0
 */
gint
garrow_struct_data_type_get_field_index(GArrowStructDataType *struct_data_type,
                                        const gchar *name)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(struct_data_type));
  auto arrow_struct_data_type =
    std::static_pointer_cast<arrow::StructType>(arrow_data_type);

  return arrow_struct_data_type->GetFieldIndex(name);
}

G_DEFINE_TYPE(GArrowMapDataType, garrow_map_data_type, GARROW_TYPE_LIST_DATA_TYPE)

static void
garrow_map_data_type_init(GArrowMapDataType *object)
{
}

static void
garrow_map_data_type_class_init(GArrowMapDataTypeClass *klass)
{
}

/**
 * garrow_map_data_type_new:
 * @key_type: The key type of the map.
 * @item_type: The item type of the map.
 *
 * Returns: The newly created map data type.
 *
 * Since: 0.17.0
 */
GArrowMapDataType *
garrow_map_data_type_new(GArrowDataType *key_type, GArrowDataType *item_type)
{
  auto arrow_key_type = garrow_data_type_get_raw(key_type);
  auto arrow_item_type = garrow_data_type_get_raw(item_type);
  auto arrow_data_type =
    std::make_shared<arrow::MapType>(arrow_key_type, arrow_item_type);
  auto data_type =
    g_object_new(GARROW_TYPE_MAP_DATA_TYPE, "data-type", &arrow_data_type, NULL);
  return GARROW_MAP_DATA_TYPE(data_type);
}

/**
 * garrow_map_data_type_get_key_type:
 * @map_data_type: A #GArrowMapDataType.
 *
 * Returns: (transfer full): The key type of the map.
 *
 * Since: 0.17.0
 */
GArrowDataType *
garrow_map_data_type_get_key_type(GArrowMapDataType *map_data_type)
{
  auto data_type = GARROW_DATA_TYPE(map_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_map_data_type = std::static_pointer_cast<arrow::MapType>(arrow_data_type);
  auto arrow_key_type = arrow_map_data_type->key_type();
  return garrow_data_type_new_raw(&arrow_key_type);
}

/**
 * garrow_map_data_type_get_item_type:
 * @map_data_type: A #GArrowMapDataType.
 *
 * Returns: (transfer full): The item type of the map.
 *
 * Since: 0.17.0
 */
GArrowDataType *
garrow_map_data_type_get_item_type(GArrowMapDataType *map_data_type)
{
  auto data_type = GARROW_DATA_TYPE(map_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_map_data_type = std::static_pointer_cast<arrow::MapType>(arrow_data_type);
  auto arrow_item_type = arrow_map_data_type->item_type();
  return garrow_data_type_new_raw(&arrow_item_type);
}

G_DEFINE_ABSTRACT_TYPE(GArrowUnionDataType, garrow_union_data_type, GARROW_TYPE_DATA_TYPE)

static void
garrow_union_data_type_init(GArrowUnionDataType *object)
{
}

static void
garrow_union_data_type_class_init(GArrowUnionDataTypeClass *klass)
{
}

/**
 * garrow_union_data_type_get_n_fields:
 * @union_data_type: A #GArrowUnionDataType.
 *
 * Returns: The number of fields of the union data type.
 *
 * Since: 0.12.0
 */
gint
garrow_union_data_type_get_n_fields(GArrowUnionDataType *union_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(union_data_type));
  return arrow_data_type->num_fields();
}

/**
 * garrow_union_data_type_get_fields:
 * @union_data_type: A #GArrowUnionDataType.
 *
 * Returns: (transfer full) (element-type GArrowField):
 *   The fields of the union data type.
 *
 * Since: 0.12.0
 */
GList *
garrow_union_data_type_get_fields(GArrowUnionDataType *union_data_type)
{
  auto data_type = GARROW_DATA_TYPE(union_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_fields = arrow_data_type->fields();

  GList *fields = NULL;
  for (auto arrow_field : arrow_fields) {
    fields = g_list_prepend(fields, garrow_field_new_raw(&arrow_field, nullptr));
  }
  return g_list_reverse(fields);
}

/**
 * garrow_union_data_type_get_field:
 * @union_data_type: A #GArrowUnionDataType.
 * @i: The index of the target field.
 *
 * Returns: (transfer full) (nullable):
 *   The field at the index in the union data type or %NULL on not found.
 *
 * Since: 0.12.0
 */
GArrowField *
garrow_union_data_type_get_field(GArrowUnionDataType *union_data_type, gint i)
{
  auto data_type = GARROW_DATA_TYPE(union_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);

  if (i < 0) {
    i += arrow_data_type->num_fields();
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= arrow_data_type->num_fields()) {
    return NULL;
  }

  auto arrow_field = arrow_data_type->field(i);
  if (arrow_field) {
    return garrow_field_new_raw(&arrow_field, nullptr);
  } else {
    return NULL;
  }
}

/**
 * garrow_union_data_type_get_type_codes:
 * @union_data_type: A #GArrowUnionDataType.
 * @n_type_codes: (out): The number of type codes.
 *
 * Returns: (transfer full) (array length=n_type_codes):
 *   The codes for each field.
 *
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.12.0
 */
gint8 *
garrow_union_data_type_get_type_codes(GArrowUnionDataType *union_data_type,
                                      gsize *n_type_codes)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(union_data_type));
  auto arrow_union_data_type =
    std::static_pointer_cast<arrow::UnionType>(arrow_data_type);

  const auto arrow_type_codes = arrow_union_data_type->type_codes();
  const auto n = arrow_type_codes.size();
  auto type_codes = static_cast<gint8 *>(g_new(gint8, n));
  for (size_t i = 0; i < n; ++i) {
    type_codes[i] = arrow_type_codes[i];
  }
  *n_type_codes = n;
  return type_codes;
}

G_DEFINE_TYPE(GArrowSparseUnionDataType,
              garrow_sparse_union_data_type,
              GARROW_TYPE_UNION_DATA_TYPE)

static void
garrow_sparse_union_data_type_init(GArrowSparseUnionDataType *object)
{
}

static void
garrow_sparse_union_data_type_class_init(GArrowSparseUnionDataTypeClass *klass)
{
}

/**
 * garrow_sparse_union_data_type_new:
 * @fields: (element-type GArrowField): The fields of the union.
 * @type_codes: (array length=n_type_codes): The codes to specify each field.
 * @n_type_codes: The number of type codes.
 *
 * Returns: The newly created sparse union data type.
 */
GArrowSparseUnionDataType *
garrow_sparse_union_data_type_new(GList *fields, gint8 *type_codes, gsize n_type_codes)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  std::vector<int8_t> arrow_type_codes;
  for (gsize i = 0; i < n_type_codes; ++i) {
    arrow_type_codes.push_back(type_codes[i]);
  }

  auto arrow_data_type =
    std::make_shared<arrow::SparseUnionType>(arrow_fields, arrow_type_codes);
  auto data_type =
    g_object_new(GARROW_TYPE_SPARSE_UNION_DATA_TYPE, "data-type", &arrow_data_type, NULL);
  return GARROW_SPARSE_UNION_DATA_TYPE(data_type);
}

G_DEFINE_TYPE(GArrowDenseUnionDataType,
              garrow_dense_union_data_type,
              GARROW_TYPE_UNION_DATA_TYPE)

static void
garrow_dense_union_data_type_init(GArrowDenseUnionDataType *object)
{
}

static void
garrow_dense_union_data_type_class_init(GArrowDenseUnionDataTypeClass *klass)
{
}

/**
 * garrow_dense_union_data_type_new:
 * @fields: (element-type GArrowField): The fields of the union.
 * @type_codes: (array length=n_type_codes): The codes to specify each field.
 * @n_type_codes: The number of type codes.
 *
 * Returns: The newly created dense union data type.
 */
GArrowDenseUnionDataType *
garrow_dense_union_data_type_new(GList *fields, gint8 *type_codes, gsize n_type_codes)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  std::vector<int8_t> arrow_type_codes;
  for (gsize i = 0; i < n_type_codes; ++i) {
    arrow_type_codes.push_back(type_codes[i]);
  }

  auto arrow_data_type =
    std::make_shared<arrow::DenseUnionType>(arrow_fields, arrow_type_codes);
  auto data_type =
    g_object_new(GARROW_TYPE_DENSE_UNION_DATA_TYPE, "data-type", &arrow_data_type, NULL);
  return GARROW_DENSE_UNION_DATA_TYPE(data_type);
}

G_DEFINE_TYPE(GArrowDictionaryDataType,
              garrow_dictionary_data_type,
              GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_dictionary_data_type_init(GArrowDictionaryDataType *object)
{
}

static void
garrow_dictionary_data_type_class_init(GArrowDictionaryDataTypeClass *klass)
{
}

/**
 * garrow_dictionary_data_type_new:
 * @index_data_type: The data type of index.
 * @value_data_type: The data type of dictionary values.
 * @ordered: Whether dictionary contents are ordered or not.
 *
 * Returns: The newly created dictionary data type.
 *
 * Since: 0.8.0
 */
GArrowDictionaryDataType *
garrow_dictionary_data_type_new(GArrowDataType *index_data_type,
                                GArrowDataType *value_data_type,
                                gboolean ordered)
{
  auto arrow_index_data_type = garrow_data_type_get_raw(index_data_type);
  auto arrow_value_data_type = garrow_data_type_get_raw(value_data_type);
  auto arrow_data_type =
    arrow::dictionary(arrow_index_data_type, arrow_value_data_type, ordered);
  return GARROW_DICTIONARY_DATA_TYPE(garrow_data_type_new_raw(&arrow_data_type));
}

/**
 * garrow_dictionary_data_type_get_index_data_type:
 * @dictionary_data_type: The #GArrowDictionaryDataType.
 *
 * Returns: (transfer full): The #GArrowDataType of index.
 *
 * Since: 0.8.0
 */
GArrowDataType *
garrow_dictionary_data_type_get_index_data_type(
  GArrowDictionaryDataType *dictionary_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(dictionary_data_type));
  auto arrow_dictionary_data_type =
    std::static_pointer_cast<arrow::DictionaryType>(arrow_data_type);
  auto arrow_index_data_type = arrow_dictionary_data_type->index_type();
  return garrow_data_type_new_raw(&arrow_index_data_type);
}

/**
 * garrow_dictionary_data_type_get_value_data_type:
 * @dictionary_data_type: The #GArrowDictionaryDataType.
 *
 * Returns: (transfer full): The #GArrowDataType of dictionary values.
 *
 * Since: 0.14.0
 */
GArrowDataType *
garrow_dictionary_data_type_get_value_data_type(
  GArrowDictionaryDataType *dictionary_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(dictionary_data_type));
  auto arrow_dictionary_data_type =
    std::static_pointer_cast<arrow::DictionaryType>(arrow_data_type);
  auto arrow_value_data_type = arrow_dictionary_data_type->value_type();
  return garrow_data_type_new_raw(&arrow_value_data_type);
}

/**
 * garrow_dictionary_data_type_is_ordered:
 * @dictionary_data_type: The #GArrowDictionaryDataType.
 *
 * Returns: Whether dictionary contents are ordered or not.
 *
 * Since: 0.8.0
 */
gboolean
garrow_dictionary_data_type_is_ordered(GArrowDictionaryDataType *dictionary_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(dictionary_data_type));
  auto arrow_dictionary_data_type =
    std::static_pointer_cast<arrow::DictionaryType>(arrow_data_type);
  return arrow_dictionary_data_type->ordered();
}

G_DEFINE_TYPE(GArrowRunEndEncodedDataType,
              garrow_run_end_encoded_data_type,
              GARROW_TYPE_FIXED_WIDTH_DATA_TYPE)

static void
garrow_run_end_encoded_data_type_init(GArrowRunEndEncodedDataType *object)
{
}

static void
garrow_run_end_encoded_data_type_class_init(GArrowRunEndEncodedDataTypeClass *klass)
{
}

/**
 * garrow_run_end_encoded_data_type_new:
 * @run_end_data_type: The data type of run-end.
 * @value_data_type: The data type of value.
 *
 * Returns: The newly created run-end encoded data type.
 *
 * Since: 13.0.0
 */
GArrowRunEndEncodedDataType *
garrow_run_end_encoded_data_type_new(GArrowDataType *run_end_data_type,
                                     GArrowDataType *value_data_type)
{
  auto arrow_run_end_data_type = garrow_data_type_get_raw(run_end_data_type);
  auto arrow_value_data_type = garrow_data_type_get_raw(value_data_type);
  auto arrow_data_type =
    arrow::run_end_encoded(arrow_run_end_data_type, arrow_value_data_type);
  return GARROW_RUN_END_ENCODED_DATA_TYPE(garrow_data_type_new_raw(&arrow_data_type));
}

/**
 * garrow_run_end_encoded_data_type_get_run_end_data_type:
 * @data_type: The #GArrowRunEndEncodedDataType.
 *
 * Returns: (transfer full): The #GArrowDataType of run-end.
 *
 * Since: 13.0.0
 */
GArrowDataType *
garrow_run_end_encoded_data_type_get_run_end_data_type(
  GArrowRunEndEncodedDataType *data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_run_end_encoded_data_type =
    std::static_pointer_cast<arrow::RunEndEncodedType>(arrow_data_type);
  auto arrow_run_end_data_type = arrow_run_end_encoded_data_type->run_end_type();
  return garrow_data_type_new_raw(&arrow_run_end_data_type);
}

/**
 * garrow_run_end_encoded_data_type_get_value_data_type:
 * @data_type: The #GArrowRunEndEncodedDataType.
 *
 * Returns: (transfer full): The #GArrowDataType of value.
 *
 * Since: 13.0.0
 */
GArrowDataType *
garrow_run_end_encoded_data_type_get_value_data_type(
  GArrowRunEndEncodedDataType *data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(data_type));
  auto arrow_run_end_encoded_data_type =
    std::static_pointer_cast<arrow::RunEndEncodedType>(arrow_data_type);
  auto arrow_value_data_type = arrow_run_end_encoded_data_type->value_type();
  return garrow_data_type_new_raw(&arrow_value_data_type);
}

enum {
  PROP_LIST_SIZE = 1
};

G_DEFINE_TYPE(GArrowFixedSizeListDataType,
              garrow_fixed_size_list_data_type,
              GARROW_TYPE_BASE_LIST_DATA_TYPE)

static void
garrow_fixed_size_list_data_type_get_property(GObject *object,
                                              guint prop_id,
                                              GValue *value,
                                              GParamSpec *pspec)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(object));
  const auto arrow_fixed_size_list_type =
    std::static_pointer_cast<arrow::FixedSizeListType>(arrow_data_type);

  switch (prop_id) {
  case PROP_LIST_SIZE:
    g_value_set_int(value, arrow_fixed_size_list_type->list_size());
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_fixed_size_list_data_type_class_init(GArrowFixedSizeListDataTypeClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->get_property = garrow_fixed_size_list_data_type_get_property;

  spec = g_param_spec_int("list-size",
                          "List size",
                          "The list size of the elements",
                          0,
                          G_MAXINT,
                          0,
                          G_PARAM_READABLE);
  g_object_class_install_property(gobject_class, PROP_LIST_SIZE, spec);
}

static void
garrow_fixed_size_list_data_type_init(GArrowFixedSizeListDataType *object)
{
}

/**
 * garrow_fixed_size_list_data_type_new_data_type:
 * @value_type: The data type of an element of each list.
 * @list_size: The size of each list.
 *
 * Returns: A newly created fixed size list data type.
 *
 * Since: 21.0.0
 */
GArrowFixedSizeListDataType *
garrow_fixed_size_list_data_type_new_data_type(GArrowDataType *value_type,
                                               gint32 list_size)
{
  auto arrow_value_type = garrow_data_type_get_raw(value_type);
  auto arrow_fixed_size_list_data_type =
    arrow::fixed_size_list(arrow_value_type, list_size);
  return GARROW_FIXED_SIZE_LIST_DATA_TYPE(
    garrow_data_type_new_raw(&arrow_fixed_size_list_data_type));
}

/**
 * garrow_fixed_size_list_data_type_new_field:
 * @field: The field of lists.
 * @list_size: The size of value.
 *
 * Returns: A newly created fixed size list data type.
 *
 * Since: 21.0.0
 */
GArrowFixedSizeListDataType *
garrow_fixed_size_list_data_type_new_field(GArrowField *field, gint32 list_size)
{
  auto arrow_field = garrow_field_get_raw(field);
  auto arrow_fixed_size_list_data_type = arrow::fixed_size_list(arrow_field, list_size);
  return GARROW_FIXED_SIZE_LIST_DATA_TYPE(
    garrow_data_type_new_raw(&arrow_fixed_size_list_data_type));
}
G_END_DECLS
