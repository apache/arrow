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
 * #GArrowListDataType is a class for list data type.
 *
 * #GArrowStructDataType is a class for struct data type.
 *
 * #GArrowUnionDataType is a base class for union data types.
 *
 * #GArrowSparseUnionDataType is a class for sparse union data type.
 *
 * #GArrowDenseUnionDataType is a class for dense union data type.
 *
 * #GArrowDictionaryDataType is a class for dictionary data type.
 */

G_DEFINE_TYPE(GArrowListDataType,
              garrow_list_data_type,
              GARROW_TYPE_DATA_TYPE)

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
  auto arrow_data_type =
    std::make_shared<arrow::ListType>(arrow_field);

  GArrowListDataType *data_type =
    GARROW_LIST_DATA_TYPE(g_object_new(GARROW_TYPE_LIST_DATA_TYPE,
                                       "data-type", &arrow_data_type,
                                       NULL));
  return data_type;
}

/**
 * garrow_list_data_type_get_value_field:
 * @list_data_type: A #GArrowListDataType.
 *
 * Returns: (transfer full): The field of value.
 */
GArrowField *
garrow_list_data_type_get_value_field(GArrowListDataType *list_data_type)
{
  auto data_type = GARROW_DATA_TYPE(list_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  auto arrow_list_data_type =
    static_cast<arrow::ListType *>(arrow_data_type.get());

  auto arrow_field = arrow_list_data_type->value_field();
  return garrow_field_new_raw(&arrow_field, nullptr);
}


G_DEFINE_TYPE(GArrowStructDataType,
              garrow_struct_data_type,
              GARROW_TYPE_DATA_TYPE)

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
  auto data_type = g_object_new(GARROW_TYPE_STRUCT_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
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
  return arrow_data_type->num_children();
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
  auto arrow_fields = arrow_data_type->children();

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
garrow_struct_data_type_get_field(GArrowStructDataType *struct_data_type,
                                  gint i)
{
  auto data_type = GARROW_DATA_TYPE(struct_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);

  if (i < 0) {
    i += arrow_data_type->num_children();
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= arrow_data_type->num_children()) {
    return NULL;
  }

  auto arrow_field = arrow_data_type->child(i);
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


G_DEFINE_ABSTRACT_TYPE(GArrowUnionDataType,
                       garrow_union_data_type,
                       GARROW_TYPE_DATA_TYPE)

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
  return arrow_data_type->num_children();
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
  auto arrow_fields = arrow_data_type->children();

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
garrow_union_data_type_get_field(GArrowUnionDataType *union_data_type,
                                 gint i)
{
  auto data_type = GARROW_DATA_TYPE(union_data_type);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);

  if (i < 0) {
    i += arrow_data_type->num_children();
  }
  if (i < 0) {
    return NULL;
  }
  if (i >= arrow_data_type->num_children()) {
    return NULL;
  }

  auto arrow_field = arrow_data_type->child(i);
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
guint8 *
garrow_union_data_type_get_type_codes(GArrowUnionDataType *union_data_type,
                                      gsize *n_type_codes)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(union_data_type));
  auto arrow_union_data_type =
    std::static_pointer_cast<arrow::UnionType>(arrow_data_type);

  const auto arrow_type_codes = arrow_union_data_type->type_codes();
  const auto n = arrow_type_codes.size();
  auto type_codes = static_cast<guint8 *>(g_new(guint8, n));
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
garrow_sparse_union_data_type_new(GList *fields,
                                  guint8 *type_codes,
                                  gsize n_type_codes)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  std::vector<uint8_t> arrow_type_codes;
  for (gsize i = 0; i < n_type_codes; ++i) {
    arrow_type_codes.push_back(type_codes[i]);
  }

  auto arrow_data_type =
    std::make_shared<arrow::UnionType>(arrow_fields,
                                       arrow_type_codes,
                                       arrow::UnionMode::SPARSE);
  auto data_type = g_object_new(GARROW_TYPE_SPARSE_UNION_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
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
garrow_dense_union_data_type_new(GList *fields,
                                 guint8 *type_codes,
                                 gsize n_type_codes)
{
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  for (auto node = fields; node; node = g_list_next(node)) {
    auto field = GARROW_FIELD(node->data);
    auto arrow_field = garrow_field_get_raw(field);
    arrow_fields.push_back(arrow_field);
  }

  std::vector<uint8_t> arrow_type_codes;
  for (gsize i = 0; i < n_type_codes; ++i) {
    arrow_type_codes.push_back(type_codes[i]);
  }

  auto arrow_data_type =
    std::make_shared<arrow::UnionType>(arrow_fields,
                                       arrow_type_codes,
                                       arrow::UnionMode::DENSE);
  auto data_type = g_object_new(GARROW_TYPE_DENSE_UNION_DATA_TYPE,
                                "data-type", &arrow_data_type,
                                NULL);
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
 * @dictionary: The dictionary.
 * @ordered: Whether dictionary contents are ordered or not.
 *
 * Returns: The newly created dictionary data type.
 *
 * Since: 0.8.0
 */
GArrowDictionaryDataType *
garrow_dictionary_data_type_new(GArrowDataType *index_data_type,
                                GArrowArray *dictionary,
                                gboolean ordered)
{
  auto arrow_index_data_type = garrow_data_type_get_raw(index_data_type);
  auto arrow_dictionary = garrow_array_get_raw(dictionary);
  auto arrow_data_type = arrow::dictionary(arrow_index_data_type,
                                           arrow_dictionary,
                                           ordered);
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
garrow_dictionary_data_type_get_index_data_type(GArrowDictionaryDataType *dictionary_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(dictionary_data_type));
  auto arrow_dictionary_data_type =
    std::static_pointer_cast<arrow::DictionaryType>(arrow_data_type);
  auto arrow_index_data_type = arrow_dictionary_data_type->index_type();
  return garrow_data_type_new_raw(&arrow_index_data_type);
}

/**
 * garrow_dictionary_data_type_get_dictionary:
 * @dictionary_data_type: The #GArrowDictionaryDataType.
 *
 * Returns: (transfer full): The dictionary as #GArrowArray.
 *
 * Since: 0.8.0
 */
GArrowArray *
garrow_dictionary_data_type_get_dictionary(GArrowDictionaryDataType *dictionary_data_type)
{
  auto arrow_data_type = garrow_data_type_get_raw(GARROW_DATA_TYPE(dictionary_data_type));
  auto arrow_dictionary_data_type =
    std::static_pointer_cast<arrow::DictionaryType>(arrow_data_type);
  auto arrow_dictionary = arrow_dictionary_data_type->dictionary();
  return garrow_array_new_raw(&arrow_dictionary);
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

G_END_DECLS
