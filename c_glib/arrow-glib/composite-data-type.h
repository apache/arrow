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

#pragma once

#include <arrow-glib/basic-array.h>
#include <arrow-glib/basic-data-type.h>
#include <arrow-glib/field.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_BASE_LIST_DATA_TYPE (garrow_base_list_data_type_get_type())
GARROW_AVAILABLE_IN_21_0
G_DECLARE_DERIVABLE_TYPE(GArrowBaseListDataType,
                         garrow_base_list_data_type,
                         GARROW,
                         BASE_LIST_DATA_TYPE,
                         GArrowDataType)
struct _GArrowBaseListDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_21_0
GArrowField *
garrow_base_list_data_type_get_field(GArrowBaseListDataType *base_list_data_type);

#define GARROW_TYPE_LIST_DATA_TYPE (garrow_list_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowListDataType,
                         garrow_list_data_type,
                         GARROW,
                         LIST_DATA_TYPE,
                         GArrowBaseListDataType)
struct _GArrowListDataTypeClass
{
  GArrowBaseListDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowListDataType *
garrow_list_data_type_new(GArrowField *field);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_0_13_FOR(garrow_list_data_type_get_field)
GArrowField *
garrow_list_data_type_get_value_field(GArrowListDataType *list_data_type);
#endif

GARROW_AVAILABLE_IN_0_13
GArrowField *
garrow_list_data_type_get_field(GArrowListDataType *list_data_type);

#define GARROW_TYPE_LARGE_LIST_DATA_TYPE (garrow_large_list_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowLargeListDataType,
                         garrow_large_list_data_type,
                         GARROW,
                         LARGE_LIST_DATA_TYPE,
                         GArrowDataType)
struct _GArrowLargeListDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeListDataType *
garrow_large_list_data_type_new(GArrowField *field);

GARROW_AVAILABLE_IN_0_16
GArrowField *
garrow_large_list_data_type_get_field(GArrowLargeListDataType *large_list_data_type);

#define GARROW_TYPE_STRUCT_DATA_TYPE (garrow_struct_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowStructDataType, garrow_struct_data_type, GARROW, STRUCT_DATA_TYPE, GArrowDataType)
struct _GArrowStructDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowStructDataType *
garrow_struct_data_type_new(GList *fields);

GARROW_AVAILABLE_IN_ALL
gint
garrow_struct_data_type_get_n_fields(GArrowStructDataType *struct_data_type);

GARROW_AVAILABLE_IN_ALL
GList *
garrow_struct_data_type_get_fields(GArrowStructDataType *struct_data_type);

GARROW_AVAILABLE_IN_ALL
GArrowField *
garrow_struct_data_type_get_field(GArrowStructDataType *struct_data_type, gint i);

GARROW_AVAILABLE_IN_ALL
GArrowField *
garrow_struct_data_type_get_field_by_name(GArrowStructDataType *struct_data_type,
                                          const gchar *name);

GARROW_AVAILABLE_IN_ALL
gint
garrow_struct_data_type_get_field_index(GArrowStructDataType *struct_data_type,
                                        const gchar *name);

#define GARROW_TYPE_MAP_DATA_TYPE (garrow_map_data_type_get_type())
GARROW_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(
  GArrowMapDataType, garrow_map_data_type, GARROW, MAP_DATA_TYPE, GArrowListDataType)
struct _GArrowMapDataTypeClass
{
  GArrowListDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowMapDataType *
garrow_map_data_type_new(GArrowDataType *key_type, GArrowDataType *item_type);
GARROW_AVAILABLE_IN_0_17
GArrowDataType *
garrow_map_data_type_get_key_type(GArrowMapDataType *map_data_type);
GARROW_AVAILABLE_IN_0_17
GArrowDataType *
garrow_map_data_type_get_item_type(GArrowMapDataType *map_data_type);

#define GARROW_TYPE_UNION_DATA_TYPE (garrow_union_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUnionDataType, garrow_union_data_type, GARROW, UNION_DATA_TYPE, GArrowDataType)
struct _GArrowUnionDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
gint
garrow_union_data_type_get_n_fields(GArrowUnionDataType *union_data_type);

GARROW_AVAILABLE_IN_ALL
GList *
garrow_union_data_type_get_fields(GArrowUnionDataType *union_data_type);

GARROW_AVAILABLE_IN_ALL
GArrowField *
garrow_union_data_type_get_field(GArrowUnionDataType *union_data_type, gint i);

GARROW_AVAILABLE_IN_ALL
gint8 *
garrow_union_data_type_get_type_codes(GArrowUnionDataType *union_data_type,
                                      gsize *n_type_codes);

#define GARROW_TYPE_SPARSE_UNION_DATA_TYPE (garrow_sparse_union_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowSparseUnionDataType,
                         garrow_sparse_union_data_type,
                         GARROW,
                         SPARSE_UNION_DATA_TYPE,
                         GArrowUnionDataType)
struct _GArrowSparseUnionDataTypeClass
{
  GArrowUnionDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowSparseUnionDataType *
garrow_sparse_union_data_type_new(GList *fields, gint8 *type_codes, gsize n_type_codes);

#define GARROW_TYPE_DENSE_UNION_DATA_TYPE (garrow_dense_union_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDenseUnionDataType,
                         garrow_dense_union_data_type,
                         GARROW,
                         DENSE_UNION_DATA_TYPE,
                         GArrowUnionDataType)
struct _GArrowDenseUnionDataTypeClass
{
  GArrowUnionDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDenseUnionDataType *
garrow_dense_union_data_type_new(GList *fields, gint8 *type_codes, gsize n_type_codes);

#define GARROW_TYPE_DICTIONARY_DATA_TYPE (garrow_dictionary_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDictionaryDataType,
                         garrow_dictionary_data_type,
                         GARROW,
                         DICTIONARY_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowDictionaryDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDictionaryDataType *
garrow_dictionary_data_type_new(GArrowDataType *index_data_type,
                                GArrowDataType *value_data_type,
                                gboolean ordered);

GARROW_AVAILABLE_IN_ALL
GArrowDataType *
garrow_dictionary_data_type_get_index_data_type(
  GArrowDictionaryDataType *dictionary_data_type);

GARROW_AVAILABLE_IN_0_14
GArrowDataType *
garrow_dictionary_data_type_get_value_data_type(
  GArrowDictionaryDataType *dictionary_data_type);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_dictionary_data_type_is_ordered(GArrowDictionaryDataType *dictionary_data_type);

#define GARROW_TYPE_RUN_END_ENCODED_DATA_TYPE                                            \
  (garrow_run_end_encoded_data_type_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRunEndEncodedDataType,
                         garrow_run_end_encoded_data_type,
                         GARROW,
                         RUN_END_ENCODED_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowRunEndEncodedDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_13_0
GArrowRunEndEncodedDataType *
garrow_run_end_encoded_data_type_new(GArrowDataType *run_end_data_type,
                                     GArrowDataType *value_data_type);
GARROW_AVAILABLE_IN_13_0
GArrowDataType *
garrow_run_end_encoded_data_type_get_run_end_data_type(
  GArrowRunEndEncodedDataType *data_type);

GARROW_AVAILABLE_IN_13_0
GArrowDataType *
garrow_run_end_encoded_data_type_get_value_data_type(
  GArrowRunEndEncodedDataType *data_type);

#define GARROW_TYPE_FIXED_SIZE_LIST_DATA_TYPE                                            \
  (garrow_fixed_size_list_data_type_get_type())
GARROW_AVAILABLE_IN_21_0
G_DECLARE_DERIVABLE_TYPE(GArrowFixedSizeListDataType,
                         garrow_fixed_size_list_data_type,
                         GARROW,
                         FIXED_SIZE_LIST_DATA_TYPE,
                         GArrowBaseListDataType)
struct _GArrowFixedSizeListDataTypeClass
{
  GArrowBaseListDataTypeClass parent_class;
};

GARROW_AVAILABLE_IN_21_0
GArrowFixedSizeListDataType *
garrow_fixed_size_list_data_type_new_data_type(GArrowDataType *value_type,
                                               gint32 list_size);

GARROW_AVAILABLE_IN_21_0
GArrowFixedSizeListDataType *
garrow_fixed_size_list_data_type_new_field(GArrowField *field, gint32 list_size);
G_END_DECLS
