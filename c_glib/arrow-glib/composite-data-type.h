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

#define GARROW_TYPE_LIST_DATA_TYPE (garrow_list_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowListDataType,
                         garrow_list_data_type,
                         GARROW,
                         LIST_DATA_TYPE,
                         GArrowDataType)
struct _GArrowListDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GArrowListDataType *garrow_list_data_type_new      (GArrowField *field);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_13_FOR(garrow_list_data_type_get_field)
GArrowField *garrow_list_data_type_get_value_field (GArrowListDataType *list_data_type);
#endif
GARROW_AVAILABLE_IN_0_13
GArrowField *garrow_list_data_type_get_field (GArrowListDataType *list_data_type);


#define GARROW_TYPE_STRUCT_DATA_TYPE (garrow_struct_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowStructDataType,
                         garrow_struct_data_type,
                         GARROW,
                         STRUCT_DATA_TYPE,
                         GArrowDataType)
struct _GArrowStructDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

GArrowStructDataType *garrow_struct_data_type_new      (GList *fields);
gint
garrow_struct_data_type_get_n_fields(GArrowStructDataType *struct_data_type);
GList *
garrow_struct_data_type_get_fields(GArrowStructDataType *struct_data_type);
GArrowField *
garrow_struct_data_type_get_field(GArrowStructDataType *struct_data_type,
                                  gint i);
GArrowField *
garrow_struct_data_type_get_field_by_name(GArrowStructDataType *struct_data_type,
                                          const gchar *name);
gint
garrow_struct_data_type_get_field_index(GArrowStructDataType *struct_data_type,
                                        const gchar *name);


#define GARROW_TYPE_UNION_DATA_TYPE (garrow_union_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowUnionDataType,
                         garrow_union_data_type,
                         GARROW,
                         UNION_DATA_TYPE,
                         GArrowDataType)
struct _GArrowUnionDataTypeClass
{
  GArrowDataTypeClass parent_class;
};

gint
garrow_union_data_type_get_n_fields(GArrowUnionDataType *union_data_type);
GList *
garrow_union_data_type_get_fields(GArrowUnionDataType *union_data_type);
GArrowField *
garrow_union_data_type_get_field(GArrowUnionDataType *union_data_type,
                                 gint i);
guint8 *
garrow_union_data_type_get_type_codes(GArrowUnionDataType *union_data_type,
                                      gsize *n_type_codes);


#define GARROW_TYPE_SPARSE_UNION_DATA_TYPE      \
  (garrow_sparse_union_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowSparseUnionDataType,
                         garrow_sparse_union_data_type,
                         GARROW,
                         SPARSE_UNION_DATA_TYPE,
                         GArrowUnionDataType)
struct _GArrowSparseUnionDataTypeClass
{
  GArrowUnionDataTypeClass parent_class;
};

GArrowSparseUnionDataType *
garrow_sparse_union_data_type_new(GList *fields,
                                  guint8 *type_codes,
                                  gsize n_type_codes);


#define GARROW_TYPE_DENSE_UNION_DATA_TYPE       \
  (garrow_dense_union_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDenseUnionDataType,
                         garrow_dense_union_data_type,
                         GARROW,
                         DENSE_UNION_DATA_TYPE,
                         GArrowUnionDataType)
struct _GArrowDenseUnionDataTypeClass
{
  GArrowUnionDataTypeClass parent_class;
};

GArrowDenseUnionDataType *
garrow_dense_union_data_type_new(GList *fields,
                                 guint8 *type_codes,
                                 gsize n_type_codes);


#define GARROW_TYPE_DICTIONARY_DATA_TYPE (garrow_dictionary_data_type_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowDictionaryDataType,
                         garrow_dictionary_data_type,
                         GARROW,
                         DICTIONARY_DATA_TYPE,
                         GArrowFixedWidthDataType)
struct _GArrowDictionaryDataTypeClass
{
  GArrowFixedWidthDataTypeClass parent_class;
};

GArrowDictionaryDataType *
garrow_dictionary_data_type_new(GArrowDataType *index_data_type,
                                GArrowArray *dictionary,
                                gboolean ordered);
GArrowDataType *
garrow_dictionary_data_type_get_index_data_type(GArrowDictionaryDataType *dictionary_data_type);
GArrowArray *
garrow_dictionary_data_type_get_dictionary(GArrowDictionaryDataType *dictionary_data_type);
gboolean
garrow_dictionary_data_type_is_ordered(GArrowDictionaryDataType *dictionary_data_type);


G_END_DECLS
