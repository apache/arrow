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

#include <arrow-glib/version.h>

#include <arrow-glib/basic-array.h>
#include <arrow-glib/data-type.h>

G_BEGIN_DECLS

#define GARROW_TYPE_LIST_ARRAY (garrow_list_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowListArray, garrow_list_array, GARROW, LIST_ARRAY, GArrowArray)
struct _GArrowListArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowListArray *
garrow_list_array_new(GArrowDataType *data_type,
                      gint64 length,
                      GArrowBuffer *value_offsets,
                      GArrowArray *values,
                      GArrowBuffer *null_bitmap,
                      gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
GArrowDataType *
garrow_list_array_get_value_type(GArrowListArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_list_array_get_value(GArrowListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
GArrowArray *
garrow_list_array_get_values(GArrowListArray *array);

GARROW_AVAILABLE_IN_2_0
gint32
garrow_list_array_get_value_offset(GArrowListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
gint32
garrow_list_array_get_value_length(GArrowListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
const gint32 *
garrow_list_array_get_value_offsets(GArrowListArray *array, gint64 *n_offsets);

#define GARROW_TYPE_LARGE_LIST_ARRAY (garrow_large_list_array_get_type())
GARROW_AVAILABLE_IN_0_16
G_DECLARE_DERIVABLE_TYPE(
  GArrowLargeListArray, garrow_large_list_array, GARROW, LARGE_LIST_ARRAY, GArrowArray)
struct _GArrowLargeListArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_0_16
GArrowLargeListArray *
garrow_large_list_array_new(GArrowDataType *data_type,
                            gint64 length,
                            GArrowBuffer *value_offsets,
                            GArrowArray *values,
                            GArrowBuffer *null_bitmap,
                            gint64 n_nulls);

GARROW_AVAILABLE_IN_0_16
GArrowDataType *
garrow_large_list_array_get_value_type(GArrowLargeListArray *array);

GARROW_AVAILABLE_IN_0_16
GArrowArray *
garrow_large_list_array_get_value(GArrowLargeListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
GArrowArray *
garrow_large_list_array_get_values(GArrowLargeListArray *array);

GARROW_AVAILABLE_IN_2_0
gint64
garrow_large_list_array_get_value_offset(GArrowLargeListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
gint64
garrow_large_list_array_get_value_length(GArrowLargeListArray *array, gint64 i);

GARROW_AVAILABLE_IN_2_0
const gint64 *
garrow_large_list_array_get_value_offsets(GArrowLargeListArray *array, gint64 *n_offsets);

#define GARROW_TYPE_STRUCT_ARRAY (garrow_struct_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowStructArray, garrow_struct_array, GARROW, STRUCT_ARRAY, GArrowArray)
struct _GArrowStructArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowStructArray *
garrow_struct_array_new(GArrowDataType *data_type,
                        gint64 length,
                        GList *fields,
                        GArrowBuffer *null_bitmap,
                        gint64 n_nulls);

GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_struct_array_get_field(GArrowStructArray *array, gint i);

GARROW_AVAILABLE_IN_ALL
GList *
garrow_struct_array_get_fields(GArrowStructArray *array);

GARROW_AVAILABLE_IN_0_10
GList *
garrow_struct_array_flatten(GArrowStructArray *array, GError **error);

#define GARROW_TYPE_MAP_ARRAY (garrow_map_array_get_type())
GARROW_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(
  GArrowMapArray, garrow_map_array, GARROW, MAP_ARRAY, GArrowListArray)
struct _GArrowMapArrayClass
{
  GArrowListArrayClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowMapArray *
garrow_map_array_new(GArrowArray *offsets,
                     GArrowArray *keys,
                     GArrowArray *items,
                     GError **error);

GARROW_AVAILABLE_IN_0_17
GArrowArray *
garrow_map_array_get_keys(GArrowMapArray *array);

GARROW_AVAILABLE_IN_0_17
GArrowArray *
garrow_map_array_get_items(GArrowMapArray *array);

#define GARROW_TYPE_UNION_ARRAY (garrow_union_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowUnionArray, garrow_union_array, GARROW, UNION_ARRAY, GArrowArray)
struct _GArrowUnionArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_12_0
gint8
garrow_union_array_get_type_code(GArrowUnionArray *array, gint64 i);

GARROW_AVAILABLE_IN_12_0
gint
garrow_union_array_get_child_id(GArrowUnionArray *array, gint64 i);

GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_union_array_get_field(GArrowUnionArray *array, gint i);

#define GARROW_TYPE_SPARSE_UNION_ARRAY (garrow_sparse_union_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowSparseUnionArray,
                         garrow_sparse_union_array,
                         GARROW,
                         SPARSE_UNION_ARRAY,
                         GArrowUnionArray)
struct _GArrowSparseUnionArrayClass
{
  GArrowUnionArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowSparseUnionArray *
garrow_sparse_union_array_new(GArrowInt8Array *type_ids, GList *fields, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowSparseUnionArray *
garrow_sparse_union_array_new_data_type(GArrowSparseUnionDataType *data_type,
                                        GArrowInt8Array *type_ids,
                                        GList *fields,
                                        GError **error);

#define GARROW_TYPE_DENSE_UNION_ARRAY (garrow_dense_union_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowDenseUnionArray,
                         garrow_dense_union_array,
                         GARROW,
                         DENSE_UNION_ARRAY,
                         GArrowUnionArray)
struct _GArrowDenseUnionArrayClass
{
  GArrowUnionArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDenseUnionArray *
garrow_dense_union_array_new(GArrowInt8Array *type_ids,
                             GArrowInt32Array *value_offsets,
                             GList *fields,
                             GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowDenseUnionArray *
garrow_dense_union_array_new_data_type(GArrowDenseUnionDataType *data_type,
                                       GArrowInt8Array *type_ids,
                                       GArrowInt32Array *value_offsets,
                                       GList *fields,
                                       GError **error);

GARROW_AVAILABLE_IN_12_0
gint32
garrow_dense_union_array_get_value_offset(GArrowDenseUnionArray *array, gint64 i);

#define GARROW_TYPE_DICTIONARY_ARRAY (garrow_dictionary_array_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowDictionaryArray, garrow_dictionary_array, GARROW, DICTIONARY_ARRAY, GArrowArray)
struct _GArrowDictionaryArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowDictionaryArray *
garrow_dictionary_array_new(GArrowDataType *data_type,
                            GArrowArray *indices,
                            GArrowArray *dictionary,
                            GError **error);
GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_dictionary_array_get_indices(GArrowDictionaryArray *array);

GARROW_AVAILABLE_IN_ALL
GArrowArray *
garrow_dictionary_array_get_dictionary(GArrowDictionaryArray *array);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
GARROW_DEPRECATED_IN_1_0_FOR(garrow_array_get_value_data_type)
GArrowDictionaryDataType *
garrow_dictionary_array_get_dictionary_data_type(GArrowDictionaryArray *array);
#endif

#define GARROW_TYPE_RUN_END_ENCODED_ARRAY (garrow_run_end_encoded_array_get_type())
GARROW_AVAILABLE_IN_13_0
G_DECLARE_DERIVABLE_TYPE(GArrowRunEndEncodedArray,
                         garrow_run_end_encoded_array,
                         GARROW,
                         RUN_END_ENCODED_ARRAY,
                         GArrowArray)
struct _GArrowRunEndEncodedArrayClass
{
  GArrowArrayClass parent_class;
};

GARROW_AVAILABLE_IN_13_0
GArrowRunEndEncodedArray *
garrow_run_end_encoded_array_new(GArrowDataType *data_type,
                                 gint64 logical_length,
                                 GArrowArray *run_ends,
                                 GArrowArray *values,
                                 gint64 logical_offset,
                                 GError **error);
GARROW_AVAILABLE_IN_13_0
GArrowArray *
garrow_run_end_encoded_array_get_run_ends(GArrowRunEndEncodedArray *array);

GARROW_AVAILABLE_IN_13_0
GArrowArray *
garrow_run_end_encoded_array_get_values(GArrowRunEndEncodedArray *array);

GARROW_AVAILABLE_IN_13_0
GArrowArray *
garrow_run_end_encoded_array_get_logical_run_ends(GArrowRunEndEncodedArray *array,
                                                  GError **error);
GARROW_AVAILABLE_IN_13_0
GArrowArray *
garrow_run_end_encoded_array_get_logical_values(GArrowRunEndEncodedArray *array);

GARROW_AVAILABLE_IN_13_0
gint64
garrow_run_end_encoded_array_find_physical_offset(GArrowRunEndEncodedArray *array);

GARROW_AVAILABLE_IN_13_0
gint64
garrow_run_end_encoded_array_find_physical_length(GArrowRunEndEncodedArray *array);

G_END_DECLS
