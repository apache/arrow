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

#include <arrow-glib/chunked-array.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>
#include <arrow-glib/version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_TABLE (garrow_table_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTable,
                         garrow_table,
                         GARROW,
                         TABLE,
                         GObject)
struct _GArrowTableClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_12
GArrowTable *
garrow_table_new_values(GArrowSchema *schema,
                        GList *values,
                        GError **error);
GARROW_AVAILABLE_IN_1_0
GArrowTable *
garrow_table_new_chunked_arrays(GArrowSchema *schema,
                                GArrowChunkedArray **chunked_arrays,
                                gsize n_chunked_arrays,
                                GError **error);
GARROW_AVAILABLE_IN_0_12
GArrowTable *
garrow_table_new_arrays(GArrowSchema *schema,
                        GArrowArray **arrays,
                        gsize n_arrays,
                        GError **error);
GARROW_AVAILABLE_IN_0_12
GArrowTable *
garrow_table_new_record_batches(GArrowSchema *schema,
                                GArrowRecordBatch **record_batches,
                                gsize n_record_batches,
                                GError **error);

gboolean        garrow_table_equal         (GArrowTable *table,
                                            GArrowTable *other_table);

GArrowSchema   *garrow_table_get_schema    (GArrowTable *table);
GARROW_AVAILABLE_IN_1_0
GArrowChunkedArray *
garrow_table_get_column_data(GArrowTable *table,
                             gint i);

guint           garrow_table_get_n_columns (GArrowTable *table);
guint64         garrow_table_get_n_rows    (GArrowTable *table);

GARROW_AVAILABLE_IN_1_0
GArrowTable    *garrow_table_add_column    (GArrowTable *table,
                                            guint i,
                                            GArrowField *field,
                                            GArrowChunkedArray *chunked_array,
                                            GError **error);
GArrowTable    *garrow_table_remove_column (GArrowTable *table,
                                            guint i,
                                            GError **error);
GARROW_AVAILABLE_IN_1_0
GArrowTable    *garrow_table_replace_column(GArrowTable *table,
                                            guint i,
                                            GArrowField *field,
                                            GArrowChunkedArray *chunked_array,
                                            GError **error);
gchar          *garrow_table_to_string     (GArrowTable *table,
                                            GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowTable *
garrow_table_concatenate(GArrowTable *table,
                         GList *other_tables,
                         GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowTable*
garrow_table_slice(GArrowTable *table,
                   gint64 offset,
                   gint64 length);

G_END_DECLS
