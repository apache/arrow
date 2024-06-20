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

#include <arrow-glib/array.h>
#include <arrow-glib/ipc-options.h>
#include <arrow-glib/schema.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH (garrow_record_batch_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowRecordBatch, garrow_record_batch, GARROW, RECORD_BATCH, GObject)
struct _GArrowRecordBatchClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowRecordBatch *
garrow_record_batch_import(gpointer c_abi_array, GArrowSchema *schema, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_new(GArrowSchema *schema,
                        guint32 n_rows,
                        GList *columns,
                        GError **error);

GARROW_AVAILABLE_IN_6_0
gboolean
garrow_record_batch_export(GArrowRecordBatch *record_batch,
                           gpointer *c_abi_array,
                           gpointer *c_abi_schema,
                           GError **error);

GARROW_AVAILABLE_IN_ALL
gboolean
garrow_record_batch_equal(GArrowRecordBatch *record_batch,
                          GArrowRecordBatch *other_record_batch);
GARROW_AVAILABLE_IN_0_17
gboolean
garrow_record_batch_equal_metadata(GArrowRecordBatch *record_batch,
                                   GArrowRecordBatch *other_record_batch,
                                   gboolean check_metadata);

GARROW_AVAILABLE_IN_ALL
GArrowSchema *
garrow_record_batch_get_schema(GArrowRecordBatch *record_batch);

GARROW_AVAILABLE_IN_0_15
GArrowArray *
garrow_record_batch_get_column_data(GArrowRecordBatch *record_batch, gint i);

GARROW_AVAILABLE_IN_ALL
const gchar *
garrow_record_batch_get_column_name(GArrowRecordBatch *record_batch, gint i);

GARROW_AVAILABLE_IN_ALL
guint
garrow_record_batch_get_n_columns(GArrowRecordBatch *record_batch);

GARROW_AVAILABLE_IN_ALL
gint64
garrow_record_batch_get_n_rows(GArrowRecordBatch *record_batch);

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_slice(GArrowRecordBatch *record_batch, gint64 offset, gint64 length);

GARROW_AVAILABLE_IN_ALL
gchar *
garrow_record_batch_to_string(GArrowRecordBatch *record_batch, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_add_column(GArrowRecordBatch *record_batch,
                               guint i,
                               GArrowField *field,
                               GArrowArray *column,
                               GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_remove_column(GArrowRecordBatch *record_batch,
                                  guint i,
                                  GError **error);
GARROW_AVAILABLE_IN_1_0
GArrowBuffer *
garrow_record_batch_serialize(GArrowRecordBatch *record_batch,
                              GArrowWriteOptions *options,
                              GError **error);

#define GARROW_TYPE_RECORD_BATCH_ITERATOR (garrow_record_batch_iterator_get_type())
GARROW_AVAILABLE_IN_0_17
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchIterator,
                         garrow_record_batch_iterator,
                         GARROW,
                         RECORD_BATCH_ITERATOR,
                         GObject)
struct _GArrowRecordBatchIteratorClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_17
GArrowRecordBatchIterator *
garrow_record_batch_iterator_new(GList *record_batches);

GARROW_AVAILABLE_IN_0_17
GArrowRecordBatch *
garrow_record_batch_iterator_next(GArrowRecordBatchIterator *iterator, GError **error);

GARROW_AVAILABLE_IN_0_17
gboolean
garrow_record_batch_iterator_equal(GArrowRecordBatchIterator *iterator,
                                   GArrowRecordBatchIterator *other_iterator);

GARROW_AVAILABLE_IN_0_17
GList *
garrow_record_batch_iterator_to_list(GArrowRecordBatchIterator *iterator, GError **error);

G_END_DECLS
