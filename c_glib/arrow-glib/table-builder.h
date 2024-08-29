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

#include <arrow-glib/array-builder.h>
#include <arrow-glib/gobject-type.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH_BUILDER (garrow_record_batch_builder_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchBuilder,
                         garrow_record_batch_builder,
                         GARROW,
                         RECORD_BATCH_BUILDER,
                         GObject)
struct _GArrowRecordBatchBuilderClass
{
  GObjectClass parent_class;
};

GArrowRecordBatchBuilder *garrow_record_batch_builder_new(GArrowSchema *schema,
                                                          GError **error);

gint64 garrow_record_batch_builder_get_initial_capacity(GArrowRecordBatchBuilder *builder);
void garrow_record_batch_builder_set_initial_capacity(GArrowRecordBatchBuilder *builder,
                                                      gint64 capacity);
GArrowSchema *garrow_record_batch_builder_get_schema(GArrowRecordBatchBuilder *builder);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_13_FOR(garrow_record_batch_builder_get_n_columns)
gint garrow_record_batch_builder_get_n_fields(GArrowRecordBatchBuilder *builder);
#endif
GARROW_AVAILABLE_IN_0_13
gint
garrow_record_batch_builder_get_n_columns(GArrowRecordBatchBuilder *builder);
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_DEPRECATED_IN_0_13_FOR(garrow_record_batch_builder_get_column_builder)
GArrowArrayBuilder *garrow_record_batch_builder_get_field(GArrowRecordBatchBuilder *builder,
                                                          gint i);
#endif
GARROW_AVAILABLE_IN_0_13
GArrowArrayBuilder *
garrow_record_batch_builder_get_column_builder(GArrowRecordBatchBuilder *builder,
                                               gint i);

GArrowRecordBatch *garrow_record_batch_builder_flush(GArrowRecordBatchBuilder *builder,
                                                     GError **error);


G_END_DECLS
