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
#include <arrow-glib/schema.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH                \
  (garrow_record_batch_get_type())
#define GARROW_RECORD_BATCH(obj)                        \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                    \
                              GARROW_TYPE_RECORD_BATCH, \
                              GArrowRecordBatch))
#define GARROW_RECORD_BATCH_CLASS(klass)                \
  (G_TYPE_CHECK_CLASS_CAST((klass),                     \
                           GARROW_TYPE_RECORD_BATCH,    \
                           GArrowRecordBatchClass))
#define GARROW_IS_RECORD_BATCH(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_RECORD_BATCH))
#define GARROW_IS_RECORD_BATCH_CLASS(klass)             \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_RECORD_BATCH))
#define GARROW_RECORD_BATCH_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_RECORD_BATCH,  \
                             GArrowRecordBatchClass))

typedef struct _GArrowRecordBatch         GArrowRecordBatch;
typedef struct _GArrowRecordBatchClass    GArrowRecordBatchClass;

/**
 * GArrowRecordBatch:
 *
 * It wraps `arrow::RecordBatch`.
 */
struct _GArrowRecordBatch
{
  /*< private >*/
  GObject parent_instance;
};

struct _GArrowRecordBatchClass
{
  GObjectClass parent_class;
};

GType garrow_record_batch_get_type(void) G_GNUC_CONST;

GArrowRecordBatch *garrow_record_batch_new(GArrowSchema *schema,
                                           guint32 n_rows,
                                           GList *columns,
                                           GError **error);

gboolean garrow_record_batch_equal(GArrowRecordBatch *record_batch,
                                   GArrowRecordBatch *other_record_batch);

GArrowSchema *garrow_record_batch_get_schema     (GArrowRecordBatch *record_batch);
GArrowArray  *garrow_record_batch_get_column     (GArrowRecordBatch *record_batch,
                                                  gint i);
GList        *garrow_record_batch_get_columns    (GArrowRecordBatch *record_batch);
const gchar  *garrow_record_batch_get_column_name(GArrowRecordBatch *record_batch,
                                                  gint i);
guint         garrow_record_batch_get_n_columns  (GArrowRecordBatch *record_batch);
gint64        garrow_record_batch_get_n_rows     (GArrowRecordBatch *record_batch);
GArrowRecordBatch *garrow_record_batch_slice     (GArrowRecordBatch *record_batch,
                                                  gint64 offset,
                                                  gint64 length);

gchar        *garrow_record_batch_to_string      (GArrowRecordBatch *record_batch,
                                                  GError **error);
GArrowRecordBatch *garrow_record_batch_add_column(GArrowRecordBatch *record_batch,
                                                  guint i,
                                                  GArrowField *field,
                                                  GArrowArray *column,
                                                  GError **error);
GArrowRecordBatch *garrow_record_batch_remove_column(GArrowRecordBatch *record_batch,
                                                     guint i,
                                                     GError **error);

G_END_DECLS
