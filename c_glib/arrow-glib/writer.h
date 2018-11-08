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
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>

#include <arrow-glib/output-stream.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH_WRITER         \
  (garrow_record_batch_writer_get_type())
#define GARROW_RECORD_BATCH_WRITER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_RECORD_BATCH_WRITER,  \
                              GArrowRecordBatchWriter))
#define GARROW_RECORD_BATCH_WRITER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_RECORD_BATCH_WRITER,     \
                           GArrowRecordBatchWriterClass))
#define GARROW_IS_RECORD_BATCH_WRITER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_RECORD_BATCH_WRITER))
#define GARROW_IS_RECORD_BATCH_WRITER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_RECORD_BATCH_WRITER))
#define GARROW_RECORD_BATCH_WRITER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_RECORD_BATCH_WRITER,   \
                             GArrowRecordBatchWriterClass))

typedef struct _GArrowRecordBatchWriter         GArrowRecordBatchWriter;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowRecordBatchWriterClass    GArrowRecordBatchWriterClass;
#endif

/**
 * GArrowRecordBatchWriter:
 *
 * It wraps `arrow::ipc::RecordBatchWriter`.
 */
struct _GArrowRecordBatchWriter
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowRecordBatchWriterClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_record_batch_writer_get_type(void) G_GNUC_CONST;

gboolean garrow_record_batch_writer_write_record_batch(
  GArrowRecordBatchWriter *writer,
  GArrowRecordBatch *record_batch,
  GError **error);
gboolean garrow_record_batch_writer_write_table(
  GArrowRecordBatchWriter *writer,
  GArrowTable *table,
  GError **error);
gboolean garrow_record_batch_writer_close(
  GArrowRecordBatchWriter *writer,
  GError **error);


#define GARROW_TYPE_RECORD_BATCH_STREAM_WRITER          \
  (garrow_record_batch_stream_writer_get_type())
#define GARROW_RECORD_BATCH_STREAM_WRITER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_STREAM_WRITER,   \
                              GArrowRecordBatchStreamWriter))
#define GARROW_RECORD_BATCH_STREAM_WRITER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_STREAM_WRITER,      \
                           GArrowRecordBatchStreamWriterClass))
#define GARROW_IS_RECORD_BATCH_STREAM_WRITER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_STREAM_WRITER))
#define GARROW_IS_RECORD_BATCH_STREAM_WRITER_CLASS(klass)            \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                     \
                           GARROW_TYPE_RECORD_BATCH_STREAM_WRITER))
#define GARROW_RECORD_BATCH_STREAM_WRITER_GET_CLASS(obj)             \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                     \
                             GARROW_TYPE_RECORD_BATCH_STREAM_WRITER, \
                             GArrowRecordBatchStreamWriterClass))

typedef struct _GArrowRecordBatchStreamWriter      GArrowRecordBatchStreamWriter;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowRecordBatchStreamWriterClass GArrowRecordBatchStreamWriterClass;
#endif

/**
 * GArrowRecordBatchStreamWriter:
 *
 * It wraps `arrow::ipc::RecordBatchStreamWriter`.
 */
struct _GArrowRecordBatchStreamWriter
{
  /*< private >*/
  GArrowRecordBatchWriter parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowRecordBatchStreamWriterClass
{
  GArrowRecordBatchWriterClass parent_class;
};
#endif

GType garrow_record_batch_stream_writer_get_type(void) G_GNUC_CONST;

GArrowRecordBatchStreamWriter *garrow_record_batch_stream_writer_new(
  GArrowOutputStream *sink,
  GArrowSchema *schema,
  GError **error);


#define GARROW_TYPE_RECORD_BATCH_FILE_WRITER    \
  (garrow_record_batch_file_writer_get_type())
#define GARROW_RECORD_BATCH_FILE_WRITER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_FILE_WRITER,     \
                              GArrowRecordBatchFileWriter))
#define GARROW_RECORD_BATCH_FILE_WRITER_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_FILE_WRITER,        \
                           GArrowRecordBatchFileWriterClass))
#define GARROW_IS_RECORD_BATCH_FILE_WRITER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_FILE_WRITER))
#define GARROW_IS_RECORD_BATCH_FILE_WRITER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_FILE_WRITER))
#define GARROW_RECORD_BATCH_FILE_WRITER_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                                     \
                             GARROW_TYPE_RECORD_BATCH_FILE_WRITER,      \
                             GArrowRecordBatchFileWriterClass))

typedef struct _GArrowRecordBatchFileWriter      GArrowRecordBatchFileWriter;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowRecordBatchFileWriterClass GArrowRecordBatchFileWriterClass;
#endif

/**
 * GArrowRecordBatchFileWriter:
 *
 * It wraps `arrow::ipc::RecordBatchFileWriter`.
 */
struct _GArrowRecordBatchFileWriter
{
  /*< private >*/
  GArrowRecordBatchStreamWriter parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowRecordBatchFileWriterClass
{
  GArrowRecordBatchStreamWriterClass parent_class;
};
#endif

GType garrow_record_batch_file_writer_get_type(void) G_GNUC_CONST;

GArrowRecordBatchFileWriter *garrow_record_batch_file_writer_new(
  GArrowOutputStream *sink,
  GArrowSchema *schema,
  GError **error);


#define GARROW_TYPE_FEATHER_FILE_WRITER (garrow_feather_file_writer_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowFeatherFileWriter,
                         garrow_feather_file_writer,
                         GARROW,
                         FEATHER_FILE_WRITER,
                         GObject)
struct _GArrowFeatherFileWriterClass
{
  GObjectClass parent_class;
};

GArrowFeatherFileWriter *garrow_feather_file_writer_new(GArrowOutputStream *sink,
                                                        GError **error);
void garrow_feather_file_writer_set_description(GArrowFeatherFileWriter *writer,
                                                const gchar *description);
void garrow_feather_file_writer_set_n_rows(GArrowFeatherFileWriter *writer,
                                           gint64 n_rows);
gboolean garrow_feather_file_writer_append(GArrowFeatherFileWriter *writer,
                                           const gchar *name,
                                           GArrowArray *array,
                                           GError **error);
gboolean garrow_feather_file_writer_write(GArrowFeatherFileWriter *writer,
                                          GArrowTable *table,
                                          GError **error);
gboolean garrow_feather_file_writer_close(GArrowFeatherFileWriter *writer,
                                          GError **error);

G_END_DECLS
