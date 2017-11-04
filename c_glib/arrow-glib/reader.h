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

#include <arrow-glib/gobject-type.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>
#include <arrow-glib/table.h>

#include <arrow-glib/input-stream.h>

#include <arrow-glib/metadata-version.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH_READER (garrow_record_batch_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchReader,
                         garrow_record_batch_reader,
                         GARROW,
                         RECORD_BATCH_READER,
                         GObject)
struct _GArrowRecordBatchReaderClass
{
  GObjectClass parent_class;
};

GArrowSchema *garrow_record_batch_reader_get_schema(
  GArrowRecordBatchReader *reader);
#ifndef GARROW_DISABLE_DEPRECATED
G_GNUC_DEPRECATED_FOR(garrow_record_batch_reader_read_next)
GArrowRecordBatch *garrow_record_batch_reader_get_next_record_batch(
  GArrowRecordBatchReader *reader,
  GError **error);
#endif
#ifndef GARROW_DISABLE_DEPRECATED
G_GNUC_DEPRECATED_FOR(garrow_record_batch_reader_read_next)
GArrowRecordBatch *garrow_record_batch_reader_read_next_record_batch(
  GArrowRecordBatchReader *reader,
  GError **error);
#endif
GArrowRecordBatch *garrow_record_batch_reader_read_next(
  GArrowRecordBatchReader *reader,
  GError **error);


#define GARROW_TYPE_TABLE_BATCH_READER (garrow_table_batch_reader_get_type())
G_DECLARE_DERIVABLE_TYPE(GArrowTableBatchReader,
                         garrow_table_batch_reader,
                         GARROW,
                         TABLE_BATCH_READER,
                         GArrowRecordBatchReader)
struct _GArrowTableBatchReaderClass
{
  GArrowRecordBatchReaderClass parent_class;
};

GArrowTableBatchReader *garrow_table_batch_reader_new(GArrowTable *table);


#define GARROW_TYPE_RECORD_BATCH_STREAM_READER          \
  (garrow_record_batch_stream_reader_get_type())
#define GARROW_RECORD_BATCH_STREAM_READER(obj)                          \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_STREAM_READER,   \
                              GArrowRecordBatchStreamReader))
#define GARROW_RECORD_BATCH_STREAM_READER_CLASS(klass)                  \
  (G_TYPE_CHECK_CLASS_CAST((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_STREAM_READER,      \
                           GArrowRecordBatchStreamReaderClass))
#define GARROW_IS_RECORD_BATCH_STREAM_READER(obj)                       \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_STREAM_READER))
#define GARROW_IS_RECORD_BATCH_STREAM_READER_CLASS(klass)               \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_STREAM_READER))
#define GARROW_RECORD_BATCH_STREAM_READER_GET_CLASS(obj)                \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                                     \
                             GARROW_TYPE_RECORD_BATCH_STREAM_READER,    \
                             GArrowRecordBatchStreamReaderClass))

typedef struct _GArrowRecordBatchStreamReader      GArrowRecordBatchStreamReader;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowRecordBatchStreamReaderClass GArrowRecordBatchStreamReaderClass;
#endif

/**
 * GArrowRecordBatchStreamReader:
 *
 * It wraps `arrow::ipc::RecordBatchStreamReader`.
 */
struct _GArrowRecordBatchStreamReader
{
  /*< private >*/
  GArrowRecordBatchReader parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowRecordBatchStreamReaderClass
{
  GArrowRecordBatchReaderClass parent_class;
};
#endif

GType garrow_record_batch_stream_reader_get_type(void) G_GNUC_CONST;

GArrowRecordBatchStreamReader *garrow_record_batch_stream_reader_new(
  GArrowInputStream *stream,
  GError **error);


#define GARROW_TYPE_RECORD_BATCH_FILE_READER    \
  (garrow_record_batch_file_reader_get_type())
#define GARROW_RECORD_BATCH_FILE_READER(obj)                            \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_FILE_READER,     \
                              GArrowRecordBatchFileReader))
#define GARROW_RECORD_BATCH_FILE_READER_CLASS(klass)                    \
  (G_TYPE_CHECK_CLASS_CAST((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_FILE_READER,        \
                           GArrowRecordBatchFileReaderClass))
#define GARROW_IS_RECORD_BATCH_FILE_READER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                                    \
                              GARROW_TYPE_RECORD_BATCH_FILE_READER))
#define GARROW_IS_RECORD_BATCH_FILE_READER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                                     \
                           GARROW_TYPE_RECORD_BATCH_FILE_READER))
#define GARROW_RECORD_BATCH_FILE_READER_GET_CLASS(obj)                  \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                                     \
                             GARROW_TYPE_RECORD_BATCH_FILE_READER,      \
                             GArrowRecordBatchFileReaderClass))

typedef struct _GArrowRecordBatchFileReader      GArrowRecordBatchFileReader;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowRecordBatchFileReaderClass GArrowRecordBatchFileReaderClass;
#endif

/**
 * GArrowRecordBatchFileReader:
 *
 * It wraps `arrow::ipc::RecordBatchFileReader`.
 */
struct _GArrowRecordBatchFileReader
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowRecordBatchFileReaderClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_record_batch_file_reader_get_type(void) G_GNUC_CONST;

GArrowRecordBatchFileReader *garrow_record_batch_file_reader_new(
  GArrowSeekableInputStream *file,
  GError **error);

GArrowSchema *garrow_record_batch_file_reader_get_schema(
  GArrowRecordBatchFileReader *reader);
guint garrow_record_batch_file_reader_get_n_record_batches(
  GArrowRecordBatchFileReader *reader);
GArrowMetadataVersion garrow_record_batch_file_reader_get_version(
  GArrowRecordBatchFileReader *reader);
#ifndef GARROW_DISABLE_DEPRECATED
G_GNUC_DEPRECATED_FOR(garrow_record_batch_file_reader_read_record_batch)
GArrowRecordBatch *garrow_record_batch_file_reader_get_record_batch(
  GArrowRecordBatchFileReader *reader,
  guint i,
  GError **error);
#endif
GArrowRecordBatch *garrow_record_batch_file_reader_read_record_batch(
  GArrowRecordBatchFileReader *reader,
  guint i,
  GError **error);


#define GARROW_TYPE_FEATHER_FILE_READER         \
  (garrow_feather_file_reader_get_type())
#define GARROW_FEATHER_FILE_READER(obj)                         \
  (G_TYPE_CHECK_INSTANCE_CAST((obj),                            \
                              GARROW_TYPE_FEATHER_FILE_READER,  \
                              GArrowFeatherFileReader))
#define GARROW_FEATHER_FILE_READER_CLASS(klass)                 \
  (G_TYPE_CHECK_CLASS_CAST((klass),                             \
                           GARROW_TYPE_FEATHER_FILE_READER,     \
                           GArrowFeatherFileReaderClass))
#define GARROW_IS_FEATHER_FILE_READER(obj)                      \
  (G_TYPE_CHECK_INSTANCE_TYPE((obj),                            \
                              GARROW_TYPE_FEATHER_FILE_READER))
#define GARROW_IS_FEATHER_FILE_READER_CLASS(klass)              \
  (G_TYPE_CHECK_CLASS_TYPE((klass),                             \
                           GARROW_TYPE_FEATHER_FILE_READER))
#define GARROW_FEATHER_FILE_READER_GET_CLASS(obj)               \
  (G_TYPE_INSTANCE_GET_CLASS((obj),                             \
                             GARROW_TYPE_FEATHER_FILE_READER,   \
                             GArrowFeatherFileReaderClass))

typedef struct _GArrowFeatherFileReader      GArrowFeatherFileReader;
#ifndef __GTK_DOC_IGNORE__
typedef struct _GArrowFeatherFileReaderClass GArrowFeatherFileReaderClass;
#endif

/**
 * GArrowFeatherFileReader:
 *
 * It wraps `arrow::ipc::feather::TableReader`.
 */
struct _GArrowFeatherFileReader
{
  /*< private >*/
  GObject parent_instance;
};

#ifndef __GTK_DOC_IGNORE__
struct _GArrowFeatherFileReaderClass
{
  GObjectClass parent_class;
};
#endif

GType garrow_feather_file_reader_get_type(void) G_GNUC_CONST;

GArrowFeatherFileReader *garrow_feather_file_reader_new(
  GArrowSeekableInputStream *file,
  GError **error);

gchar *garrow_feather_file_reader_get_description(
  GArrowFeatherFileReader *reader);
gboolean garrow_feather_file_reader_has_description(
  GArrowFeatherFileReader *reader);
gint garrow_feather_file_reader_get_version(
  GArrowFeatherFileReader *reader);
gint64 garrow_feather_file_reader_get_n_rows(
  GArrowFeatherFileReader *reader);
gint64 garrow_feather_file_reader_get_n_columns(
  GArrowFeatherFileReader *reader);
gchar *garrow_feather_file_reader_get_column_name(
  GArrowFeatherFileReader *reader,
  gint i);
GArrowColumn *garrow_feather_file_reader_get_column(
  GArrowFeatherFileReader *reader,
  gint i,
  GError **error);
GList *garrow_feather_file_reader_get_columns(
  GArrowFeatherFileReader *reader,
  GError **error);

G_END_DECLS
