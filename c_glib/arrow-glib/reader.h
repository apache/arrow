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

#include <arrow-glib/input-stream.h>
#include <arrow-glib/metadata-version.h>
#include <arrow-glib/record-batch.h>
#include <arrow-glib/schema.h>
#include <arrow-glib/table.h>
#include <arrow-glib/timestamp-parser.h>

G_BEGIN_DECLS

#define GARROW_TYPE_RECORD_BATCH_READER (garrow_record_batch_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchReader,
                         garrow_record_batch_reader,
                         GARROW,
                         RECORD_BATCH_READER,
                         GObject)
struct _GArrowRecordBatchReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_6_0
GArrowRecordBatchReader *
garrow_record_batch_reader_import(gpointer c_abi_array_stream, GError **error);

GARROW_AVAILABLE_IN_6_0
GArrowRecordBatchReader *
garrow_record_batch_reader_new(GList *record_batches,
                               GArrowSchema *schema,
                               GError **error);

GARROW_AVAILABLE_IN_6_0
gpointer
garrow_record_batch_reader_export(GArrowRecordBatchReader *reader, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowSchema *
garrow_record_batch_reader_get_schema(GArrowRecordBatchReader *reader);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
G_GNUC_DEPRECATED_FOR(garrow_record_batch_reader_read_next)
GArrowRecordBatch *
garrow_record_batch_reader_get_next_record_batch(GArrowRecordBatchReader *reader,
                                                 GError **error);
#endif
#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
G_GNUC_DEPRECATED_FOR(garrow_record_batch_reader_read_next)
GArrowRecordBatch *
garrow_record_batch_reader_read_next_record_batch(GArrowRecordBatchReader *reader,
                                                  GError **error);
#endif

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_reader_read_next(GArrowRecordBatchReader *reader, GError **error);

GARROW_AVAILABLE_IN_6_0
GArrowTable *
garrow_record_batch_reader_read_all(GArrowRecordBatchReader *reader, GError **error);

GARROW_AVAILABLE_IN_13_0
GList *
garrow_record_batch_reader_get_sources(GArrowRecordBatchReader *reader);

#define GARROW_TYPE_TABLE_BATCH_READER (garrow_table_batch_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowTableBatchReader,
                         garrow_table_batch_reader,
                         GARROW,
                         TABLE_BATCH_READER,
                         GArrowRecordBatchReader)
struct _GArrowTableBatchReaderClass
{
  GArrowRecordBatchReaderClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowTableBatchReader *
garrow_table_batch_reader_new(GArrowTable *table);

GARROW_AVAILABLE_IN_12_0
void
garrow_table_batch_reader_set_max_chunk_size(GArrowTableBatchReader *reader,
                                             gint64 max_chunk_size);

#define GARROW_TYPE_RECORD_BATCH_STREAM_READER                                           \
  (garrow_record_batch_stream_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchStreamReader,
                         garrow_record_batch_stream_reader,
                         GARROW,
                         RECORD_BATCH_STREAM_READER,
                         GArrowRecordBatchReader)
struct _GArrowRecordBatchStreamReaderClass
{
  GArrowRecordBatchReaderClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatchStreamReader *
garrow_record_batch_stream_reader_new(GArrowInputStream *stream, GError **error);

#define GARROW_TYPE_RECORD_BATCH_FILE_READER (garrow_record_batch_file_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowRecordBatchFileReader,
                         garrow_record_batch_file_reader,
                         GARROW,
                         RECORD_BATCH_FILE_READER,
                         GObject)
struct _GArrowRecordBatchFileReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatchFileReader *
garrow_record_batch_file_reader_new(GArrowSeekableInputStream *file, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowSchema *
garrow_record_batch_file_reader_get_schema(GArrowRecordBatchFileReader *reader);

GARROW_AVAILABLE_IN_ALL
guint
garrow_record_batch_file_reader_get_n_record_batches(GArrowRecordBatchFileReader *reader);

GARROW_AVAILABLE_IN_ALL
GArrowMetadataVersion
garrow_record_batch_file_reader_get_version(GArrowRecordBatchFileReader *reader);

#ifndef GARROW_DISABLE_DEPRECATED
GARROW_AVAILABLE_IN_ALL
G_GNUC_DEPRECATED_FOR(garrow_record_batch_file_reader_read_record_batch)
GArrowRecordBatch *
garrow_record_batch_file_reader_get_record_batch(GArrowRecordBatchFileReader *reader,
                                                 guint i,
                                                 GError **error);
#endif

GARROW_AVAILABLE_IN_ALL
GArrowRecordBatch *
garrow_record_batch_file_reader_read_record_batch(GArrowRecordBatchFileReader *reader,
                                                  guint i,
                                                  GError **error);

#define GARROW_TYPE_FEATHER_FILE_READER (garrow_feather_file_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowFeatherFileReader,
                         garrow_feather_file_reader,
                         GARROW,
                         FEATHER_FILE_READER,
                         GObject)
struct _GArrowFeatherFileReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowFeatherFileReader *
garrow_feather_file_reader_new(GArrowSeekableInputStream *file, GError **error);

GARROW_AVAILABLE_IN_ALL
gint
garrow_feather_file_reader_get_version(GArrowFeatherFileReader *reader);

GARROW_AVAILABLE_IN_ALL
GArrowTable *
garrow_feather_file_reader_read(GArrowFeatherFileReader *reader, GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowTable *
garrow_feather_file_reader_read_indices(GArrowFeatherFileReader *reader,
                                        const gint *indices,
                                        guint n_indices,
                                        GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowTable *
garrow_feather_file_reader_read_names(GArrowFeatherFileReader *reader,
                                      const gchar **names,
                                      guint n_names,
                                      GError **error);

#define GARROW_TYPE_CSV_READ_OPTIONS (garrow_csv_read_options_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(
  GArrowCSVReadOptions, garrow_csv_read_options, GARROW, CSV_READ_OPTIONS, GObject)
struct _GArrowCSVReadOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowCSVReadOptions *
garrow_csv_read_options_new(void);

GARROW_AVAILABLE_IN_ALL
void
garrow_csv_read_options_add_column_type(GArrowCSVReadOptions *options,
                                        const gchar *name,
                                        GArrowDataType *data_type);
GARROW_AVAILABLE_IN_ALL
void
garrow_csv_read_options_add_schema(GArrowCSVReadOptions *options, GArrowSchema *schema);

GARROW_AVAILABLE_IN_ALL
GHashTable *
garrow_csv_read_options_get_column_types(GArrowCSVReadOptions *options);

GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_set_null_values(GArrowCSVReadOptions *options,
                                        const gchar **null_values,
                                        gsize n_null_values);
GARROW_AVAILABLE_IN_0_14
gchar **
garrow_csv_read_options_get_null_values(GArrowCSVReadOptions *options);
GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_add_null_value(GArrowCSVReadOptions *options,
                                       const gchar *null_value);
GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_set_true_values(GArrowCSVReadOptions *options,
                                        const gchar **true_values,
                                        gsize n_true_values);
GARROW_AVAILABLE_IN_0_14
gchar **
garrow_csv_read_options_get_true_values(GArrowCSVReadOptions *options);
GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_add_true_value(GArrowCSVReadOptions *options,
                                       const gchar *true_value);
GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_set_false_values(GArrowCSVReadOptions *options,
                                         const gchar **false_values,
                                         gsize n_false_values);
GARROW_AVAILABLE_IN_0_14
gchar **
garrow_csv_read_options_get_false_values(GArrowCSVReadOptions *options);
GARROW_AVAILABLE_IN_0_14
void
garrow_csv_read_options_add_false_value(GArrowCSVReadOptions *options,
                                        const gchar *false_value);
GARROW_AVAILABLE_IN_0_15
void
garrow_csv_read_options_set_column_names(GArrowCSVReadOptions *options,
                                         const gchar **column_names,
                                         gsize n_column_names);
GARROW_AVAILABLE_IN_0_15
gchar **
garrow_csv_read_options_get_column_names(GArrowCSVReadOptions *options);
GARROW_AVAILABLE_IN_0_15
void
garrow_csv_read_options_add_column_name(GArrowCSVReadOptions *options,
                                        const gchar *column_name);
GARROW_AVAILABLE_IN_16_0
void
garrow_csv_read_options_set_timestamp_parsers(GArrowCSVReadOptions *options,
                                              GList *parsers);
GARROW_AVAILABLE_IN_16_0
GList *
garrow_csv_read_options_get_timestamp_parsers(GArrowCSVReadOptions *options);
GARROW_AVAILABLE_IN_16_0
void
garrow_csv_read_options_add_timestamp_parser(GArrowCSVReadOptions *options,
                                             GArrowTimestampParser *parser);

#define GARROW_TYPE_CSV_READER (garrow_csv_reader_get_type())
GARROW_AVAILABLE_IN_ALL
G_DECLARE_DERIVABLE_TYPE(GArrowCSVReader, garrow_csv_reader, GARROW, CSV_READER, GObject)
struct _GArrowCSVReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_ALL
GArrowCSVReader *
garrow_csv_reader_new(GArrowInputStream *input,
                      GArrowCSVReadOptions *options,
                      GError **error);

GARROW_AVAILABLE_IN_ALL
GArrowTable *
garrow_csv_reader_read(GArrowCSVReader *reader, GError **error);

/**
 * GArrowJSONReadUnexpectedFieldBehavior:
 * @GARROW_JSON_READ_IGNORE: Ignore other fields.
 * @GARROW_JSON_READ_ERROR: Return error.
 * @GARROW_JSON_READ_INFER_TYPE: Infer a type.
 *
 * They are corresponding to `arrow::json::UnexpectedFieldBehavior` values.
 */
typedef enum {
  GARROW_JSON_READ_IGNORE,
  GARROW_JSON_READ_ERROR,
  GARROW_JSON_READ_INFER_TYPE,
} GArrowJSONReadUnexpectedFieldBehavior;

#define GARROW_TYPE_JSON_READ_OPTIONS (garrow_json_read_options_get_type())
GARROW_AVAILABLE_IN_0_14
G_DECLARE_DERIVABLE_TYPE(
  GArrowJSONReadOptions, garrow_json_read_options, GARROW, JSON_READ_OPTIONS, GObject)
struct _GArrowJSONReadOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_14
GArrowJSONReadOptions *
garrow_json_read_options_new(void);

#define GARROW_TYPE_JSON_READER (garrow_json_reader_get_type())
GARROW_AVAILABLE_IN_0_14
G_DECLARE_DERIVABLE_TYPE(
  GArrowJSONReader, garrow_json_reader, GARROW, JSON_READER, GObject)
struct _GArrowJSONReaderClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_0_14
GArrowJSONReader *
garrow_json_reader_new(GArrowInputStream *input,
                       GArrowJSONReadOptions *options,
                       GError **error);
GARROW_AVAILABLE_IN_0_14
GArrowTable *
garrow_json_reader_read(GArrowJSONReader *reader, GError **error);

G_END_DECLS
