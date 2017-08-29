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

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <arrow-glib/column.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-glib/input-stream.hpp>

#include <arrow-glib/metadata-version.hpp>
#include <arrow-glib/reader.hpp>

G_BEGIN_DECLS

/**
 * SECTION: reader
 * @section_id: reader-classes
 * @title: Reader classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowRecordBatchReader is a base class for reading record batches
 * in stream format from input.
 *
 * #GArrowRecordBatchStreamReader is a class for reading record
 * batches in stream format from input synchronously.
 *
 * #GArrowRecordBatchFileReader is a class for reading record
 * batches in file format from input.
 *
 * #GArrowFeatherFileReader is a class for reading columns in Feather
 * file format from input.
 */

typedef struct GArrowRecordBatchReaderPrivate_ {
  std::shared_ptr<arrow::ipc::RecordBatchReader> record_batch_reader;
} GArrowRecordBatchReaderPrivate;

enum {
  PROP_0,
  PROP_RECORD_BATCH_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchReader,
                           garrow_record_batch_reader,
                           G_TYPE_OBJECT);

#define GARROW_RECORD_BATCH_READER_GET_PRIVATE(obj)             \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_RECORD_BATCH_READER, \
                               GArrowRecordBatchReaderPrivate))

static void
garrow_record_batch_reader_finalize(GObject *object)
{
  GArrowRecordBatchReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(object);

  priv->record_batch_reader = nullptr;

  G_OBJECT_CLASS(garrow_record_batch_reader_parent_class)->finalize(object);
}

static void
garrow_record_batch_reader_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  GArrowRecordBatchReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH_READER:
    priv->record_batch_reader =
      *static_cast<std::shared_ptr<arrow::ipc::RecordBatchReader> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_reader_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_reader_init(GArrowRecordBatchReader *object)
{
}

static void
garrow_record_batch_reader_class_init(GArrowRecordBatchReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_reader_finalize;
  gobject_class->set_property = garrow_record_batch_reader_set_property;
  gobject_class->get_property = garrow_record_batch_reader_get_property;

  spec = g_param_spec_pointer("record-batch-reader",
                              "arrow::ipc::RecordBatchReader",
                              "The raw std::shared<arrow::ipc::RecordBatchRecordBatchReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH_READER, spec);
}

/**
 * garrow_record_batch_reader_get_schema:
 * @reader: A #GArrowRecordBatchReader.
 *
 * Returns: (transfer full): The schema in the stream.
 *
 * Since: 0.4.0
 */
GArrowSchema *
garrow_record_batch_reader_get_schema(GArrowRecordBatchReader *reader)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  auto arrow_schema = arrow_reader->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_record_batch_reader_get_next_record_batch:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 *
 * Since: 0.4.0
 *
 * Deprecated: 0.5.0:
 *   Use garrow_record_batch_reader_read_next_record_batch() instead.
 */
GArrowRecordBatch *
garrow_record_batch_reader_get_next_record_batch(GArrowRecordBatchReader *reader,
                                                 GError **error)
{
  return garrow_record_batch_reader_read_next_record_batch(reader, error);
}

/**
 * garrow_record_batch_reader_read_next_record_batch:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 *
 * Since: 0.5.0
 */
GArrowRecordBatch *
garrow_record_batch_reader_read_next_record_batch(GArrowRecordBatchReader *reader,
                                                  GError **error)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_reader->ReadNextRecordBatch(&arrow_record_batch);

  if (garrow_error_check(error,
                         status,
                         "[record-batch-reader][read-next-record-batch]")) {
    if (arrow_record_batch == nullptr) {
      return NULL;
    } else {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowRecordBatchStreamReader,
              garrow_record_batch_stream_reader,
              GARROW_TYPE_RECORD_BATCH_READER);

static void
garrow_record_batch_stream_reader_init(GArrowRecordBatchStreamReader *object)
{
}

static void
garrow_record_batch_stream_reader_class_init(GArrowRecordBatchStreamReaderClass *klass)
{
}

/**
 * garrow_record_batch_stream_reader_new:
 * @stream: The stream to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatchStreamReader
 *   or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowRecordBatchStreamReader *
garrow_record_batch_stream_reader_new(GArrowInputStream *stream,
                                      GError **error)
{
  using BaseType = arrow::ipc::RecordBatchReader;
  using ReaderType = arrow::ipc::RecordBatchStreamReader;

  auto arrow_input_stream = garrow_input_stream_get_raw(stream);
  std::shared_ptr<BaseType> arrow_reader;
  auto status = ReaderType::Open(arrow_input_stream, &arrow_reader);
  if (garrow_error_check(error, status, "[record-batch-stream-reader][open]")) {
    auto subtype = std::dynamic_pointer_cast<ReaderType>(arrow_reader);
    return garrow_record_batch_stream_reader_new_raw(&subtype);
  } else {
    return NULL;
  }
}


typedef struct GArrowRecordBatchFileReaderPrivate_ {
  std::shared_ptr<arrow::ipc::RecordBatchFileReader> record_batch_file_reader;
} GArrowRecordBatchFileReaderPrivate;

enum {
  PROP_0_,
  PROP_RECORD_BATCH_FILE_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchFileReader,
                           garrow_record_batch_file_reader,
                           G_TYPE_OBJECT);

#define GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(obj)                \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                                   \
                               GARROW_TYPE_RECORD_BATCH_FILE_READER,    \
                               GArrowRecordBatchFileReaderPrivate))

static void
garrow_record_batch_file_reader_finalize(GObject *object)
{
  GArrowRecordBatchFileReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(object);

  priv->record_batch_file_reader = nullptr;

  G_OBJECT_CLASS(garrow_record_batch_file_reader_parent_class)->finalize(object);
}

static void
garrow_record_batch_file_reader_set_property(GObject *object,
                                             guint prop_id,
                                             const GValue *value,
                                             GParamSpec *pspec)
{
  GArrowRecordBatchFileReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH_FILE_READER:
    priv->record_batch_file_reader =
      *static_cast<std::shared_ptr<arrow::ipc::RecordBatchFileReader> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_file_reader_get_property(GObject *object,
                                             guint prop_id,
                                             GValue *value,
                                             GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_file_reader_init(GArrowRecordBatchFileReader *object)
{
}

static void
garrow_record_batch_file_reader_class_init(GArrowRecordBatchFileReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_file_reader_finalize;
  gobject_class->set_property = garrow_record_batch_file_reader_set_property;
  gobject_class->get_property = garrow_record_batch_file_reader_get_property;

  spec = g_param_spec_pointer("record-batch-file-reader",
                              "arrow::ipc::RecordBatchFileReader",
                              "The raw std::shared<arrow::ipc::RecordBatchFileReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH_FILE_READER, spec);
}


/**
 * garrow_record_batch_file_reader_new:
 * @file: The file to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatchFileReader
 *   or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowRecordBatchFileReader *
garrow_record_batch_file_reader_new(GArrowSeekableInputStream *file,
                                    GError **error)
{
  auto arrow_random_access_file = garrow_seekable_input_stream_get_raw(file);

  std::shared_ptr<arrow::ipc::RecordBatchFileReader> arrow_reader;
  auto status =
    arrow::ipc::RecordBatchFileReader::Open(arrow_random_access_file,
                                            &arrow_reader);
  if (garrow_error_check(error, status, "[record-batch-file-reader][open]")) {
    return garrow_record_batch_file_reader_new_raw(&arrow_reader);
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_file_reader_get_schema:
 * @reader: A #GArrowRecordBatchFileReader.
 *
 * Returns: (transfer full): The schema in the file.
 *
 * Since: 0.4.0
 */
GArrowSchema *
garrow_record_batch_file_reader_get_schema(GArrowRecordBatchFileReader *reader)
{
  auto arrow_reader = garrow_record_batch_file_reader_get_raw(reader);
  auto arrow_schema = arrow_reader->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_record_batch_file_reader_get_n_record_batches:
 * @reader: A #GArrowRecordBatchFileReader.
 *
 * Returns: The number of record batches in the file.
 *
 * Since: 0.4.0
 */
guint
garrow_record_batch_file_reader_get_n_record_batches(GArrowRecordBatchFileReader *reader)
{
  auto arrow_reader = garrow_record_batch_file_reader_get_raw(reader);
  return arrow_reader->num_record_batches();
}

/**
 * garrow_record_batch_file_reader_get_version:
 * @reader: A #GArrowRecordBatchFileReader.
 *
 * Returns: The format version in the file.
 *
 * Since: 0.4.0
 */
GArrowMetadataVersion
garrow_record_batch_file_reader_get_version(GArrowRecordBatchFileReader *reader)
{
  auto arrow_reader = garrow_record_batch_file_reader_get_raw(reader);
  auto arrow_version = arrow_reader->version();
  return garrow_metadata_version_from_raw(arrow_version);
}

/**
 * garrow_record_batch_file_reader_get_record_batch:
 * @reader: A #GArrowRecordBatchFileReader.
 * @i: The index of the target record batch.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The i-th record batch in the file or %NULL on error.
 *
 * Since: 0.4.0
 *
 * Deprecated: 0.5.0:
 *   Use garrow_record_batch_file_reader_read_record_batch() instead.
 */
GArrowRecordBatch *
garrow_record_batch_file_reader_get_record_batch(GArrowRecordBatchFileReader *reader,
                                                 guint i,
                                                 GError **error)
{
  return garrow_record_batch_file_reader_read_record_batch(reader, i, error);
}

/**
 * garrow_record_batch_file_reader_read_record_batch:
 * @reader: A #GArrowRecordBatchFileReader.
 * @i: The index of the target record batch.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The i-th record batch in the file or %NULL on error.
 *
 * Since: 0.5.0
 */
GArrowRecordBatch *
garrow_record_batch_file_reader_read_record_batch(GArrowRecordBatchFileReader *reader,
                                                  guint i,
                                                  GError **error)
{
  auto arrow_reader = garrow_record_batch_file_reader_get_raw(reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_reader->ReadRecordBatch(i, &arrow_record_batch);

  if (garrow_error_check(error,
                         status,
                         "[record-batch-file-reader][read-record-batch]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
}


typedef struct GArrowFeatherFileReaderPrivate_ {
  arrow::ipc::feather::TableReader *feather_table_reader;
} GArrowFeatherFileReaderPrivate;

enum {
  PROP_0__,
  PROP_FEATHER_TABLE_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFeatherFileReader,
                           garrow_feather_file_reader,
                           G_TYPE_OBJECT);

#define GARROW_FEATHER_FILE_READER_GET_PRIVATE(obj)             \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_FEATHER_FILE_READER, \
                               GArrowFeatherFileReaderPrivate))

static void
garrow_feather_file_reader_finalize(GObject *object)
{
  GArrowFeatherFileReaderPrivate *priv;

  priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(object);

  delete priv->feather_table_reader;

  G_OBJECT_CLASS(garrow_feather_file_reader_parent_class)->finalize(object);
}

static void
garrow_feather_file_reader_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  GArrowFeatherFileReaderPrivate *priv;

  priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FEATHER_TABLE_READER:
    priv->feather_table_reader =
      static_cast<arrow::ipc::feather::TableReader *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_feather_file_reader_get_property(GObject *object,
                                        guint prop_id,
                                        GValue *value,
                                        GParamSpec *pspec)
{
  switch (prop_id) {
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_feather_file_reader_init(GArrowFeatherFileReader *object)
{
}

static void
garrow_feather_file_reader_class_init(GArrowFeatherFileReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_feather_file_reader_finalize;
  gobject_class->set_property = garrow_feather_file_reader_set_property;
  gobject_class->get_property = garrow_feather_file_reader_get_property;

  spec = g_param_spec_pointer("feather-table-reader",
                              "arrow::ipc::feather::TableReader",
                              "The raw std::shared<arrow::ipc::feather::TableReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FEATHER_TABLE_READER, spec);
}


/**
 * garrow_feather_file_reader_new:
 * @file: The file to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowFeatherFileReader
 *   or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowFeatherFileReader *
garrow_feather_file_reader_new(GArrowSeekableInputStream *file,
                               GError **error)
{
  auto arrow_random_access_file = garrow_seekable_input_stream_get_raw(file);
  std::unique_ptr<arrow::ipc::feather::TableReader> arrow_reader;
  auto status =
    arrow::ipc::feather::TableReader::Open(arrow_random_access_file,
                                           &arrow_reader);
  if (garrow_error_check(error, status, "[feather-file-reader][new]")) {
    return garrow_feather_file_reader_new_raw(arrow_reader.release());
  } else {
    return NULL;
  }
}

/**
 * garrow_feather_file_reader_get_description:
 * @reader: A #GArrowFeatherFileReader.
 *
 * Returns: (nullable): The description of the file if it exists,
 *   %NULL otherwise. You can confirm whether description exists or not by
 *   garrow_feather_file_reader_has_description().
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_feather_file_reader_get_description(GArrowFeatherFileReader *reader)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  if (arrow_reader->HasDescription()) {
    auto description = arrow_reader->GetDescription();
    return g_strndup(description.data(),
                     description.size());
  } else {
    return NULL;
  }
}

/**
 * garrow_feather_file_reader_has_description:
 * @reader: A #GArrowFeatherFileReader.
 *
 * Returns: Whether the file has description or not.
 *
 * Since: 0.4.0
 */
gboolean
garrow_feather_file_reader_has_description(GArrowFeatherFileReader *reader)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  return arrow_reader->HasDescription();
}

/**
 * garrow_feather_file_reader_get_version:
 * @reader: A #GArrowFeatherFileReader.
 *
 * Returns: The format version of the file.
 *
 * Since: 0.4.0
 */
gint
garrow_feather_file_reader_get_version(GArrowFeatherFileReader *reader)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  return arrow_reader->version();
}

/**
 * garrow_feather_file_reader_get_n_rows:
 * @reader: A #GArrowFeatherFileReader.
 *
 * Returns: The number of rows in the file.
 *
 * Since: 0.4.0
 */
gint64
garrow_feather_file_reader_get_n_rows(GArrowFeatherFileReader *reader)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  return arrow_reader->num_rows();
}

/**
 * garrow_feather_file_reader_get_n_columns:
 * @reader: A #GArrowFeatherFileReader.
 *
 * Returns: The number of columns in the file.
 *
 * Since: 0.4.0
 */
gint64
garrow_feather_file_reader_get_n_columns(GArrowFeatherFileReader *reader)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  return arrow_reader->num_columns();
}

/**
 * garrow_feather_file_reader_get_column_name:
 * @reader: A #GArrowFeatherFileReader.
 * @i: The index of the target column.
 *
 * Returns: The i-th column name in the file.
 *   It should be freed with g_free() when no longer needed.
 *
 * Since: 0.4.0
 */
gchar *
garrow_feather_file_reader_get_column_name(GArrowFeatherFileReader *reader,
                                           gint i)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  auto column_name = arrow_reader->GetColumnName(i);
  return g_strndup(column_name.data(),
                   column_name.size());
}

/**
 * garrow_feather_file_reader_get_column:
 * @reader: A #GArrowFeatherFileReader.
 * @i: The index of the target column.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The i-th column in the file or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowColumn *
garrow_feather_file_reader_get_column(GArrowFeatherFileReader *reader,
                                      gint i,
                                      GError **error)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  std::shared_ptr<arrow::Column> arrow_column;
  auto status = arrow_reader->GetColumn(i, &arrow_column);

  if (garrow_error_check(error, status, "[feather-file-reader][get-column]")) {
    return garrow_column_new_raw(&arrow_column);
  } else {
    return NULL;
  }
}

/**
 * garrow_feather_file_reader_get_columns:
 * @reader: A #GArrowFeatherFileReader.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (element-type GArrowColumn) (transfer full):
 *   The columns in the file.
 *
 * Since: 0.4.0
 */
GList *
garrow_feather_file_reader_get_columns(GArrowFeatherFileReader *reader,
                                       GError **error)
{
  GList *columns = NULL;
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  auto n_columns = arrow_reader->num_columns();
  for (gint i = 0; i < n_columns; ++i) {
    std::shared_ptr<arrow::Column> arrow_column;
    auto status = arrow_reader->GetColumn(i, &arrow_column);
    if (!garrow_error_check(error,
                            status,
                            "[feather-file-reader][get-columns]")) {
      g_list_foreach(columns, (GFunc)g_object_unref, NULL);
      g_list_free(columns);
      return NULL;
    }
    columns = g_list_prepend(columns,
                             garrow_column_new_raw(&arrow_column));
  }
  return g_list_reverse(columns);
}


G_END_DECLS

GArrowRecordBatchReader *
garrow_record_batch_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchReader> *arrow_reader)
{
  auto reader =
    GARROW_RECORD_BATCH_READER(g_object_new(GARROW_TYPE_RECORD_BATCH_READER,
                                            "record-batch-reader", arrow_reader,
                                            NULL));
  return reader;
}

std::shared_ptr<arrow::ipc::RecordBatchReader>
garrow_record_batch_reader_get_raw(GArrowRecordBatchReader *reader)
{
  GArrowRecordBatchReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(reader);
  return priv->record_batch_reader;
}

GArrowRecordBatchStreamReader *
garrow_record_batch_stream_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchStreamReader> *arrow_reader)
{
  auto reader =
    GARROW_RECORD_BATCH_STREAM_READER(
      g_object_new(GARROW_TYPE_RECORD_BATCH_STREAM_READER,
                   "record-batch-reader", arrow_reader,
                   NULL));
  return reader;
}

GArrowRecordBatchFileReader *
garrow_record_batch_file_reader_new_raw(std::shared_ptr<arrow::ipc::RecordBatchFileReader> *arrow_reader)
{
  auto reader =
    GARROW_RECORD_BATCH_FILE_READER(
      g_object_new(GARROW_TYPE_RECORD_BATCH_FILE_READER,
                   "record-batch-file-reader", arrow_reader,
                   NULL));
  return reader;
}

std::shared_ptr<arrow::ipc::RecordBatchFileReader>
garrow_record_batch_file_reader_get_raw(GArrowRecordBatchFileReader *reader)
{
  GArrowRecordBatchFileReaderPrivate *priv;

  priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(reader);
  return priv->record_batch_file_reader;
}

GArrowFeatherFileReader *
garrow_feather_file_reader_new_raw(arrow::ipc::feather::TableReader *arrow_reader)
{
  auto reader =
    GARROW_FEATHER_FILE_READER(
      g_object_new(GARROW_TYPE_FEATHER_FILE_READER,
                   "feather-table-reader", arrow_reader,
                   NULL));
  return reader;
}

arrow::ipc::feather::TableReader *
garrow_feather_file_reader_get_raw(GArrowFeatherFileReader *reader)
{
  GArrowFeatherFileReaderPrivate *priv;

  priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(reader);
  return priv->feather_table_reader;
}
