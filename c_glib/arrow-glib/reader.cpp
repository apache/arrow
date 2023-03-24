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

#include <arrow-glib/array.hpp>
#include <arrow-glib/chunked-array.hpp>
#include <arrow-glib/data-type.hpp>
#include <arrow-glib/enums.h>
#include <arrow-glib/error.hpp>
#include <arrow-glib/input-stream.hpp>
#include <arrow-glib/internal-index.hpp>
#include <arrow-glib/metadata-version.hpp>
#include <arrow-glib/reader.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>
#include <arrow-glib/table.hpp>

#include <arrow/c/bridge.h>

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
 *
 * #GArrowCSVReader is a class for reading table in CSV format from
 * input.
 *
 * #GArrowJSONReader is a class for reading table in JSON format from
 * input.
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

#define GARROW_RECORD_BATCH_READER_GET_PRIVATE(obj)         \
  static_cast<GArrowRecordBatchReaderPrivate *>(            \
     garrow_record_batch_reader_get_instance_private(       \
       GARROW_RECORD_BATCH_READER(obj)))

static void
garrow_record_batch_reader_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(object);

  priv->record_batch_reader.~shared_ptr();

  G_OBJECT_CLASS(garrow_record_batch_reader_parent_class)->finalize(object);
}

static void
garrow_record_batch_reader_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(object);

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
  auto priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(object);
  new(&priv->record_batch_reader) std::shared_ptr<arrow::ipc::RecordBatchReader>;
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
 * garrow_record_batch_reader_import:
 * @c_abi_array_stream: (not nullable): A `struct ArrowArrayStream *`.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An imported
 *   #GArrowRecordBatchReader on success, %NULL on error.
 *
 *   You don't need to release the passed `struct ArrowArrayStream *`,
 *   even if this function reports an error.
 *
 * Since: 6.0.0
 */
GArrowRecordBatchReader *
garrow_record_batch_reader_import(gpointer c_abi_array_stream, GError **error)
{
  auto arrow_reader_result =
    arrow::ImportRecordBatchReader(
      static_cast<ArrowArrayStream *>(c_abi_array_stream));
  if (garrow::check(error,
                    arrow_reader_result,
                    "[record-batch-reader][import]")) {
    return garrow_record_batch_reader_new_raw(&(*arrow_reader_result));
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_reader_new:
 * @record_batches: (element-type GArrowRecordBatch):
 *   A list of #GArrowRecordBatch.
 * @schema: (nullable): A #GArrowSchema to confirm to.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The schema in the stream on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GArrowRecordBatchReader *
garrow_record_batch_reader_new(GList *record_batches,
                               GArrowSchema *schema,
                               GError **error)
{
  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_record_batches;
  for (auto node = record_batches; node; node = node->next) {
    auto record_batch = GARROW_RECORD_BATCH(node->data);
    arrow_record_batches.push_back(garrow_record_batch_get_raw(record_batch));
  }
  std::shared_ptr<arrow::Schema> arrow_schema;
  if (schema) {
    arrow_schema = garrow_schema_get_raw(schema);
  }
  auto arrow_reader_result =
    arrow::RecordBatchReader::Make(arrow_record_batches, arrow_schema);
  if (garrow::check(error,
                    arrow_reader_result,
                    "[record-batch-stream-reader][new]")) {
    return garrow_record_batch_reader_new_raw(&*arrow_reader_result);
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_reader_export:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full) (nullable): An exported
 *   #GArrowRecordBatchReader as `struct ArrowArrayStream *` on
 *   success, %NULL on error.
 *
 *   It should be freed with the `ArrowArrayStream::release` callback then
 *   g_free() when no longer needed.
 *
 * Since: 6.0.0
 */
gpointer
garrow_record_batch_reader_export(GArrowRecordBatchReader *reader,
                                  GError **error)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  auto c_abi_array_stream = g_new(ArrowArrayStream, 1);
  auto status = arrow::ExportRecordBatchReader(arrow_reader,
                                               c_abi_array_stream);
  if (garrow::check(error, status, "[record-batch-reader][export]")) {
    return c_abi_array_stream;
  } else {
    g_free(c_abi_array_stream);
    return NULL;
  }
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
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 *
 * Since: 0.4.0
 *
 * Deprecated: 0.5.0:
 *   Use garrow_record_batch_reader_read_next() instead.
 */
GArrowRecordBatch *
garrow_record_batch_reader_get_next_record_batch(GArrowRecordBatchReader *reader,
                                                 GError **error)
{
  return garrow_record_batch_reader_read_next(reader, error);
}

/**
 * garrow_record_batch_reader_read_next_record_batch:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 *
 * Since: 0.5.0
 *
 * Deprecated: 0.8.0:
 *   Use garrow_record_batch_reader_read_next() instead.
 */
GArrowRecordBatch *
garrow_record_batch_reader_read_next_record_batch(GArrowRecordBatchReader *reader,
                                                  GError **error)
{
  return garrow_record_batch_reader_read_next(reader, error);
}

/**
 * garrow_record_batch_reader_read_next:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 *
 * Since: 0.8.0
 */
GArrowRecordBatch *
garrow_record_batch_reader_read_next(GArrowRecordBatchReader *reader,
                                     GError **error)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_reader->ReadNext(&arrow_record_batch);

  if (garrow_error_check(error,
                         status,
                         "[record-batch-reader][read-next]")) {
    if (arrow_record_batch == nullptr) {
      return NULL;
    } else {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  } else {
    return NULL;
  }
}

/**
 * garrow_record_batch_reader_read_all:
 * @reader: A #GArrowRecordBatchReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The all record batches in the stream as #GArrowTable.
 *
 * Since: 6.0.0
 */
GArrowTable *
garrow_record_batch_reader_read_all(GArrowRecordBatchReader *reader,
                                    GError **error)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = arrow_reader->ToTable().Value(&arrow_table);

  if (garrow::check(error,
                    status,
                    "[record-batch-reader][read-all]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowTableBatchReader,
              garrow_table_batch_reader,
              GARROW_TYPE_RECORD_BATCH_READER);

static void
garrow_table_batch_reader_init(GArrowTableBatchReader *object)
{
}

static void
garrow_table_batch_reader_class_init(GArrowTableBatchReaderClass *klass)
{
}

/**
 * garrow_table_batch_reader_new:
 * @table: The table to be read.
 *
 * Returns: A newly created #GArrowTableBatchReader.
 *
 * Since: 0.8.0
 */
GArrowTableBatchReader *
garrow_table_batch_reader_new(GArrowTable *table)
{
  auto arrow_table = garrow_table_get_raw(table);
  auto arrow_table_batch_reader =
    std::make_shared<arrow::TableBatchReader>(*arrow_table);
  return garrow_table_batch_reader_new_raw(&arrow_table_batch_reader);
}

/**
 * garrow_table_batch_reader_set_max_chunk_size:
 * @reader: A #GArrowTableBatchReader.
 * @max_chunk_size: The maximum chunk size of record batches.
 *
 * Set the desired maximum chunk size of record batches.
 *
 * The actual chunk size of each record batch may be smaller,
 * depending on actual chunking characteristics of each table column.
 *
 * Since: 12.0.0
 */
void
garrow_table_batch_reader_set_max_chunk_size(GArrowTableBatchReader *reader,
                                             gint64 max_chunk_size)
{
  auto arrow_reader = garrow_table_batch_reader_get_raw(reader);
  arrow_reader->set_chunksize(max_chunk_size);
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
 * @error: (nullable): Return location for a #GError or %NULL.
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
  using ReaderType = arrow::ipc::RecordBatchStreamReader;

  auto arrow_input_stream = garrow_input_stream_get_raw(stream);
  auto arrow_reader = ReaderType::Open(arrow_input_stream);
  if (garrow::check(error, arrow_reader, "[record-batch-stream-reader][open]")) {
    auto subtype = std::dynamic_pointer_cast<ReaderType>(*arrow_reader);
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

#define GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(obj)        \
  static_cast<GArrowRecordBatchFileReaderPrivate *>(            \
     garrow_record_batch_file_reader_get_instance_private(      \
       GARROW_RECORD_BATCH_FILE_READER(obj)))

static void
garrow_record_batch_file_reader_finalize(GObject *object)
{
  auto priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(object);

  priv->record_batch_file_reader.~shared_ptr();

  G_OBJECT_CLASS(garrow_record_batch_file_reader_parent_class)->finalize(object);
}

static void
garrow_record_batch_file_reader_set_property(GObject *object,
                                             guint prop_id,
                                             const GValue *value,
                                             GParamSpec *pspec)
{
  auto priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(object);

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
  auto priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(object);
  new(&priv->record_batch_file_reader)
    std::shared_ptr<arrow::ipc::RecordBatchFileReader>;
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
 * @error: (nullable): Return location for a #GError or %NULL.
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
  using ReaderType = arrow::ipc::RecordBatchFileReader;

  auto arrow_random_access_file = garrow_seekable_input_stream_get_raw(file);
  auto arrow_reader = ReaderType::Open(arrow_random_access_file);
  if (garrow::check(error, arrow_reader, "[record-batch-file-reader][open]")) {
    auto subtype = std::dynamic_pointer_cast<ReaderType>(*arrow_reader);
    return garrow_record_batch_file_reader_new_raw(&subtype);
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
 * @error: (nullable): Return location for a #GError or %NULL.
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
 * @error: (nullable): Return location for a #GError or %NULL.
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
  auto arrow_record_batch = arrow_reader->ReadRecordBatch(i);

  if (garrow::check(error, arrow_record_batch,
                    "[record-batch-file-reader][read-record-batch]")) {
    return garrow_record_batch_new_raw(&(*arrow_record_batch));
  } else {
    return NULL;
  }
}


typedef struct GArrowFeatherFileReaderPrivate_ {
  std::shared_ptr<arrow::ipc::feather::Reader> feather_reader;
} GArrowFeatherFileReaderPrivate;

enum {
  PROP_FEATHER_READER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowFeatherFileReader,
                           garrow_feather_file_reader,
                           G_TYPE_OBJECT);

#define GARROW_FEATHER_FILE_READER_GET_PRIVATE(obj)             \
  static_cast<GArrowFeatherFileReaderPrivate *>(                \
    garrow_feather_file_reader_get_instance_private(            \
      GARROW_FEATHER_FILE_READER(obj)))

static void
garrow_feather_file_reader_finalize(GObject *object)
{
  auto priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(object);

  priv->feather_reader.~shared_ptr();

  G_OBJECT_CLASS(garrow_feather_file_reader_parent_class)->finalize(object);
}

static void
garrow_feather_file_reader_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  auto priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FEATHER_READER:
    priv->feather_reader =
      *static_cast<std::shared_ptr<arrow::ipc::feather::Reader> *>(g_value_get_pointer(value));
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
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_feather_file_reader_finalize;
  gobject_class->set_property = garrow_feather_file_reader_set_property;
  gobject_class->get_property = garrow_feather_file_reader_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("feather-reader",
                              "arrow::ipc::feather::Reader",
                              "The raw std::shared<arrow::ipc::feather::Reader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FEATHER_READER, spec);
}


/**
 * garrow_feather_file_reader_new:
 * @file: The file to be read.
 * @error: (nullable): Return location for a #GError or %NULL.
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
  auto reader = arrow::ipc::feather::Reader::Open(arrow_random_access_file);
  if (garrow::check(error, reader, "[feather-file-reader][new]")) {
    return garrow_feather_file_reader_new_raw(&(*reader));
  } else {
    return NULL;
  }
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
 * garrow_feather_file_reader_read:
 * @reader: A #GArrowFeatherFileReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The table in the file that has all columns.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_feather_file_reader_read(GArrowFeatherFileReader *reader,
                                GError **error)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = arrow_reader->Read(&arrow_table);
  if (garrow_error_check(error, status, "[feather-file-reader][read]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_feather_file_reader_read_indices:
 * @reader: A #GArrowFeatherFileReader.
 * @indices: (array length=n_indices): The indices of column to be read.
 * @n_indices: The number of indices.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The table in the file that has only the
 *   specified columns.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_feather_file_reader_read_indices(GArrowFeatherFileReader *reader,
                                        const gint *indices,
                                        guint n_indices,
                                        GError **error)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  std::vector<int> cpp_indices(n_indices);
  for (guint i = 0; i < n_indices; ++i) {
    cpp_indices.push_back(indices[i]);
  }
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = arrow_reader->Read(cpp_indices, &arrow_table);
  if (garrow_error_check(error, status, "[feather-file-reader][read-indices]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}

/**
 * garrow_feather_file_reader_read_names:
 * @reader: A #GArrowFeatherFileReader.
 * @names: (array length=n_names): The names of column to be read.
 * @n_names: The number of names.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): The table in the file that has only the
 *   specified columns.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_feather_file_reader_read_names(GArrowFeatherFileReader *reader,
                                      const gchar **names,
                                      guint n_names,
                                      GError **error)
{
  auto arrow_reader = garrow_feather_file_reader_get_raw(reader);
  std::vector<std::string> cpp_names;
  for (guint i = 0; i < n_names; ++i) {
    cpp_names.push_back(names[i]);
  }
  std::shared_ptr<arrow::Table> arrow_table;
  auto status = arrow_reader->Read(cpp_names, &arrow_table);
  if (garrow_error_check(error, status, "[feather-file-reader][read-names]")) {
    return garrow_table_new_raw(&arrow_table);
  } else {
    return NULL;
  }
}


typedef struct GArrowCSVReadOptionsPrivate_ {
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  arrow::csv::ConvertOptions convert_options;
} GArrowCSVReadOptionsPrivate;

enum {
  PROP_USE_THREADS = 1,
  PROP_BLOCK_SIZE,
  PROP_N_SKIP_ROWS,
  PROP_DELIMITER,
  PROP_IS_QUOTED,
  PROP_QUOTE_CHARACTER,
  PROP_IS_DOUBLE_QUOTED,
  PROP_IS_ESCAPED,
  PROP_ESCAPE_CHARACTER,
  PROP_ALLOW_NEWLINES_IN_VALUES,
  PROP_IGNORE_EMPTY_LINES,
  PROP_CHECK_UTF8,
  PROP_ALLOW_NULL_STRINGS,
  PROP_GENERATE_COLUMN_NAMES
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCSVReadOptions,
                           garrow_csv_read_options,
                           G_TYPE_OBJECT)

#define GARROW_CSV_READ_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowCSVReadOptionsPrivate *>(       \
    garrow_csv_read_options_get_instance_private(   \
      GARROW_CSV_READ_OPTIONS(object)))

static void
garrow_csv_read_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_USE_THREADS:
    priv->read_options.use_threads = g_value_get_boolean(value);
    break;
  case PROP_BLOCK_SIZE:
    priv->read_options.block_size = g_value_get_int(value);
    break;
  case PROP_N_SKIP_ROWS:
    priv->read_options.skip_rows = g_value_get_uint(value);
    break;
  case PROP_GENERATE_COLUMN_NAMES:
    priv->read_options.autogenerate_column_names = g_value_get_boolean(value);
    break;
  case PROP_DELIMITER:
    priv->parse_options.delimiter = g_value_get_schar(value);
    break;
  case PROP_IS_QUOTED:
    priv->parse_options.quoting = g_value_get_boolean(value);
    break;
  case PROP_QUOTE_CHARACTER:
    priv->parse_options.quote_char = g_value_get_schar(value);
    break;
  case PROP_IS_DOUBLE_QUOTED:
    priv->parse_options.double_quote = g_value_get_boolean(value);
    break;
  case PROP_IS_ESCAPED:
    priv->parse_options.escaping = g_value_get_boolean(value);
    break;
  case PROP_ESCAPE_CHARACTER:
    priv->parse_options.escape_char = g_value_get_schar(value);
    break;
  case PROP_ALLOW_NEWLINES_IN_VALUES:
    priv->parse_options.newlines_in_values = g_value_get_boolean(value);
    break;
  case PROP_IGNORE_EMPTY_LINES:
    priv->parse_options.ignore_empty_lines = g_value_get_boolean(value);
    break;
  case PROP_CHECK_UTF8:
    priv->convert_options.check_utf8 = g_value_get_boolean(value);
    break;
  case PROP_ALLOW_NULL_STRINGS:
    priv->convert_options.strings_can_be_null = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_csv_read_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_USE_THREADS:
    g_value_set_boolean(value, priv->read_options.use_threads);
    break;
  case PROP_BLOCK_SIZE:
    g_value_set_int(value, priv->read_options.block_size);
    break;
  case PROP_N_SKIP_ROWS:
    g_value_set_uint(value, priv->read_options.skip_rows);
    break;
  case PROP_GENERATE_COLUMN_NAMES:
    g_value_set_boolean(value, priv->read_options.autogenerate_column_names);
    break;
  case PROP_DELIMITER:
    g_value_set_schar(value, priv->parse_options.delimiter);
    break;
  case PROP_IS_QUOTED:
    g_value_set_boolean(value, priv->parse_options.quoting);
    break;
  case PROP_QUOTE_CHARACTER:
    g_value_set_schar(value, priv->parse_options.quote_char);
    break;
  case PROP_IS_DOUBLE_QUOTED:
    g_value_set_boolean(value, priv->parse_options.double_quote);
    break;
  case PROP_IS_ESCAPED:
    g_value_set_boolean(value, priv->parse_options.escaping);
    break;
  case PROP_ESCAPE_CHARACTER:
    g_value_set_schar(value, priv->parse_options.escape_char);
    break;
  case PROP_ALLOW_NEWLINES_IN_VALUES:
    g_value_set_boolean(value, priv->parse_options.newlines_in_values);
    break;
  case PROP_IGNORE_EMPTY_LINES:
    g_value_set_boolean(value, priv->parse_options.ignore_empty_lines);
    break;
  case PROP_CHECK_UTF8:
    g_value_set_boolean(value, priv->convert_options.check_utf8);
    break;
  case PROP_ALLOW_NULL_STRINGS:
    g_value_set_boolean(value, priv->convert_options.strings_can_be_null);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_csv_read_options_init(GArrowCSVReadOptions *object)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(object);
  priv->read_options = arrow::csv::ReadOptions::Defaults();
  priv->parse_options = arrow::csv::ParseOptions::Defaults();
  priv->convert_options = arrow::csv::ConvertOptions::Defaults();
}

static void
garrow_csv_read_options_class_init(GArrowCSVReadOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = garrow_csv_read_options_set_property;
  gobject_class->get_property = garrow_csv_read_options_get_property;

  auto read_options = arrow::csv::ReadOptions::Defaults();

  /**
   * GArrowCSVReadOptions:use-threads:
   *
   * Whether to use the global CPU thread pool.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Whether to use the global CPU thread pool",
                              read_options.use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_USE_THREADS, spec);

  /**
   * GArrowCSVReadOptions:block-size:
   *
   * Block size we request from the IO layer; also determines the size
   * of chunks when #GArrowCSVReadOptions:use-threads is %TRUE.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_int("block-size",
                          "Block size",
                          "Block size we request from the IO layer; "
                          "also determines the size of chunks "
                          "when ::use-threads is TRUE",
                          0,
                          G_MAXINT,
                          read_options.block_size,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_BLOCK_SIZE, spec);

  /**
   * GArrowCSVReadOptions:n-skip-rows:
   *
   * The number of header rows to skip (not including
   * the row of column names, if any)
   *
   * Since: 0.15.0
   */
  spec = g_param_spec_uint("n-skip-rows",
                           "N skip rows",
                           "The number of header rows to skip "
                           "(not including the row of column names, if any)",
                           0,
                           G_MAXUINT,
                           read_options.skip_rows,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_N_SKIP_ROWS, spec);

  /**
   * GArrowCSVReadOptions:generate_column_names:
   *
   * Whether to autogenerate column names if #GArrowCSVReadOptions:column-names is empty.
   * If %TRUE, column names will be of the form 'f0', 'f1'...
   * If %FALSE, column names will be read from the first CSV row
   * after #GArrowCSVReadOptions:n-skip-rows.
   *
   * Since: 0.15.0
   */
  spec = g_param_spec_boolean("generate-column-names",
                              "Generate column names",
                              "Whether to autogenerate column names if column-names is empty. "
                              "If TRUE, column names will be of the form 'f0', 'f1'... "
                              "If FALSE, column names will be read from the first CSV row "
                              "after n-skip-rows",
                              read_options.autogenerate_column_names,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_GENERATE_COLUMN_NAMES, spec);


  auto parse_options = arrow::csv::ParseOptions::Defaults();

  /**
   * GArrowCSVReadOptions:delimiter:
   *
   * Field delimiter character.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_char("delimiter",
                           "Delimiter",
                           "Field delimiter character",
                           0,
                           G_MAXINT8,
                           parse_options.delimiter,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_DELIMITER, spec);

  /**
   * GArrowCSVReadOptions:is-quoted:
   *
   * Whether quoting is used.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("is-quoted",
                              "Is quoted",
                              "Whether quoting is used",
                              parse_options.quoting,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_IS_QUOTED, spec);

  /**
   * GArrowCSVReadOptions:quote-character:
   *
   * Quoting character. This is used only when
   * #GArrowCSVReadOptions:is-quoted is %TRUE.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_char("quote-character",
                           "Quote character",
                           "Quoting character",
                           0,
                           G_MAXINT8,
                           parse_options.quote_char,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_QUOTE_CHARACTER, spec);

  /**
   * GArrowCSVReadOptions:is-double-quoted:
   *
   * Whether a quote inside a value is double quoted.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("is-double-quoted",
                              "Is double quoted",
                              "Whether a quote inside a value is double quoted",
                              parse_options.double_quote,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_IS_DOUBLE_QUOTED, spec);

  /**
   * GArrowCSVReadOptions:is-escaped:
   *
   * Whether escaping is used.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("is-escaped",
                              "Is escaped",
                              "Whether escaping is used",
                              parse_options.escaping,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_IS_ESCAPED, spec);

  /**
   * GArrowCSVReadOptions:escape-character:
   *
   * Escaping character. This is used only when
   * #GArrowCSVReadOptions:is-escaped is %TRUE.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_char("escape-character",
                           "Escape character",
                           "Escaping character",
                           0,
                           G_MAXINT8,
                           parse_options.escape_char,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ESCAPE_CHARACTER, spec);

  /**
   * GArrowCSVReadOptions:allow-newlines-in-values:
   *
   * Whether values are allowed to contain CR (0x0d) and LF (0x0a) characters.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("allow-newlines-in-values",
                              "Allow newlines in values",
                              "Whether values are allowed to contain "
                              "CR (0x0d) and LF (0x0a) characters.",
                              parse_options.newlines_in_values,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_ALLOW_NEWLINES_IN_VALUES,
                                  spec);

  /**
   * GArrowCSVReadOptions:ignore-empty-lines:
   *
   * Whether empty lines are ignored. If %FALSE, an empty line
   * represents a simple empty value (assuming a one-column CSV file).
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("ignore-empty-lines",
                              "Ignore empty lines",
                              "Whether empty lines are ignored. "
                              "If FALSE, an empty line represents "
                              "a simple empty value "
                              "(assuming a one-column CSV file).",
                              parse_options.ignore_empty_lines,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_IGNORE_EMPTY_LINES,
                                  spec);

  auto convert_options = arrow::csv::ConvertOptions::Defaults();

  /**
   * GArrowCSVReadOptions:check-utf8:
   *
   * Whether to check UTF8 validity of string columns.
   *
   * Since: 0.12.0
   */
  spec = g_param_spec_boolean("check-utf8",
                              "Check UTF8",
                              "Whether to check UTF8 validity of string columns",
                              convert_options.check_utf8,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_CHECK_UTF8, spec);

  /**
   * GArrowCSVReadOptions:allow-null-strings:
   *
   * Whether string / binary columns can have null values.
   * If %TRUE, then strings in "null_values" are considered null for string columns.
   * If %FALSE, then all strings are valid string values.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_boolean("allow-null-strings",
                              "Allow null strings",
                              "Whether string / binary columns can have null values. "
                              "If TRUE, then strings in null_values are considered null for string columns. "
                              "If FALSE, then all strings are valid string values.",
                              convert_options.strings_can_be_null,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_ALLOW_NULL_STRINGS, spec);
}

/**
 * garrow_csv_read_options_new:
 *
 * Returns: A newly created #GArrowCSVReadOptions.
 *
 * Since: 0.12.0
 */
GArrowCSVReadOptions *
garrow_csv_read_options_new(void)
{
  auto csv_read_options = g_object_new(GARROW_TYPE_CSV_READ_OPTIONS, NULL);
  return GARROW_CSV_READ_OPTIONS(csv_read_options);
}

/**
 * garrow_csv_read_options_add_column_type:
 * @options: A #GArrowCSVReadOptions.
 * @name: The name of the target column.
 * @data_type: The #GArrowDataType for the column.
 *
 * Add value type of a column.
 *
 * Since: 0.12.0
 */
void
garrow_csv_read_options_add_column_type(GArrowCSVReadOptions *options,
                                        const gchar *name,
                                        GArrowDataType *data_type)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  auto arrow_data_type = garrow_data_type_get_raw(data_type);
  priv->convert_options.column_types[name] = arrow_data_type;
}

/**
 * garrow_csv_read_options_add_schema:
 * @options: A #GArrowCSVReadOptions.
 * @schema: The #GArrowSchema that specifies columns and their types.
 *
 * Add value types for columns in the schema.
 *
 * Since: 0.12.0
 */
void
garrow_csv_read_options_add_schema(GArrowCSVReadOptions *options,
                                   GArrowSchema *schema)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  auto arrow_schema = garrow_schema_get_raw(schema);
  for (const auto &field : arrow_schema->fields()) {
    priv->convert_options.column_types[field->name()] = field->type();
  }
}

/**
 * garrow_csv_read_options_get_column_types:
 * @options: A #GArrowCSVReadOptions.
 *
 * Returns: (transfer full) (element-type gchar* GArrowDataType):
 *   The column name and value type mapping of the options.
 *
 * Since: 0.12.0
 */
GHashTable *
garrow_csv_read_options_get_column_types(GArrowCSVReadOptions *options)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  GHashTable *types = g_hash_table_new_full(g_str_hash,
                                            g_str_equal,
                                            g_free,
                                            g_object_unref);
  for (const auto &iter : priv->convert_options.column_types) {
    auto arrow_name = iter.first;
    auto arrow_data_type = iter.second;
    g_hash_table_insert(types,
                        g_strdup(arrow_name.c_str()),
                        garrow_data_type_new_raw(&arrow_data_type));
  }
  return types;
}

/**
 * garrow_csv_read_options_set_null_values:
 * @options: A #GArrowCSVReadOptions.
 * @null_values: (array length=n_null_values):
 *   The values to be processed as null.
 * @n_null_values: The number of the specified null values.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_set_null_values(GArrowCSVReadOptions *options,
                                        const gchar **null_values,
                                        gsize n_null_values)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.null_values.resize(n_null_values);
  for (gsize i = 0; i < n_null_values; ++i) {
    priv->convert_options.null_values[i] = null_values[i];
  }
}

/**
 * garrow_csv_read_options_get_null_values:
 * @options: A #GArrowCSVReadOptions.
 *
 * Returns: (nullable) (array zero-terminated=1) (element-type utf8) (transfer full):
 *   The values to be processed as null.
 *
 *   If the number of values is zero, this returns %NULL.
 *
 *   It's a %NULL-terminated string array. It must be freed with
 *   g_strfreev() when no longer needed.
 *
 * Since: 0.14.0
 */
gchar **
garrow_csv_read_options_get_null_values(GArrowCSVReadOptions *options)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  const auto &arrow_null_values = priv->convert_options.null_values;
  if (arrow_null_values.empty()) {
    return NULL;
  } else {
    auto n = arrow_null_values.size();
    gchar **null_values = g_new(gchar *, n + 1);
    for (size_t i = 0; i < n; ++i) {
      null_values[i] = g_strdup(arrow_null_values[i].c_str());
    }
    null_values[n] = NULL;
    return null_values;
  }
}

/**
 * garrow_csv_read_options_add_null_value:
 * @options: A #GArrowCSVReadOptions.
 * @null_value: The value to be processed as null.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_add_null_value(GArrowCSVReadOptions *options,
                                       const gchar *null_value)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.null_values.push_back(null_value);
}

/**
 * garrow_csv_read_options_set_true_values:
 * @options: A #GArrowCSVReadOptions.
 * @true_values: (array length=n_true_values):
 *   The values to be processed as true.
 * @n_true_values: The number of the specified true values.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_set_true_values(GArrowCSVReadOptions *options,
                                        const gchar **true_values,
                                        gsize n_true_values)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.true_values.resize(n_true_values);
  for (gsize i = 0; i < n_true_values; ++i) {
    priv->convert_options.true_values[i] = true_values[i];
  }
}

/**
 * garrow_csv_read_options_get_true_values:
 * @options: A #GArrowCSVReadOptions.
 *
 * Returns: (nullable) (array zero-terminated=1) (element-type utf8) (transfer full):
 *   The values to be processed as true.
 *
 *   If the number of values is zero, this returns %NULL.
 *
 *   It's a %NULL-terminated string array. It must be freed with
 *   g_strfreev() when no longer needed.
 *
 * Since: 0.14.0
 */
gchar **
garrow_csv_read_options_get_true_values(GArrowCSVReadOptions *options)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  const auto &arrow_true_values = priv->convert_options.true_values;
  if (arrow_true_values.empty()) {
    return NULL;
  } else {
    auto n = arrow_true_values.size();
    gchar **true_values = g_new(gchar *, n + 1);
    for (size_t i = 0; i < n; ++i) {
      true_values[i] = g_strdup(arrow_true_values[i].c_str());
    }
    true_values[n] = NULL;
    return true_values;
  }
}

/**
 * garrow_csv_read_options_add_true_value:
 * @options: A #GArrowCSVReadOptions.
 * @true_value: The value to be processed as true.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_add_true_value(GArrowCSVReadOptions *options,
                                       const gchar *true_value)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.true_values.push_back(true_value);
}

/**
 * garrow_csv_read_options_set_false_values:
 * @options: A #GArrowCSVReadOptions.
 * @false_values: (array length=n_false_values):
 *   The values to be processed as false.
 * @n_false_values: The number of the specified false values.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_set_false_values(GArrowCSVReadOptions *options,
                                         const gchar **false_values,
                                         gsize n_false_values)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.false_values.resize(n_false_values);
  for (gsize i = 0; i < n_false_values; ++i) {
    priv->convert_options.false_values[i] = false_values[i];
  }
}

/**
 * garrow_csv_read_options_get_false_values:
 * @options: A #GArrowCSVReadOptions.
 *
 * Returns: (nullable) (array zero-terminated=1) (element-type utf8) (transfer full):
 *   The values to be processed as false.
 *
 *   If the number of values is zero, this returns %NULL.
 *
 *   It's a %NULL-terminated string array. It must be freed with
 *   g_strfreev() when no longer needed.
 *
 * Since: 0.14.0
 */
gchar **
garrow_csv_read_options_get_false_values(GArrowCSVReadOptions *options)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  const auto &arrow_false_values = priv->convert_options.false_values;
  if (arrow_false_values.empty()) {
    return NULL;
  } else {
    auto n = arrow_false_values.size();
    gchar **false_values = g_new(gchar *, n + 1);
    for (size_t i = 0; i < n; ++i) {
      false_values[i] = g_strdup(arrow_false_values[i].c_str());
    }
    false_values[n] = NULL;
    return false_values;
  }
}

/**
 * garrow_csv_read_options_add_false_value:
 * @options: A #GArrowCSVReadOptions.
 * @false_value: The value to be processed as false.
 *
 * Since: 0.14.0
 */
void
garrow_csv_read_options_add_false_value(GArrowCSVReadOptions *options,
                                        const gchar *false_value)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->convert_options.false_values.push_back(false_value);
}

/**
 * garrow_csv_read_options_set_column_names:
 * @options: A #GArrowCSVReadOptions.
 * @column_names: (array length=n_column_names):
 *   The column names (if empty, will be read from first
 *   row after `skip_rows`)
 * @n_column_names: The number of the specified column names.
 *
 * Since: 0.15.0
 */
void
garrow_csv_read_options_set_column_names(GArrowCSVReadOptions *options,
                                         const gchar **column_names,
                                         gsize n_column_names)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->read_options.column_names.resize(n_column_names);
  for (gsize i = 0; i < n_column_names; ++i) {
    priv->read_options.column_names[i] = column_names[i];
  }
}

/**
 * garrow_csv_read_options_get_column_names:
 * @options: A #GArrowCSVReadOptions.
 *
 * Returns: (nullable) (array zero-terminated=1) (element-type utf8) (transfer full):
 *   The column names.
 *
 *   If the number of values is zero, this returns %NULL.
 *
 *   It's a %NULL-terminated string array. It must be freed with
 *   g_strfreev() when no longer needed.
 *
 * Since: 0.15.0
 */
gchar **
garrow_csv_read_options_get_column_names(GArrowCSVReadOptions *options)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  const auto &arrow_column_names = priv->read_options.column_names;
  if (arrow_column_names.empty()) {
    return NULL;
  } else {
    auto n = arrow_column_names.size();
    gchar **column_names = g_new(gchar *, n + 1);
    for (size_t i = 0; i < n; ++i) {
      column_names[i] = g_strdup(arrow_column_names[i].c_str());
    }
    column_names[n] = NULL;
    return column_names;
  }
}

/**
 * garrow_csv_read_options_add_column_names:
 * @options: A #GArrowCSVReadOptions.
 * @column_name: The column name to be added.
 *
 * Since: 0.15.0
 */
void
garrow_csv_read_options_add_column_name(GArrowCSVReadOptions *options,
                                        const gchar *column_name)
{
  auto priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
  priv->read_options.column_names.push_back(column_name);
}

typedef struct GArrowCSVReaderPrivate_ {
  std::shared_ptr<arrow::csv::TableReader> reader;
  GArrowInputStream *input;
} GArrowCSVReaderPrivate;

enum {
  PROP_CSV_TABLE_READER = 1,
  PROP_CSV_READER_INPUT,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowCSVReader,
                           garrow_csv_reader,
                           G_TYPE_OBJECT)

#define GARROW_CSV_READER_GET_PRIVATE(object)   \
  static_cast<GArrowCSVReaderPrivate *>(        \
    garrow_csv_reader_get_instance_private(     \
      GARROW_CSV_READER(object)))

static void
garrow_csv_reader_dispose(GObject *object)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(object);

  if (priv->input) {
    g_object_unref(priv->input);
    priv->input = nullptr;
  }

  G_OBJECT_CLASS(garrow_csv_reader_parent_class)->dispose(object);
}

static void
garrow_csv_reader_finalize(GObject *object)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(object);

  priv->reader.~shared_ptr();

  G_OBJECT_CLASS(garrow_csv_reader_parent_class)->finalize(object);
}

static void
garrow_csv_reader_set_property(GObject *object,
                               guint prop_id,
                               const GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CSV_TABLE_READER:
    priv->reader =
      *static_cast<std::shared_ptr<arrow::csv::TableReader> *>(g_value_get_pointer(value));
    break;
  case PROP_CSV_READER_INPUT:
    priv->input = GARROW_INPUT_STREAM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_csv_reader_get_property(GObject *object,
                               guint prop_id,
                               GValue *value,
                               GParamSpec *pspec)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CSV_READER_INPUT:
    g_value_set_object(value, priv->input);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_csv_reader_init(GArrowCSVReader *object)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(object);
  new(&priv->reader) std::shared_ptr<arrow::csv::TableReader>;
}

static void
garrow_csv_reader_class_init(GArrowCSVReaderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_csv_reader_dispose;
  gobject_class->finalize     = garrow_csv_reader_finalize;
  gobject_class->set_property = garrow_csv_reader_set_property;
  gobject_class->get_property = garrow_csv_reader_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("csv-table-reader",
                              "CSV table reader",
                              "The raw std::shared<arrow::csv::TableReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CSV_TABLE_READER, spec);

  spec = g_param_spec_object("input",
                             "Input",
                             "The input stream to be read",
                             GARROW_TYPE_INPUT_STREAM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class,
                                  PROP_CSV_READER_INPUT,
                                  spec);
}

/**
 * garrow_csv_reader_new:
 * @input: The input to be read.
 * @options: (nullable): A #GArrowCSVReadOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowCSVReader or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowCSVReader *
garrow_csv_reader_new(GArrowInputStream *input,
                      GArrowCSVReadOptions *options,
                      GError **error)
{
  auto arrow_input = garrow_input_stream_get_raw(input);
  arrow::csv::ReadOptions read_options;
  arrow::csv::ParseOptions parse_options;
  arrow::csv::ConvertOptions convert_options;
  if (options) {
    auto options_priv = GARROW_CSV_READ_OPTIONS_GET_PRIVATE(options);
    read_options = options_priv->read_options;
    parse_options = options_priv->parse_options;
    convert_options = options_priv->convert_options;
  } else {
    read_options = arrow::csv::ReadOptions::Defaults();
    parse_options = arrow::csv::ParseOptions::Defaults();
    convert_options = arrow::csv::ConvertOptions::Defaults();
  }

  auto arrow_reader =
    arrow::csv::TableReader::Make(arrow::io::default_io_context(),
                                  arrow_input,
                                  read_options,
                                  parse_options,
                                  convert_options);
  if (garrow::check(error, arrow_reader, "[csv-reader][new]")) {
    return garrow_csv_reader_new_raw(&(*arrow_reader), input);
  } else {
    return NULL;
  }
}

/**
 * garrow_csv_reader_read:
 * @reader: A #GArrowCSVReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A read #GArrowTable or %NULL on error.
 *
 * Since: 0.12.0
 */
GArrowTable *
garrow_csv_reader_read(GArrowCSVReader *reader,
                       GError **error)
{
  auto arrow_reader = garrow_csv_reader_get_raw(reader);
  auto arrow_table = arrow_reader->Read();
  if (garrow::check(error, arrow_table, "[csv-reader][read]")) {
    return garrow_table_new_raw(&(arrow_table.ValueOrDie()));
  } else {
    return NULL;
  }
}


typedef struct GArrowJSONReadOptionsPrivate_ {
  arrow::json::ReadOptions read_options;
  arrow::json::ParseOptions parse_options;
  GArrowSchema *schema;
} GArrowJSONReadOptionsPrivate;

enum {
  PROP_JSON_READ_OPTIONS_USE_THREADS = 1,
  PROP_JSON_READ_OPTIONS_BLOCK_SIZE,
  PROP_JSON_READ_OPTIONS_ALLOW_NEWLINES_IN_VALUES,
  PROP_JSON_READ_OPTIONS_UNEXPECTED_FIELD_BEHAVIOR,
  PROP_JSON_READ_OPTIONS_SCHEMA,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowJSONReadOptions,
                           garrow_json_read_options,
                           G_TYPE_OBJECT)

#define GARROW_JSON_READ_OPTIONS_GET_PRIVATE(object) \
  static_cast<GArrowJSONReadOptionsPrivate *>(       \
    garrow_json_read_options_get_instance_private(   \
      GARROW_JSON_READ_OPTIONS(object)))

static void
garrow_json_read_options_dispose(GObject *object)
{
  auto priv = GARROW_JSON_READ_OPTIONS_GET_PRIVATE(object);

  if (priv->schema) {
    g_object_unref(priv->schema);
    priv->schema = nullptr;
  }

  G_OBJECT_CLASS(garrow_json_read_options_parent_class)->dispose(object);
}

static void
garrow_json_read_options_set_property(GObject *object,
                                      guint prop_id,
                                      const GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_JSON_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_JSON_READ_OPTIONS_USE_THREADS:
    priv->read_options.use_threads = g_value_get_boolean(value);
    break;
  case PROP_JSON_READ_OPTIONS_BLOCK_SIZE:
    priv->read_options.block_size = g_value_get_int(value);
    break;
  case PROP_JSON_READ_OPTIONS_ALLOW_NEWLINES_IN_VALUES:
    priv->parse_options.newlines_in_values = g_value_get_boolean(value);
    break;
  case PROP_JSON_READ_OPTIONS_UNEXPECTED_FIELD_BEHAVIOR:
    priv->parse_options.unexpected_field_behavior =
      static_cast<arrow::json::UnexpectedFieldBehavior>(g_value_get_enum(value));
    break;
  case PROP_JSON_READ_OPTIONS_SCHEMA:
    {
      auto schema = g_value_dup_object(value);
      if (priv->schema) {
        g_object_unref(priv->schema);
      }
      if (schema) {
        priv->schema = GARROW_SCHEMA(schema);
        priv->parse_options.explicit_schema = garrow_schema_get_raw(priv->schema);
      } else {
        priv->schema = NULL;
        priv->parse_options.explicit_schema = nullptr;
      }
      break;
    }
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_json_read_options_get_property(GObject *object,
                                      guint prop_id,
                                      GValue *value,
                                      GParamSpec *pspec)
{
  auto priv = GARROW_JSON_READ_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_JSON_READ_OPTIONS_USE_THREADS:
    g_value_set_boolean(value, priv->read_options.use_threads);
    break;
  case PROP_JSON_READ_OPTIONS_BLOCK_SIZE:
    g_value_set_int(value, priv->read_options.block_size);
    break;
  case PROP_JSON_READ_OPTIONS_ALLOW_NEWLINES_IN_VALUES:
    g_value_set_boolean(value, priv->parse_options.newlines_in_values);
    break;
  case PROP_JSON_READ_OPTIONS_UNEXPECTED_FIELD_BEHAVIOR:
    g_value_set_enum(value, static_cast<int>(priv->parse_options.unexpected_field_behavior));
    break;
  case PROP_JSON_READ_OPTIONS_SCHEMA:
    g_value_set_object(value, priv->schema);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_json_read_options_init(GArrowJSONReadOptions *object)
{
  auto priv = GARROW_JSON_READ_OPTIONS_GET_PRIVATE(object);
  priv->read_options = arrow::json::ReadOptions::Defaults();
  priv->parse_options = arrow::json::ParseOptions::Defaults();
}

static void
garrow_json_read_options_class_init(GArrowJSONReadOptionsClass *klass)
{
  GParamSpec *spec;

  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_json_read_options_dispose;
  gobject_class->set_property = garrow_json_read_options_set_property;
  gobject_class->get_property = garrow_json_read_options_get_property;

  auto read_options = arrow::json::ReadOptions::Defaults();

  /**
   * GArrowJSONReadOptions:use-threads:
   *
   * Whether to use the global CPU thread pool.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_boolean("use-threads",
                              "Use threads",
                              "Whether to use the global CPU thread pool",
                              read_options.use_threads,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READ_OPTIONS_USE_THREADS,
                                  spec);

  /**
   * GArrowJSONReadOptions:block-size:
   *
   * Block size we request from the IO layer; also determines the size
   * of chunks when #GArrowJSONReadOptions:use-threads is %TRUE.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_int("block-size",
                          "Block size",
                          "Block size we request from the IO layer; "
                          "also determines the size of chunks "
                          "when ::use-threads is TRUE",
                          0,
                          G_MAXINT,
                          read_options.block_size,
                          static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READ_OPTIONS_BLOCK_SIZE,
                                  spec);


  auto parse_options = arrow::json::ParseOptions::Defaults();

  /**
   * GArrowJSONReadOptions:allow-newlines-in-values:
   *
   * Whether objects may be printed across multiple lines (for example pretty printed).
   * if %FALSE, input must end with an empty line.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_boolean("allow-newlines-in-values",
                              "Allow newlines in values",
                              "Whether objects may be printed across multiple lines "
                              "(for example pretty printed). "
                              "if FALSE, input must end with an empty line.",
                              parse_options.newlines_in_values,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READ_OPTIONS_ALLOW_NEWLINES_IN_VALUES,
                                  spec);

  /**
   * GArrowJSONReadOptions:unexpected-field-behavior:
   *
   * How to parse handle fields outside the explicit schema.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_enum("unexpected-field-behavior",
                           "UnexpectedFieldBehavior",
                           "How to parse handle fields outside the explicit schema.",
                           GARROW_TYPE_JSON_READ_UNEXPECTED_FIELD_BEHAVIOR,
                           GARROW_JSON_READ_INFER_TYPE,
                           static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READ_OPTIONS_UNEXPECTED_FIELD_BEHAVIOR,
                                  spec);

  /**
   * GArrowJSONReadOptions:schema:
   *
   * Schema for passing custom conversion rules.
   *
   * Since: 0.14.0
   */
  spec = g_param_spec_object("schema",
                             "Schema",
                             "Schema for passing custom conversion rules.",
                              GARROW_TYPE_SCHEMA,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READ_OPTIONS_SCHEMA,
                                  spec);
}

/**
 * garrow_json_read_options_new:
 *
 * Returns: A newly created #GArrowJSONReadOptions.
 *
 * Since: 0.14.0
 */
GArrowJSONReadOptions *
garrow_json_read_options_new(void)
{
  auto json_read_options = g_object_new(GARROW_TYPE_JSON_READ_OPTIONS, NULL);
  return GARROW_JSON_READ_OPTIONS(json_read_options);
}


typedef struct GArrowJSONReaderPrivate_ {
  std::shared_ptr<arrow::json::TableReader> reader;
  GArrowInputStream *input;
} GArrowJSONReaderPrivate;

enum {
  PROP_JSON_TABLE_READER = 1,
  PROP_JSON_READER_INPUT,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowJSONReader,
                           garrow_json_reader,
                           G_TYPE_OBJECT)

#define GARROW_JSON_READER_GET_PRIVATE(object)   \
  static_cast<GArrowJSONReaderPrivate *>(        \
    garrow_json_reader_get_instance_private(     \
      GARROW_JSON_READER(object)))

static void
garrow_json_reader_dispose(GObject *object)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(object);

  if (priv->input) {
    g_object_unref(priv->input);
    priv->input = nullptr;
  }

  G_OBJECT_CLASS(garrow_json_reader_parent_class)->dispose(object);
}

static void
garrow_json_reader_finalize(GObject *object)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(object);

  priv->reader.~shared_ptr();

  G_OBJECT_CLASS(garrow_json_reader_parent_class)->finalize(object);
}

static void
garrow_json_reader_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_JSON_TABLE_READER:
    priv->reader =
      *static_cast<std::shared_ptr<arrow::json::TableReader> *>(g_value_get_pointer(value));
    break;
  case PROP_JSON_READER_INPUT:
    priv->input = GARROW_INPUT_STREAM(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_json_reader_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_JSON_READER_INPUT:
    g_value_set_object(value, priv->input);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_json_reader_init(GArrowJSONReader *object)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(object);
  new(&priv->reader) std::shared_ptr<arrow::json::TableReader>;
}

static void
garrow_json_reader_class_init(GArrowJSONReaderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose      = garrow_json_reader_dispose;
  gobject_class->finalize     = garrow_json_reader_finalize;
  gobject_class->set_property = garrow_json_reader_set_property;
  gobject_class->get_property = garrow_json_reader_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("json-table-reader",
                              "JSON table reader",
                              "The raw std::shared<arrow::json::TableReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_JSON_TABLE_READER, spec);

  spec = g_param_spec_object("input",
                             "Input",
                             "The input stream to be read",
                             GARROW_TYPE_INPUT_STREAM,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class,
                                  PROP_JSON_READER_INPUT,
                                  spec);
}

/**
 * garrow_json_reader_new:
 * @input: The input to be read.
 * @options: (nullable): A #GArrowJSONReadOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowJSONReader or %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowJSONReader *
garrow_json_reader_new(GArrowInputStream *input,
                       GArrowJSONReadOptions *options,
                       GError **error)
{
  auto arrow_input = garrow_input_stream_get_raw(input);
  arrow::Status status;

  arrow::Result<std::shared_ptr<arrow::json::TableReader>> arrow_reader;
  if (options) {
    auto options_priv = GARROW_JSON_READ_OPTIONS_GET_PRIVATE(options);
    arrow_reader = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                                  arrow_input,
                                                  options_priv->read_options,
                                                  options_priv->parse_options);
  } else {
    arrow_reader = arrow::json::TableReader::Make(arrow::default_memory_pool(),
                                                  arrow_input,
                                                  arrow::json::ReadOptions::Defaults(),
                                                  arrow::json::ParseOptions::Defaults());
  }

  if (garrow::check(error, arrow_reader, "[json-reader][new]")) {
    return garrow_json_reader_new_raw(&*arrow_reader, input);
  } else {
    return NULL;
  }
}

/**
 * garrow_json_reader_read:
 * @reader: A #GArrowJSONReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A read #GArrowTable or %NULL on error.
 *
 * Since: 0.14.0
 */
GArrowTable *
garrow_json_reader_read(GArrowJSONReader *reader,
                        GError **error)
{
  auto arrow_reader = garrow_json_reader_get_raw(reader);
  auto arrow_table = arrow_reader->Read();
  if (garrow::check(error, arrow_table, "[json-reader][read]")) {
    return garrow_table_new_raw(&(arrow_table.ValueOrDie()));
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowRecordBatchReader *
garrow_record_batch_reader_new_raw(
  std::shared_ptr<arrow::RecordBatchReader> *arrow_reader)
{
  return GARROW_RECORD_BATCH_READER(
    g_object_new(GARROW_TYPE_RECORD_BATCH_READER,
                 "record-batch-reader", arrow_reader,
                 NULL));
}

std::shared_ptr<arrow::ipc::RecordBatchReader>
garrow_record_batch_reader_get_raw(GArrowRecordBatchReader *reader)
{
  auto priv = GARROW_RECORD_BATCH_READER_GET_PRIVATE(reader);
  return priv->record_batch_reader;
}

GArrowTableBatchReader *
garrow_table_batch_reader_new_raw(std::shared_ptr<arrow::TableBatchReader> *arrow_reader)
{
  auto reader =
    GARROW_TABLE_BATCH_READER(g_object_new(GARROW_TYPE_TABLE_BATCH_READER,
                                           "record-batch-reader", arrow_reader,
                                           NULL));
  return reader;
}

std::shared_ptr<arrow::TableBatchReader>
garrow_table_batch_reader_get_raw(GArrowTableBatchReader *reader)
{
  return std::static_pointer_cast<arrow::TableBatchReader>(
    garrow_record_batch_reader_get_raw(GARROW_RECORD_BATCH_READER(reader)));
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
  auto priv = GARROW_RECORD_BATCH_FILE_READER_GET_PRIVATE(reader);
  return priv->record_batch_file_reader;
}

GArrowFeatherFileReader *
garrow_feather_file_reader_new_raw(std::shared_ptr<arrow::ipc::feather::Reader> *arrow_reader)
{
  auto reader =
    GARROW_FEATHER_FILE_READER(
      g_object_new(GARROW_TYPE_FEATHER_FILE_READER,
                   "feather-reader", arrow_reader,
                   NULL));
  return reader;
}

std::shared_ptr<arrow::ipc::feather::Reader>
garrow_feather_file_reader_get_raw(GArrowFeatherFileReader *reader)
{
  auto priv = GARROW_FEATHER_FILE_READER_GET_PRIVATE(reader);
  return priv->feather_reader;
}

GArrowCSVReader *
garrow_csv_reader_new_raw(std::shared_ptr<arrow::csv::TableReader> *arrow_reader,
                          GArrowInputStream *input)
{
  auto reader = GARROW_CSV_READER(g_object_new(GARROW_TYPE_CSV_READER,
                                               "csv-table-reader", arrow_reader,
                                               "input", input,
                                               NULL));
  return reader;
}

std::shared_ptr<arrow::csv::TableReader>
garrow_csv_reader_get_raw(GArrowCSVReader *reader)
{
  auto priv = GARROW_CSV_READER_GET_PRIVATE(reader);
  return priv->reader;
}

GArrowJSONReader *
garrow_json_reader_new_raw(std::shared_ptr<arrow::json::TableReader> *arrow_reader,
                           GArrowInputStream *input)
{
  auto reader = GARROW_JSON_READER(g_object_new(GARROW_TYPE_JSON_READER,
                                                "json-table-reader", arrow_reader,
                                                "input", input,
                                                NULL));
  return reader;
}

std::shared_ptr<arrow::json::TableReader>
garrow_json_reader_get_raw(GArrowJSONReader *reader)
{
  auto priv = GARROW_JSON_READER_GET_PRIVATE(reader);
  return priv->reader;
}
