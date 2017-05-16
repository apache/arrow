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

#include <arrow/ipc/api.h>

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
 */
GArrowRecordBatch *
garrow_record_batch_reader_get_next_record_batch(GArrowRecordBatchReader *reader,
                                                 GError **error)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_reader->GetNextRecordBatch(&arrow_record_batch);

  if (garrow_error_check(error,
                         status,
                         "[record-batch-reader][get-next-record-batch]")) {
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
  auto arrow_input_stream = garrow_input_stream_get_raw(stream);
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> arrow_reader;
  auto status =
    arrow::ipc::RecordBatchStreamReader::Open(arrow_input_stream, &arrow_reader);
  if (garrow_error_check(error, status, "[record-batch-stream-reader][open]")) {
    return garrow_record_batch_stream_reader_new_raw(&arrow_reader);
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
 */
GArrowRecordBatch *
garrow_record_batch_file_reader_get_record_batch(GArrowRecordBatchFileReader *reader,
                                                 guint i,
                                                 GError **error)
{
  auto arrow_reader = garrow_record_batch_file_reader_get_raw(reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_reader->GetRecordBatch(i, &arrow_record_batch);

  if (garrow_error_check(error,
                         status,
                         "[record-batch-file-reader][get-record-batch]")) {
    return garrow_record_batch_new_raw(&arrow_record_batch);
  } else {
    return NULL;
  }
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
