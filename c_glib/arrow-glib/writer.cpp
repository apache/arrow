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

#include <arrow-glib/array.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

#include <arrow-glib/output-stream.hpp>

#include <arrow-glib/writer.hpp>

G_BEGIN_DECLS

/**
 * SECTION: writer
 * @section_id: writer-classes
 * @title: Writer classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowRecordBatchWriter is a base class for writing record batches
 * in stream format into output.
 *
 * #GArrowRecordBatchStreamWriter is a base class for writing record
 * batches in stream format into output synchronously.
 *
 * #GArrowRecordBatchFileWriter is a class for writing record
 * batches in file format into output.
 */

typedef struct GArrowRecordBatchWriterPrivate_ {
  std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer;
} GArrowRecordBatchWriterPrivate;

enum {
  PROP_0,
  PROP_RECORD_BATCH_WRITER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowRecordBatchWriter,
                           garrow_record_batch_writer,
                           G_TYPE_OBJECT);

#define GARROW_RECORD_BATCH_WRITER_GET_PRIVATE(obj)             \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_RECORD_BATCH_WRITER, \
                               GArrowRecordBatchWriterPrivate))

static void
garrow_record_batch_writer_finalize(GObject *object)
{
  GArrowRecordBatchWriterPrivate *priv;

  priv = GARROW_RECORD_BATCH_WRITER_GET_PRIVATE(object);

  priv->record_batch_writer = nullptr;

  G_OBJECT_CLASS(garrow_record_batch_writer_parent_class)->finalize(object);
}

static void
garrow_record_batch_writer_set_property(GObject *object,
                                        guint prop_id,
                                        const GValue *value,
                                        GParamSpec *pspec)
{
  GArrowRecordBatchWriterPrivate *priv;

  priv = GARROW_RECORD_BATCH_WRITER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_RECORD_BATCH_WRITER:
    priv->record_batch_writer =
      *static_cast<std::shared_ptr<arrow::ipc::RecordBatchWriter> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_record_batch_writer_get_property(GObject *object,
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
garrow_record_batch_writer_init(GArrowRecordBatchWriter *object)
{
}

static void
garrow_record_batch_writer_class_init(GArrowRecordBatchWriterClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_record_batch_writer_finalize;
  gobject_class->set_property = garrow_record_batch_writer_set_property;
  gobject_class->get_property = garrow_record_batch_writer_get_property;

  spec = g_param_spec_pointer("record-batch-writer",
                              "arrow::ipc::RecordBatchWriter",
                              "The raw std::shared<arrow::ipc::RecordBatchWriter> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_RECORD_BATCH_WRITER, spec);
}

/**
 * garrow_record_batch_writer_write_record_batch:
 * @writer: A #GArrowRecordBatchWriter.
 * @record_batch: The record batch to be written.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.4.0
 */
gboolean
garrow_record_batch_writer_write_record_batch(GArrowRecordBatchWriter *writer,
                                              GArrowRecordBatch *record_batch,
                                              GError **error)
{
  auto arrow_writer = garrow_record_batch_writer_get_raw(writer);
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  auto arrow_record_batch_raw = arrow_record_batch.get();

  auto status = arrow_writer->WriteRecordBatch(*arrow_record_batch_raw);
  return garrow_error_check(error,
                            status,
                            "[record-batch-writer][write-record-batch]");
}

/**
 * garrow_record_batch_writer_close:
 * @writer: A #GArrowRecordBatchWriter.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 0.4.0
 */
gboolean
garrow_record_batch_writer_close(GArrowRecordBatchWriter *writer,
                                 GError **error)
{
  auto arrow_writer = garrow_record_batch_writer_get_raw(writer);

  auto status = arrow_writer->Close();
  return garrow_error_check(error, status, "[record-batch-writer][close]");
}


G_DEFINE_TYPE(GArrowRecordBatchStreamWriter,
              garrow_record_batch_stream_writer,
              GARROW_TYPE_RECORD_BATCH_WRITER);

static void
garrow_record_batch_stream_writer_init(GArrowRecordBatchStreamWriter *object)
{
}

static void
garrow_record_batch_stream_writer_class_init(GArrowRecordBatchStreamWriterClass *klass)
{
}

/**
 * garrow_record_batch_stream_writer_new:
 * @sink: The output of the writer.
 * @schema: The schema of the writer.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatchStreamWriter
 *   or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowRecordBatchStreamWriter *
garrow_record_batch_stream_writer_new(GArrowOutputStream *sink,
                                      GArrowSchema *schema,
                                      GError **error)
{
  auto arrow_sink = garrow_output_stream_get_raw(sink).get();
  std::shared_ptr<arrow::ipc::RecordBatchStreamWriter> arrow_writer;
  auto status =
    arrow::ipc::RecordBatchStreamWriter::Open(arrow_sink,
                                              garrow_schema_get_raw(schema),
                                              &arrow_writer);
  if (garrow_error_check(error, status, "[record-batch-stream-writer][open]")) {
    return garrow_record_batch_stream_writer_new_raw(&arrow_writer);
  } else {
    return NULL;
  }
}


G_DEFINE_TYPE(GArrowRecordBatchFileWriter,
              garrow_record_batch_file_writer,
              GARROW_TYPE_RECORD_BATCH_STREAM_WRITER);

static void
garrow_record_batch_file_writer_init(GArrowRecordBatchFileWriter *object)
{
}

static void
garrow_record_batch_file_writer_class_init(GArrowRecordBatchFileWriterClass *klass)
{
}

/**
 * garrow_record_batch_file_writer_new:
 * @sink: The output of the writer.
 * @schema: The schema of the writer.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowRecordBatchFileWriter
 *   or %NULL on error.
 *
 * Since: 0.4.0
 */
GArrowRecordBatchFileWriter *
garrow_record_batch_file_writer_new(GArrowOutputStream *sink,
                       GArrowSchema *schema,
                       GError **error)
{
  auto arrow_sink = garrow_output_stream_get_raw(sink);
  std::shared_ptr<arrow::ipc::RecordBatchFileWriter> arrow_writer;
  auto status =
    arrow::ipc::RecordBatchFileWriter::Open(arrow_sink.get(),
                                            garrow_schema_get_raw(schema),
                                            &arrow_writer);
  if (garrow_error_check(error, status, "[record-batch-file-writer][open]")) {
    return garrow_record_batch_file_writer_new_raw(&arrow_writer);
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowRecordBatchWriter *
garrow_record_batch_writer_new_raw(std::shared_ptr<arrow::ipc::RecordBatchWriter> *arrow_writer)
{
  auto writer =
    GARROW_RECORD_BATCH_WRITER(
      g_object_new(GARROW_TYPE_RECORD_BATCH_WRITER,
                   "record-batch-writer", arrow_writer,
                   NULL));
  return writer;
}

std::shared_ptr<arrow::ipc::RecordBatchWriter>
garrow_record_batch_writer_get_raw(GArrowRecordBatchWriter *writer)
{
  GArrowRecordBatchWriterPrivate *priv;

  priv = GARROW_RECORD_BATCH_WRITER_GET_PRIVATE(writer);
  return priv->record_batch_writer;
}

GArrowRecordBatchStreamWriter *
garrow_record_batch_stream_writer_new_raw(std::shared_ptr<arrow::ipc::RecordBatchStreamWriter> *arrow_writer)
{
  auto writer =
    GARROW_RECORD_BATCH_STREAM_WRITER(
      g_object_new(GARROW_TYPE_RECORD_BATCH_STREAM_WRITER,
                   "record-batch-writer", arrow_writer,
                   NULL));
  return writer;
}

GArrowRecordBatchFileWriter *
garrow_record_batch_file_writer_new_raw(std::shared_ptr<arrow::ipc::RecordBatchFileWriter> *arrow_writer)
{
  auto writer =
    GARROW_RECORD_BATCH_FILE_WRITER(
      g_object_new(GARROW_TYPE_RECORD_BATCH_FILE_WRITER,
                   "record-batch-writer", arrow_writer,
                   NULL));
  return writer;
}
