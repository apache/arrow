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

#include <arrow-glib/io-output-stream.hpp>

#include <arrow-glib/ipc-stream-writer.hpp>

G_BEGIN_DECLS

/**
 * SECTION: ipc-stream-writer
 * @short_description: Stream writer class
 *
 * #GArrowIPCStreamWriter is a class for sending data by stream based
 * IPC.
 */

typedef struct GArrowIPCStreamWriterPrivate_ {
  std::shared_ptr<arrow::ipc::StreamWriter> stream_writer;
} GArrowIPCStreamWriterPrivate;

enum {
  PROP_0,
  PROP_STREAM_WRITER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowIPCStreamWriter,
                           garrow_ipc_stream_writer,
                           G_TYPE_OBJECT);

#define GARROW_IPC_STREAM_WRITER_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_IPC_TYPE_STREAM_WRITER,   \
                               GArrowIPCStreamWriterPrivate))

static void
garrow_ipc_stream_writer_finalize(GObject *object)
{
  GArrowIPCStreamWriterPrivate *priv;

  priv = GARROW_IPC_STREAM_WRITER_GET_PRIVATE(object);

  priv->stream_writer = nullptr;

  G_OBJECT_CLASS(garrow_ipc_stream_writer_parent_class)->finalize(object);
}

static void
garrow_ipc_stream_writer_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  GArrowIPCStreamWriterPrivate *priv;

  priv = GARROW_IPC_STREAM_WRITER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STREAM_WRITER:
    priv->stream_writer =
      *static_cast<std::shared_ptr<arrow::ipc::StreamWriter> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_ipc_stream_writer_get_property(GObject *object,
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
garrow_ipc_stream_writer_init(GArrowIPCStreamWriter *object)
{
}

static void
garrow_ipc_stream_writer_class_init(GArrowIPCStreamWriterClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_ipc_stream_writer_finalize;
  gobject_class->set_property = garrow_ipc_stream_writer_set_property;
  gobject_class->get_property = garrow_ipc_stream_writer_get_property;

  spec = g_param_spec_pointer("stream-writer",
                              "ipc::StreamWriter",
                              "The raw std::shared<arrow::ipc::StreamWriter> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STREAM_WRITER, spec);
}

/**
 * garrow_ipc_stream_writer_open:
 * @sink: The output of the writer.
 * @schema: The schema of the writer.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowIPCStreamWriter or %NULL on error.
 */
GArrowIPCStreamWriter *
garrow_ipc_stream_writer_open(GArrowIOOutputStream *sink,
                              GArrowSchema *schema,
                              GError **error)
{
  std::shared_ptr<arrow::ipc::StreamWriter> arrow_stream_writer;
  auto status =
    arrow::ipc::StreamWriter::Open(garrow_io_output_stream_get_raw(sink).get(),
                                 garrow_schema_get_raw(schema),
                                 &arrow_stream_writer);
  if (status.ok()) {
    return garrow_ipc_stream_writer_new_raw(&arrow_stream_writer);
  } else {
    garrow_error_set(error, status, "[ipc][stream-writer][open]");
    return NULL;
  }
}

/**
 * garrow_ipc_stream_writer_write_record_batch:
 * @stream_writer: A #GArrowIPCStreamWriter.
 * @record_batch: The record batch to be written.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_ipc_stream_writer_write_record_batch(GArrowIPCStreamWriter *stream_writer,
                                            GArrowRecordBatch *record_batch,
                                            GError **error)
{
  auto arrow_stream_writer =
    garrow_ipc_stream_writer_get_raw(stream_writer);
  auto arrow_record_batch =
    garrow_record_batch_get_raw(record_batch);
  auto arrow_record_batch_raw =
    arrow_record_batch.get();

  auto status = arrow_stream_writer->WriteRecordBatch(*arrow_record_batch_raw);
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[ipc][stream-writer][write-record-batch]");
    return FALSE;
  }
}

/**
 * garrow_ipc_stream_writer_close:
 * @stream_writer: A #GArrowIPCStreamWriter.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 */
gboolean
garrow_ipc_stream_writer_close(GArrowIPCStreamWriter *stream_writer,
                               GError **error)
{
  auto arrow_stream_writer =
    garrow_ipc_stream_writer_get_raw(stream_writer);

  auto status = arrow_stream_writer->Close();
  if (status.ok()) {
    return TRUE;
  } else {
    garrow_error_set(error, status, "[ipc][stream-writer][close]");
    return FALSE;
  }
}

G_END_DECLS

GArrowIPCStreamWriter *
garrow_ipc_stream_writer_new_raw(std::shared_ptr<arrow::ipc::StreamWriter> *arrow_stream_writer)
{
  auto stream_writer =
    GARROW_IPC_STREAM_WRITER(g_object_new(GARROW_IPC_TYPE_STREAM_WRITER,
                                        "stream-writer", arrow_stream_writer,
                                        NULL));
  return stream_writer;
}

std::shared_ptr<arrow::ipc::StreamWriter>
garrow_ipc_stream_writer_get_raw(GArrowIPCStreamWriter *stream_writer)
{
  GArrowIPCStreamWriterPrivate *priv;

  priv = GARROW_IPC_STREAM_WRITER_GET_PRIVATE(stream_writer);
  return priv->stream_writer;
}
