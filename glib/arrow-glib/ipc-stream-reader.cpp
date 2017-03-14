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

#include <arrow-glib/io-input-stream.hpp>

#include <arrow-glib/ipc-metadata-version.hpp>
#include <arrow-glib/ipc-stream-reader.hpp>

G_BEGIN_DECLS

/**
 * SECTION: ipc-stream-reader
 * @short_description: Stream reader class
 *
 * #GArrowIPCStreamReader is a class for receiving data by stream
 * based IPC.
 */

typedef struct GArrowIPCStreamReaderPrivate_ {
  std::shared_ptr<arrow::ipc::StreamReader> stream_reader;
} GArrowIPCStreamReaderPrivate;

enum {
  PROP_0,
  PROP_STREAM_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowIPCStreamReader,
                           garrow_ipc_stream_reader,
                           G_TYPE_OBJECT);

#define GARROW_IPC_STREAM_READER_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_IPC_TYPE_STREAM_READER,   \
                               GArrowIPCStreamReaderPrivate))

static void
garrow_ipc_stream_reader_finalize(GObject *object)
{
  GArrowIPCStreamReaderPrivate *priv;

  priv = GARROW_IPC_STREAM_READER_GET_PRIVATE(object);

  priv->stream_reader = nullptr;

  G_OBJECT_CLASS(garrow_ipc_stream_reader_parent_class)->finalize(object);
}

static void
garrow_ipc_stream_reader_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  GArrowIPCStreamReaderPrivate *priv;

  priv = GARROW_IPC_STREAM_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STREAM_READER:
    priv->stream_reader =
      *static_cast<std::shared_ptr<arrow::ipc::StreamReader> *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_ipc_stream_reader_get_property(GObject *object,
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
garrow_ipc_stream_reader_init(GArrowIPCStreamReader *object)
{
}

static void
garrow_ipc_stream_reader_class_init(GArrowIPCStreamReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_ipc_stream_reader_finalize;
  gobject_class->set_property = garrow_ipc_stream_reader_set_property;
  gobject_class->get_property = garrow_ipc_stream_reader_get_property;

  spec = g_param_spec_pointer("stream-reader",
                              "ipc::StreamReader",
                              "The raw std::shared<arrow::ipc::StreamReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STREAM_READER, spec);
}

/**
 * garrow_ipc_stream_reader_open:
 * @stream: The stream to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A newly opened
 *   #GArrowIPCStreamReader or %NULL on error.
 */
GArrowIPCStreamReader *
garrow_ipc_stream_reader_open(GArrowIOInputStream *stream,
                              GError **error)
{
  std::shared_ptr<arrow::ipc::StreamReader> arrow_stream_reader;
  auto status =
    arrow::ipc::StreamReader::Open(garrow_io_input_stream_get_raw(stream),
                                   &arrow_stream_reader);
  if (status.ok()) {
    return garrow_ipc_stream_reader_new_raw(&arrow_stream_reader);
  } else {
    garrow_error_set(error, status, "[ipc][stream-reader][open]");
    return NULL;
  }
}

/**
 * garrow_ipc_stream_reader_get_schema:
 * @stream_reader: A #GArrowIPCStreamReader.
 *
 * Returns: (transfer full): The schema in the stream.
 */
GArrowSchema *
garrow_ipc_stream_reader_get_schema(GArrowIPCStreamReader *stream_reader)
{
  auto arrow_stream_reader =
    garrow_ipc_stream_reader_get_raw(stream_reader);
  auto arrow_schema = arrow_stream_reader->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_ipc_stream_reader_get_next_record_batch:
 * @stream_reader: A #GArrowIPCStreamReader.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 */
GArrowRecordBatch *
garrow_ipc_stream_reader_get_next_record_batch(GArrowIPCStreamReader *stream_reader,
                                               GError **error)
{
  auto arrow_stream_reader =
    garrow_ipc_stream_reader_get_raw(stream_reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_stream_reader->GetNextRecordBatch(&arrow_record_batch);

  if (status.ok()) {
    if (arrow_record_batch == nullptr) {
      return NULL;
    } else {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  } else {
    garrow_error_set(error, status, "[ipc][stream-reader][get-next-record-batch]");
    return NULL;
  }
}

G_END_DECLS

GArrowIPCStreamReader *
garrow_ipc_stream_reader_new_raw(std::shared_ptr<arrow::ipc::StreamReader> *arrow_stream_reader)
{
  auto stream_reader =
    GARROW_IPC_STREAM_READER(g_object_new(GARROW_IPC_TYPE_STREAM_READER,
                                          "stream-reader", arrow_stream_reader,
                                          NULL));
  return stream_reader;
}

std::shared_ptr<arrow::ipc::StreamReader>
garrow_ipc_stream_reader_get_raw(GArrowIPCStreamReader *stream_reader)
{
  GArrowIPCStreamReaderPrivate *priv;

  priv = GARROW_IPC_STREAM_READER_GET_PRIVATE(stream_reader);
  return priv->stream_reader;
}
