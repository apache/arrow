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
#include <arrow-glib/stream-reader.hpp>

G_BEGIN_DECLS

/**
 * SECTION: stream-reader
 * @short_description: Stream reader class
 *
 * #GArrowStreamReader is a class for receiving data by stream
 * based IPC.
 */

typedef struct GArrowStreamReaderPrivate_ {
  std::shared_ptr<arrow::ipc::StreamReader> stream_reader;
} GArrowStreamReaderPrivate;

enum {
  PROP_0,
  PROP_STREAM_READER
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowStreamReader,
                           garrow_stream_reader,
                           G_TYPE_OBJECT);

#define GARROW_STREAM_READER_GET_PRIVATE(obj)               \
  (G_TYPE_INSTANCE_GET_PRIVATE((obj),                           \
                               GARROW_TYPE_STREAM_READER,   \
                               GArrowStreamReaderPrivate))

static void
garrow_stream_reader_finalize(GObject *object)
{
  GArrowStreamReaderPrivate *priv;

  priv = GARROW_STREAM_READER_GET_PRIVATE(object);

  priv->stream_reader = nullptr;

  G_OBJECT_CLASS(garrow_stream_reader_parent_class)->finalize(object);
}

static void
garrow_stream_reader_set_property(GObject *object,
                                    guint prop_id,
                                    const GValue *value,
                                    GParamSpec *pspec)
{
  GArrowStreamReaderPrivate *priv;

  priv = GARROW_STREAM_READER_GET_PRIVATE(object);

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
garrow_stream_reader_get_property(GObject *object,
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
garrow_stream_reader_init(GArrowStreamReader *object)
{
}

static void
garrow_stream_reader_class_init(GArrowStreamReaderClass *klass)
{
  GObjectClass *gobject_class;
  GParamSpec *spec;

  gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize     = garrow_stream_reader_finalize;
  gobject_class->set_property = garrow_stream_reader_set_property;
  gobject_class->get_property = garrow_stream_reader_get_property;

  spec = g_param_spec_pointer("stream-reader",
                              "ipc::StreamReader",
                              "The raw std::shared<arrow::ipc::StreamReader> *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STREAM_READER, spec);
}

/**
 * garrow_stream_reader_new:
 * @stream: The stream to be read.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable): A newly created #GArrowStreamReader or %NULL
 *   on error.
 */
GArrowStreamReader *
garrow_stream_reader_new(GArrowInputStream *stream,
                         GError **error)
{
  std::shared_ptr<arrow::ipc::StreamReader> arrow_stream_reader;
  auto status =
    arrow::ipc::StreamReader::Open(garrow_input_stream_get_raw(stream),
                                   &arrow_stream_reader);
  if (garrow_error_check(error, status, "[ipc][stream-reader][open]")) {
    return garrow_stream_reader_new_raw(&arrow_stream_reader);
  } else {
    return NULL;
  }
}

/**
 * garrow_stream_reader_get_schema:
 * @stream_reader: A #GArrowStreamReader.
 *
 * Returns: (transfer full): The schema in the stream.
 */
GArrowSchema *
garrow_stream_reader_get_schema(GArrowStreamReader *stream_reader)
{
  auto arrow_stream_reader =
    garrow_stream_reader_get_raw(stream_reader);
  auto arrow_schema = arrow_stream_reader->schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * garrow_stream_reader_get_next_record_batch:
 * @stream_reader: A #GArrowStreamReader.
 * @error: (nullable): Return locatipcn for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The next record batch in the stream or %NULL on end of stream.
 */
GArrowRecordBatch *
garrow_stream_reader_get_next_record_batch(GArrowStreamReader *stream_reader,
                                               GError **error)
{
  auto arrow_stream_reader =
    garrow_stream_reader_get_raw(stream_reader);
  std::shared_ptr<arrow::RecordBatch> arrow_record_batch;
  auto status = arrow_stream_reader->GetNextRecordBatch(&arrow_record_batch);

  if (garrow_error_check(error,
                       status,
                       "[ipc][stream-reader][get-next-record-batch]")) {
    if (arrow_record_batch == nullptr) {
      return NULL;
    } else {
      return garrow_record_batch_new_raw(&arrow_record_batch);
    }
  } else {
    return NULL;
  }
}

G_END_DECLS

GArrowStreamReader *
garrow_stream_reader_new_raw(std::shared_ptr<arrow::ipc::StreamReader> *arrow_stream_reader)
{
  auto stream_reader =
    GARROW_STREAM_READER(g_object_new(GARROW_TYPE_STREAM_READER,
                                          "stream-reader", arrow_stream_reader,
                                          NULL));
  return stream_reader;
}

std::shared_ptr<arrow::ipc::StreamReader>
garrow_stream_reader_get_raw(GArrowStreamReader *stream_reader)
{
  GArrowStreamReaderPrivate *priv;

  priv = GARROW_STREAM_READER_GET_PRIVATE(stream_reader);
  return priv->stream_reader;
}
