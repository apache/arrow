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

#include <arrow-glib/buffer.hpp>
#include <arrow-glib/decoder.hpp>
#include <arrow-glib/error.hpp>
#include <arrow-glib/internal-hash-table.hpp>
#include <arrow-glib/ipc-options.hpp>
#include <arrow-glib/record-batch.hpp>
#include <arrow-glib/schema.hpp>

G_BEGIN_DECLS

/**
 * SECTION: decoder
 * @section_id: decoder-classes
 * @title: Decoder classes
 * @include: arrow-glib/arrow-glib.h
 *
 * #GArrowStreamListener is a class for receiving decoded information
 * from #GArrowStreamDecoder.
 *
 * #GArrowStreamDecoder is a class for decoding record batches in
 * stream format from given data chunks.
 */

struct GArrowStreamListenerPrivate
{
  std::shared_ptr<arrow::ipc::Listener> listener;
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GArrowStreamListener,
                                    garrow_stream_listener,
                                    G_TYPE_OBJECT);

#define GARROW_STREAM_LISTENER_GET_PRIVATE(object)                                       \
  static_cast<GArrowStreamListenerPrivate *>(                                            \
    garrow_stream_listener_get_instance_private(GARROW_STREAM_LISTENER(object)))

G_END_DECLS

namespace garrow {
  class StreamListener : public arrow::ipc::Listener {
  public:
    StreamListener(GArrowStreamListener *listener) : listener_(listener)
    {
      g_object_ref(listener_);
    }
    ~StreamListener() { g_object_unref(listener_); }

    arrow::Status
    OnEOS() override
    {
      if (!klass()->on_eos) {
        return arrow::Status::OK();
      }

      GError *error = nullptr;
      if (garrow_stream_listener_on_eos(listener_, &error)) {
        return arrow::Status::OK();
      } else {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::UnknownError,
                                      "[stream-listener][on-eos]");
      }
    }

    arrow::Status
    OnRecordBatchWithMetadataDecoded(
      arrow::RecordBatchWithMetadata arrow_record_batch_with_metadata) override
    {
      if (!klass()->on_record_batch_decoded) {
        return arrow::Status::OK();
      }

      auto record_batch =
        garrow_record_batch_new_raw(&(arrow_record_batch_with_metadata.batch));
      GHashTable *metadata = nullptr;
      if (arrow_record_batch_with_metadata.custom_metadata) {
        metadata = garrow_internal_hash_table_from_metadata(
          arrow_record_batch_with_metadata.custom_metadata);
      }
      GError *error = nullptr;
      auto success = garrow_stream_listener_on_record_batch_decoded(listener_,
                                                                    record_batch,
                                                                    metadata,
                                                                    &error);
      g_object_unref(record_batch);
      if (metadata) {
        g_hash_table_unref(metadata);
      }
      if (success) {
        return arrow::Status::OK();
      } else {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::UnknownError,
                                      "[stream-listener][on-record-batch-decoded]");
      }
    }

    arrow::Status
    OnSchemaDecoded(std::shared_ptr<arrow::Schema> arrow_schema,
                    std::shared_ptr<arrow::Schema> arrow_filtered_schema) override
    {
      if (!klass()->on_schema_decoded) {
        return arrow::Status::OK();
      }

      auto schema = garrow_schema_new_raw(&arrow_schema);
      auto filtered_schema = garrow_schema_new_raw(&arrow_filtered_schema);
      GError *error = nullptr;
      auto success = garrow_stream_listener_on_schema_decoded(listener_,
                                                              schema,
                                                              filtered_schema,
                                                              &error);
      g_object_unref(schema);
      g_object_unref(filtered_schema);
      if (success) {
        return arrow::Status::OK();
      } else {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::UnknownError,
                                      "[stream-listener][on-schema-decoded]");
      }
    }

  private:
    GArrowStreamListener *listener_;

    GArrowStreamListenerClass *
    klass()
    {
      return GARROW_STREAM_LISTENER_GET_CLASS(listener_);
    }
  };
}; // namespace garrow

G_BEGIN_DECLS

static void
garrow_stream_listener_finalize(GObject *object)
{
  auto priv = GARROW_STREAM_LISTENER_GET_PRIVATE(object);
  priv->listener.~shared_ptr();
  G_OBJECT_CLASS(garrow_stream_listener_parent_class)->finalize(object);
}

static void
garrow_stream_listener_init(GArrowStreamListener *object)
{
  auto priv = GARROW_STREAM_LISTENER_GET_PRIVATE(object);
  new (&priv->listener)
    std::shared_ptr<garrow::StreamListener>(new garrow::StreamListener(object));
}

static void
garrow_stream_listener_class_init(GArrowStreamListenerClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = garrow_stream_listener_finalize;

  klass->on_eos = nullptr;
  klass->on_record_batch_decoded = nullptr;
  klass->on_schema_decoded = nullptr;
}

/**
 * garrow_stream_listener_on_eos:
 * @listener: A #GArrowStreamListener.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Processes an EOS event.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_listener_on_eos(GArrowStreamListener *listener, GError **error)
{
  auto klass = GARROW_STREAM_LISTENER_GET_CLASS(listener);
  if (!(klass && klass->on_eos)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "[stream-listener][on-eos] not implemented");
    return false;
  }
  return klass->on_eos(listener, error);
}

/**
 * garrow_stream_listener_on_record_batch_decoded:
 * @listener: A #GArrowStreamListener.
 * @record_batch: A decoded #GArrowRecordBatch.
 * @metadata: (element-type utf8 utf8) (nullable): A decoded metadata.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Processes a decoded record batch.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_listener_on_record_batch_decoded(GArrowStreamListener *listener,
                                               GArrowRecordBatch *record_batch,
                                               GHashTable *metadata,
                                               GError **error)
{
  auto klass = GARROW_STREAM_LISTENER_GET_CLASS(listener);
  if (!(klass && klass->on_record_batch_decoded)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "[stream-listener][on-record-batch-decoded] not implemented");
    return false;
  }
  return klass->on_record_batch_decoded(listener, record_batch, metadata, error);
}

/**
 * garrow_stream_listener_on_schema_decoded:
 * @listener: A #GArrowStreamListener.
 * @schema: A decoded #GArrowSchema.
 * @filtered_schema: A decoded #GArrowSchema that only has read fields.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Processes a decoded schema.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_listener_on_schema_decoded(GArrowStreamListener *listener,
                                         GArrowSchema *schema,
                                         GArrowSchema *filtered_schema,
                                         GError **error)
{
  auto klass = GARROW_STREAM_LISTENER_GET_CLASS(listener);
  if (!(klass && klass->on_schema_decoded)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "[stream-listener][on-schema-decoded] not implemented");
    return false;
  }
  return klass->on_schema_decoded(listener, schema, filtered_schema, error);
}

struct GArrowStreamDecoderPrivate
{
  std::shared_ptr<arrow::ipc::StreamDecoder> decoder;
  GArrowStreamListener *listener;
};

enum {
  PROP_DECODER = 1,
  PROP_LISTENER,
};

G_DEFINE_TYPE_WITH_PRIVATE(GArrowStreamDecoder, garrow_stream_decoder, G_TYPE_OBJECT);

#define GARROW_STREAM_DECODER_GET_PRIVATE(object)                                        \
  static_cast<GArrowStreamDecoderPrivate *>(                                             \
    garrow_stream_decoder_get_instance_private(GARROW_STREAM_DECODER(object)))

static void
garrow_stream_decoder_finalize(GObject *object)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(object);
  priv->decoder.~shared_ptr();
  G_OBJECT_CLASS(garrow_stream_decoder_parent_class)->finalize(object);
}

static void
garrow_stream_decoder_dispose(GObject *object)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(object);

  if (priv->listener) {
    g_object_unref(priv->listener);
    priv->listener = nullptr;
  }

  G_OBJECT_CLASS(garrow_stream_decoder_parent_class)->dispose(object);
}

static void
garrow_stream_decoder_set_property(GObject *object,
                                   guint prop_id,
                                   const GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_DECODER:
    priv->decoder = *static_cast<std::shared_ptr<arrow::ipc::StreamDecoder> *>(
      g_value_get_pointer(value));
    break;
  case PROP_LISTENER:
    priv->listener = GARROW_STREAM_LISTENER(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_stream_decoder_get_property(GObject *object,
                                   guint prop_id,
                                   GValue *value,
                                   GParamSpec *pspec)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_LISTENER:
    g_value_set_object(value, priv->listener);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
garrow_stream_decoder_init(GArrowStreamDecoder *object)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(object);
  new (&priv->decoder) std::shared_ptr<arrow::ipc::StreamDecoder>;
}

static void
garrow_stream_decoder_class_init(GArrowStreamDecoderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = garrow_stream_decoder_finalize;
  gobject_class->dispose = garrow_stream_decoder_dispose;
  gobject_class->set_property = garrow_stream_decoder_set_property;
  gobject_class->get_property = garrow_stream_decoder_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer(
    "decoder",
    nullptr,
    nullptr,
    static_cast<GParamFlags>(G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_DECODER, spec);

  /**
   * GArrowStreamDecoder:listener:
   *
   * A listener that receives decoded events.
   *
   * Since: 18.0.0
   */
  spec = g_param_spec_object(
    "listener",
    nullptr,
    nullptr,
    GARROW_TYPE_STREAM_LISTENER,
    static_cast<GParamFlags>(G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_LISTENER, spec);
}

/**
 * garrow_stream_decoder_new:
 * @listener: The #GArrowStreamListener that receives decoded events.
 * @options: (nullable): The #GArrowReadOptions.
 *
 * Returns: A newly created #GArrowStreamDecoder.
 *
 * Since: 18.0.0
 */
GArrowStreamDecoder *
garrow_stream_decoder_new(GArrowStreamListener *listener, GArrowReadOptions *options)
{
  auto arrow_listener = garrow_stream_listener_get_raw(listener);
  arrow::ipc::IpcReadOptions arrow_options;
  if (options) {
    arrow_options = *garrow_read_options_get_raw(options);
  } else {
    arrow_options = arrow::ipc::IpcReadOptions::Defaults();
  }
  auto arrow_decoder =
    std::make_shared<arrow::ipc::StreamDecoder>(arrow_listener, arrow_options);
  return garrow_stream_decoder_new_raw(&arrow_decoder, listener);
}

/**
 * garrow_stream_decoder_consume_bytes:
 * @decoder: A #GArrowStreamDecoder.
 * @bytes: A #GBytes to be decoded.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Feed data to the decoder as a raw data.
 *
 * If the decoder can read one or more record batches by the data, the
 * decoder calls [vfunc@GArrowStreamListener.on_record_batch_decoded]
 * with a decoded record batch multiple times.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_decoder_consume_bytes(GArrowStreamDecoder *decoder,
                                    GBytes *bytes,
                                    GError **error)
{
  auto arrow_decoder = garrow_stream_decoder_get_raw(decoder);
  gsize size;
  gconstpointer data = g_bytes_get_data(bytes, &size);
  return garrow::check(error,
                       arrow_decoder->Consume(static_cast<const uint8_t *>(data), size),
                       "[stream-decoder][consume-bytes]");
}

/**
 * garrow_stream_decoder_consume_buffer:
 * @decoder: A #GArrowStreamDecoder.
 * @buffer: A #GArrowBuffer to be decoded.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Feed data to the decoder as a #GArrowBuffer.
 *
 * If the decoder can read one or more record batches by the data, the
 * decoder calls [vfunc@GArrowStreamListener.on_record_batch_decoded]
 * with a decoded record batch multiple times.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_decoder_consume_buffer(GArrowStreamDecoder *decoder,
                                     GArrowBuffer *buffer,
                                     GError **error)
{
  auto arrow_decoder = garrow_stream_decoder_get_raw(decoder);
  auto arrow_buffer = garrow_buffer_get_raw(buffer);
  return garrow::check(error,
                       arrow_decoder->Consume(arrow_buffer),
                       "[stream-decoder][consume-buffer]");
}

/**
 * garrow_stream_decoder_reset:
 * @decoder: A #GArrowStreamDecoder.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Reset the internal status.
 *
 * You can reuse this decoder for new stream after calling this.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 18.0.0
 */
gboolean
garrow_stream_decoder_reset(GArrowStreamDecoder *decoder, GError **error)
{
  auto arrow_decoder = garrow_stream_decoder_get_raw(decoder);
  return garrow::check(error, arrow_decoder->Reset(), "[stream-decoder][reset]");
}

/**
 * garrow_stream_decoder_get_schema:
 * @decoder: A #GArrowStreamDecoder.
 *
 * Returns: (nullable) (transfer full): The shared #GArrowSchema of
 *   the record batches in the stream.
 *
 * Since: 18.0.0
 */
GArrowSchema *
garrow_stream_decoder_get_schema(GArrowStreamDecoder *decoder)
{
  auto arrow_decoder = garrow_stream_decoder_get_raw(decoder);
  auto arrow_schema = arrow_decoder->schema();
  if (arrow_schema) {
    return garrow_schema_new_raw(&arrow_schema);
  } else {
    return nullptr;
  }
}

/**
 * garrow_stream_decoder_get_next_required_size:
 * @decoder: A #GArrowStreamDecoder.
 *
 * This method is provided for users who want to optimize performance.
 * Normal users don't need to use this method.
 *
 * Here is an example usage for normal users:
 *
 *     garrow_stream_decoder_consume_buffer(decoder, buffer1);
 *     garrow_stream_decoder_consume_buffer(decoder, buffer2);
 *     garrow_stream_decoder_consume_buffer(decoder, buffer3);
 *
 * Decoder has internal buffer. If consumed data isn't enough to
 * advance the state of the decoder, consumed data is buffered to
 * the internal buffer. It causes performance overhead.
 *
 * If you pass garrow_stream_decoer_get_next_required_size() size data
 * to each
 * garrow_stream_decoder_consume_bytes()/garrow_stream_decoder_consume_buffer()
 * call, the decoder doesn't use its internal buffer. It improves
 * performance.
 *
 * Here is an example usage to avoid using internal buffer:
 *
 *     buffer1 = get_data(garrow_stream_decoder_get_next_required_size(decoder));
 *     garrow_stream_decoder_consume_buffer(buffer1);
 *     buffer2 = get_data(garrow_stream_decoder_get_next_required_size(decoder));
 *     garrow_stream_decoder_consume_buffer(buffer2);
 *
 * Users can use this method to avoid creating small chunks. Record
 * batch data must be contiguous data. If users pass small chunks to
 * the decoder, the decoder needs concatenate small chunks
 * internally. It causes performance overhead.
 *
 * Here is an example usage to reduce small chunks:
 *
 *     GArrowResizablBuffer *buffer = garrow_resizable_buffer_new(1024, NULL);
 *     while ((small_chunk = get_data(&small_chunk_size))) {
 *       size_t current_buffer_size = garrow_buffer_get_size(GARROW_BUFFER(buffer));
 *       garrow_resizable_buffer_resize(buffer, current_buffer_size + small_chunk_size,
NULL);
 *       garrow_mutable_buffer_set_data(GARROW_MUTABLE_BUFFER(buffer),
 *                                      current_buffer_size,
 *                                      small_chunk,
 *                                      small_chunk_size,
 *                                      NULL);
 *       if (garrow_buffer_get_size(GARROW_BUFFER(buffer)) <
 *           garrow_stream_decoder_get_next_required_size(decoder)) {
 *         continue;
 *       }
 *       garrow_stream_decoder_consume_buffer(decoder, GARROW_BUFFER(buffer), NULL);
 *       g_object_unref(buffer);
 *       buffer = garrow_resizable_buffer_new(1024, NULL);
 *     }
 *     if (garrow_buffer_get_size(GARROW_BUFFER(buffer)) > 0) {
 *       garrow_stream_decoder_consume_buffer(decoder, GARROW_BUFFER(buffer), NULL);
 *     }
 *     g_object_unref(buffer);
 *
 * Returns: The number of bytes needed to advance the state of
 *   the decoder.
 *
 * Since: 18.0.0
 */
gsize
garrow_stream_decoder_get_next_required_size(GArrowStreamDecoder *decoder)
{
  auto arrow_decoder = garrow_stream_decoder_get_raw(decoder);
  return arrow_decoder->next_required_size();
}

G_END_DECLS

std::shared_ptr<arrow::ipc::Listener>
garrow_stream_listener_get_raw(GArrowStreamListener *listener)
{
  auto priv = GARROW_STREAM_LISTENER_GET_PRIVATE(listener);
  return priv->listener;
}

GArrowStreamDecoder *
garrow_stream_decoder_new_raw(std::shared_ptr<arrow::ipc::StreamDecoder> *arrow_decoder,
                              GArrowStreamListener *listener)
{
  return GARROW_STREAM_DECODER(g_object_new(GARROW_TYPE_STREAM_DECODER,
                                            "decoder",
                                            arrow_decoder,
                                            "listener",
                                            listener,
                                            nullptr));
}

std::shared_ptr<arrow::ipc::StreamDecoder>
garrow_stream_decoder_get_raw(GArrowStreamDecoder *decoder)
{
  auto priv = GARROW_STREAM_DECODER_GET_PRIVATE(decoder);
  return priv->decoder;
}
