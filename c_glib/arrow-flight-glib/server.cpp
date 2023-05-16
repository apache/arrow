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

#include <memory>

#include <arrow-glib/arrow-glib.hpp>

#include <arrow-flight-glib/common.hpp>
#include <arrow-flight-glib/server.hpp>

G_BEGIN_DECLS

/**
 * SECTION: server
 * @section_id: server
 * @title: Server related classes
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightDataStream is a class for producing a sequence of IPC
 * payloads to be sent in `FlightData` protobuf messages. Generally,
 * this is not used directly. Generally, #GAFlightRecordBatchStream is
 * used instead.
 *
 * #GAFlightRecordBatchStream is a class for producing a sequence of
 * IPC payloads to be sent in `FlightData` protobuf messages by
 * #GArrowRecordBatchReader`.
 *
 * #GAFlightServerAuthSender is a class for sending messages to the
 * client during an authentication handshake.
 *
 * #GAFlightServerAuthReader is a class for reading messages from the
 * client during an authentication handshake.
 *
 * #GAFlightServerAuthHandler is an abstract class to implement
 * authentication for a Flight service.
 *
 * #GAFlightServerOptions is a class for options of each server.
 *
 * #GAFlightServerCallContext is a class for context of each server call.
 *
 * #GAFlightServer is a class to develop an Apache Arrow Flight server.
 *
 * Since: 5.0.0
 */


typedef struct GAFlightDataStreamPrivate_ {
  arrow::flight::FlightDataStream *stream;
} GAFlightDataStreamPrivate;

enum {
  PROP_STREAM = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightDataStream,
                           gaflight_data_stream,
                           G_TYPE_OBJECT)

#define GAFLIGHT_DATA_STREAM_GET_PRIVATE(obj)        \
  static_cast<GAFlightDataStreamPrivate *>(          \
    gaflight_data_stream_get_instance_private(       \
      GAFLIGHT_DATA_STREAM(obj)))

static void
gaflight_data_stream_finalize(GObject *object)
{
  auto priv = GAFLIGHT_DATA_STREAM_GET_PRIVATE(object);

  delete priv->stream;

  G_OBJECT_CLASS(gaflight_data_stream_parent_class)->finalize(object);
}

static void
gaflight_data_stream_set_property(GObject *object,
                                  guint prop_id,
                                  const GValue *value,
                                  GParamSpec *pspec)
{
  auto priv = GAFLIGHT_DATA_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STREAM:
    priv->stream = static_cast<arrow::flight::FlightDataStream *>(
      g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_data_stream_init(GAFlightDataStream *object)
{
}

static void
gaflight_data_stream_class_init(GAFlightDataStreamClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_data_stream_finalize;
  gobject_class->set_property = gaflight_data_stream_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("stream",
                              "Stream",
                              "The raw arrow::flight::FlightDataStream *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STREAM, spec);
}


typedef struct GAFlightRecordBatchStreamPrivate_ {
  GArrowRecordBatchReader *reader;
} GAFlightRecordBatchStreamPrivate;

enum {
  PROP_READER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightRecordBatchStream,
                           gaflight_record_batch_stream,
                           GAFLIGHT_TYPE_DATA_STREAM)

#define GAFLIGHT_RECORD_BATCH_STREAM_GET_PRIVATE(obj)        \
  static_cast<GAFlightRecordBatchStreamPrivate *>(           \
    gaflight_record_batch_stream_get_instance_private(       \
      GAFLIGHT_RECORD_BATCH_STREAM(obj)))

static void
gaflight_record_batch_stream_dispose(GObject *object)
{
  auto priv = GAFLIGHT_RECORD_BATCH_STREAM_GET_PRIVATE(object);

  if (priv->reader) {
    g_object_unref(priv->reader);
    priv->reader = NULL;
  }

  G_OBJECT_CLASS(gaflight_record_batch_stream_parent_class)->dispose(object);
}

static void
gaflight_record_batch_stream_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GAFLIGHT_RECORD_BATCH_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READER:
    priv->reader = GARROW_RECORD_BATCH_READER(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_record_batch_stream_get_property(GObject *object,
                                          guint prop_id,
                                          GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GAFLIGHT_RECORD_BATCH_STREAM_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_READER:
    g_value_set_object(value, priv->reader);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_record_batch_stream_init(GAFlightRecordBatchStream *object)
{
}

static void
gaflight_record_batch_stream_class_init(GAFlightRecordBatchStreamClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflight_record_batch_stream_dispose;
  gobject_class->set_property = gaflight_record_batch_stream_set_property;
  gobject_class->get_property = gaflight_record_batch_stream_get_property;

  GParamSpec *spec;
  /**
   * GAFlightRecordBatchStream:reader:
   *
   * The reader that produces record batches.
   *
   * Since: 6.0.0
   */
  spec = g_param_spec_object("reader",
                             "Reader",
                             "The reader that produces record batches",
                             GARROW_TYPE_RECORD_BATCH_READER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_READER, spec);
}

/**
 * gaflight_record_batch_stream_new:
 * @reader: A #GArrowRecordBatchReader to be read.
 * @options: (nullable): A #GArrowWriteOptions for writing record batches to
 *   a client.
 *
 * Returns: The newly created #GAFlightRecordBatchStream.
 *
 * Since: 6.0.0
 */
GAFlightRecordBatchStream *
gaflight_record_batch_stream_new(GArrowRecordBatchReader *reader,
                                 GArrowWriteOptions *options)
{
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  auto arrow_options_default = arrow::ipc::IpcWriteOptions::Defaults();
  arrow::ipc::IpcWriteOptions *arrow_options = NULL;
  if (options) {
    arrow_options = garrow_write_options_get_raw(options);
  } else {
    arrow_options = &arrow_options_default;
  }
  auto stream = std::make_unique<
    arrow::flight::RecordBatchStream>(arrow_reader, *arrow_options);
  return static_cast<GAFlightRecordBatchStream *>(
    g_object_new(GAFLIGHT_TYPE_RECORD_BATCH_STREAM,
                 "stream", stream.release(),
                 "reader", reader,
                 NULL));
}


typedef struct GAFlightServerCallContextPrivate_ {
  arrow::flight::ServerCallContext *call_context;
} GAFlightServerCallContextPrivate;

enum {
  PROP_CALL_CONTEXT = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightServerCallContext,
                           gaflight_server_call_context,
                           G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_CALL_CONTEXT_GET_PRIVATE(obj)   \
  static_cast<GAFlightServerCallContextPrivate *>(      \
    gaflight_server_call_context_get_instance_private(  \
      GAFLIGHT_SERVER_CALL_CONTEXT(obj)))

static void
gaflight_server_call_context_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_CALL_CONTEXT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CALL_CONTEXT:
    priv->call_context =
      static_cast<arrow::flight::ServerCallContext *>(
        g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_call_context_init(GAFlightServerCallContext *object)
{
}

static void
gaflight_server_call_context_class_init(GAFlightServerCallContextClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = gaflight_server_call_context_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("call-context",
                              "Call context",
                              "The raw arrow::flight::ServerCallContext",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CALL_CONTEXT, spec);
}


struct GAFlightServerAuthSenderPrivate {
  arrow::flight::ServerAuthSender *sender;
};

enum {
  PROP_SERVER_AUTH_SENDER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightServerAuthSender,
                           gaflight_server_auth_sender,
                           G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_AUTH_SENDER_GET_PRIVATE(obj)        \
  static_cast<GAFlightServerAuthSenderPrivate *>(           \
    gaflight_server_auth_sender_get_instance_private(       \
      GAFLIGHT_SERVER_AUTH_SENDER(obj)))

static void
gaflight_server_auth_sender_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_AUTH_SENDER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SERVER_AUTH_SENDER:
    priv->sender =
      static_cast<arrow::flight::ServerAuthSender *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_auth_sender_init(GAFlightServerAuthSender *object)
{
}

static void
gaflight_server_auth_sender_class_init(GAFlightServerAuthSenderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = gaflight_server_auth_sender_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("sender",
                              "Sender",
                              "The raw arrow::flight::ServerAuthSender *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SERVER_AUTH_SENDER, spec);
}

/**
 * gaflight_server_auth_sender_write:
 * @sender: A #GAFlightServerAuthSender.
 * @message: A #GBytes to be sent.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Writes a message to the client.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 12.0.0
 */
gboolean
gaflight_server_auth_sender_write(GAFlightServerAuthSender *sender,
                                  GBytes *message,
                                  GError **error)
{
  auto flight_sender = gaflight_server_auth_sender_get_raw(sender);
  gsize size;
  auto data = g_bytes_get_data(message, &size);
  const std::string flight_message(static_cast<const char *>(data), size);
  auto status = flight_sender->Write(flight_message);
  return garrow::check(error, status, "[flight-server-auth-sender][write]");
}


struct GAFlightServerAuthReaderPrivate {
  arrow::flight::ServerAuthReader *reader;
};

enum {
  PROP_SERVER_AUTH_READER = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightServerAuthReader,
                           gaflight_server_auth_reader,
                           G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_AUTH_READER_GET_PRIVATE(obj)        \
  static_cast<GAFlightServerAuthReaderPrivate *>(           \
    gaflight_server_auth_reader_get_instance_private(       \
      GAFLIGHT_SERVER_AUTH_READER(obj)))

static void
gaflight_server_auth_reader_set_property(GObject *object,
                                         guint prop_id,
                                         const GValue *value,
                                         GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_AUTH_READER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SERVER_AUTH_READER:
    priv->reader =
      static_cast<arrow::flight::ServerAuthReader *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_auth_reader_init(GAFlightServerAuthReader *object)
{
}

static void
gaflight_server_auth_reader_class_init(GAFlightServerAuthReaderClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->set_property = gaflight_server_auth_reader_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("reader",
                              "Reader",
                              "The raw arrow::flight::ServerAuthReader *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SERVER_AUTH_READER, spec);
}

/**
 * gaflight_server_auth_reader_read:
 * @reader: A #GAFlightServerAuthReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Reads a message from the client.
 *
 * Returns: (nullable) (transfer full): Read data as #GBytes on
 *   success, %NULL on error.
 *
 * Since: 12.0.0
 */
GBytes *
gaflight_server_auth_reader_read(GAFlightServerAuthReader *reader,
                                 GError **error)
{
  auto flight_reader = gaflight_server_auth_reader_get_raw(reader);
  std::string flight_message;
  auto status = flight_reader->Read(&flight_message);
  if (!garrow::check(error, status, "[flight-server-auth-reader][read]")) {
    return nullptr;
  }
  return g_bytes_new(flight_message.data(), flight_message.size());
}


struct GAFlightServerAuthHandlerPrivate {
  std::shared_ptr<arrow::flight::ServerAuthHandler> handler;
};

enum {
  PROP_SERVER_AUTH_HANDLER = 1,
};

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GAFlightServerAuthHandler,
                                    gaflight_server_auth_handler,
                                    G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(obj)        \
  static_cast<GAFlightServerAuthHandlerPrivate *>(           \
    gaflight_server_auth_handler_get_instance_private(       \
      GAFLIGHT_SERVER_AUTH_HANDLER(obj)))

static void
gaflight_server_auth_handler_finalize(GObject *object)
{
  auto priv = GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(object);
  priv->handler.~shared_ptr();
  G_OBJECT_CLASS(gaflight_server_auth_handler_parent_class)->finalize(object);
}

static void
gaflight_server_auth_handler_set_property(GObject *object,
                                          guint prop_id,
                                          const GValue *value,
                                          GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_SERVER_AUTH_HANDLER:
    if (g_value_get_pointer(value)) {
      priv->handler =
        *static_cast<std::shared_ptr<arrow::flight::ServerAuthHandler>*>(
          g_value_get_pointer(value));
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_auth_handler_init(GAFlightServerAuthHandler *object)
{
  auto priv = GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(object);
  new(&priv->handler) std::shared_ptr<arrow::flight::ServerAuthHandler>;
}

static void
gaflight_server_auth_handler_class_init(GAFlightServerAuthHandlerClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_server_auth_handler_finalize;
  gobject_class->set_property = gaflight_server_auth_handler_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("handler",
                              "Handler",
                              "The raw std::shared_ptr<"
                              "arrow::flight::ServerAuthHandler>",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_SERVER_AUTH_HANDLER, spec);
}


G_END_DECLS
namespace gaflight {
  class ServerCustomAuthHandler : public arrow::flight::ServerAuthHandler {
  public:
    explicit ServerCustomAuthHandler(GAFlightServerCustomAuthHandler *handler)
      : arrow::flight::ServerAuthHandler(),
        handler_(handler) {
    }

    arrow::Status
    Authenticate(const arrow::flight::ServerCallContext &context,
                 arrow::flight::ServerAuthSender *sender,
                 arrow::flight::ServerAuthReader *reader) override {
      auto klass = GAFLIGHT_SERVER_CUSTOM_AUTH_HANDLER_GET_CLASS(handler_);
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto gasender = gaflight_server_auth_sender_new_raw(sender);
      auto gareader = gaflight_server_auth_reader_new_raw(reader);
      GError *error = nullptr;
      klass->authenticate(handler_, gacontext, gasender, gareader, &error);
      g_object_unref(gareader);
      g_object_unref(gasender);
      g_object_unref(gacontext);
      if (error) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::Invalid,
                                      "[flight-server-custom-auth-handler]"
                                      "[authenticate]");
      } else {
        return arrow::Status::OK();
      }
    }

    arrow::Status
    IsValid(const arrow::flight::ServerCallContext &context,
            const std::string &token,
            std::string *peer_identity) override {
      auto klass = GAFLIGHT_SERVER_CUSTOM_AUTH_HANDLER_GET_CLASS(handler_);
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto gtoken = g_bytes_new_static(token.data(), token.size());
      GBytes *gpeer_identity = nullptr;
      GError *error = nullptr;
      klass->is_valid(handler_, gacontext, gtoken, &gpeer_identity, &error);
      g_bytes_unref(gtoken);
      g_object_unref(gacontext);
      if (gpeer_identity) {
        gsize gpeer_identity_size;
        auto gpeer_identity_data = g_bytes_get_data(gpeer_identity,
                                                    &gpeer_identity_size);
        *peer_identity =
          std::string(static_cast<const char *>(gpeer_identity_data),
                      gpeer_identity_size);
        g_bytes_unref(gpeer_identity);
      }
      if (error) {
        return garrow_error_to_status(error,
                                      arrow::StatusCode::Invalid,
                                      "[flight-server-custom-auth-handler]"
                                      "[is-valid]");
      } else {
        return arrow::Status::OK();
      }
    }

  private:
    GAFlightServerCustomAuthHandler *handler_;
  };
}
G_BEGIN_DECLS

G_DEFINE_TYPE(GAFlightServerCustomAuthHandler,
              gaflight_server_custom_auth_handler,
              GAFLIGHT_TYPE_SERVER_AUTH_HANDLER)

static void
gaflight_server_custom_auth_handler_init(GAFlightServerCustomAuthHandler *object)
{
  auto priv = GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(object);
  priv->handler = std::make_shared<gaflight::ServerCustomAuthHandler>(object);
}

static void
gaflight_server_custom_auth_handler_class_init(
  GAFlightServerCustomAuthHandlerClass *klass)
{
}

/**
 * gaflight_server_custom_auth_handler_authenticate:
 * @handler: A #GAFlightServerCustomAuthHandler.
 * @context: A #GAFlightServerCallContext.
 * @sender: A #GAFlightServerAuthSender.
 * @reader: A #GAFlightServerAuthReader.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Authenticates the client on initial connection. The server can send
 * and read responses from the client at any time.
 *
 * Since: 12.0.0
 */
void
gaflight_server_custom_auth_handler_authenticate(
  GAFlightServerCustomAuthHandler *handler,
  GAFlightServerCallContext *context,
  GAFlightServerAuthSender *sender,
  GAFlightServerAuthReader *reader,
  GError **error)
{
  auto flight_handler =
    gaflight_server_auth_handler_get_raw(
      GAFLIGHT_SERVER_AUTH_HANDLER(handler));
  auto flight_context = gaflight_server_call_context_get_raw(context);
  auto flight_sender = gaflight_server_auth_sender_get_raw(sender);
  auto flight_reader = gaflight_server_auth_reader_get_raw(reader);
  auto status = flight_handler->Authenticate(*flight_context,
                                             flight_sender,
                                             flight_reader);
  garrow::check(error,
                status,
                "[flight-server-custom-auth-handler][authenticate]");
}

/**
 * gaflight_server_custom_auth_handler_is_valid:
 * @handler: A #GAFlightServerCustomAuthHandler.
 * @context: A #GAFlightServerCallContext.
 * @token: The client token. May be the empty string if the client does not
 *   provide a token.
 * @peer_identity: (out): The identity of the peer, if this authentication
 *   method supports it.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Validates a per-call client token.
 *
 * Since: 12.0.0
 */
void
gaflight_server_custom_auth_handler_is_valid(
  GAFlightServerCustomAuthHandler *handler,
  GAFlightServerCallContext *context,
  GBytes *token,
  GBytes **peer_identity,
  GError **error)
{
  auto flight_handler =
    gaflight_server_auth_handler_get_raw(
      GAFLIGHT_SERVER_AUTH_HANDLER(handler));
  gsize token_size;
  auto token_data = g_bytes_get_data(token, &token_size);
  auto flight_context = gaflight_server_call_context_get_raw(context);
  std::string flight_token(static_cast<const char *>(token_data), token_size);
  std::string flight_peer_identity;
  auto status = flight_handler->IsValid(*flight_context,
                                        flight_token,
                                        &flight_peer_identity);
  if (garrow::check(error,
                    status,
                    "[flight-server-custom-auth-handler]"
                    "[is-valid]")) {
    *peer_identity = g_bytes_new(flight_peer_identity.data(),
                                 flight_peer_identity.size());
  }
}


typedef struct GAFlightServerOptionsPrivate_ {
  arrow::flight::FlightServerOptions options;
  GAFlightLocation *location;
  GAFlightServerAuthHandler *auth_handler;
} GAFlightServerOptionsPrivate;

enum {
  PROP_LOCATION = 1,
  PROP_AUTH_HANDLER,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightServerOptions,
                           gaflight_server_options,
                           G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(obj)        \
  static_cast<GAFlightServerOptionsPrivate *>(          \
    gaflight_server_options_get_instance_private(       \
      GAFLIGHT_SERVER_OPTIONS(obj)))

static void
gaflight_server_options_dispose(GObject *object)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(object);

  if (priv->location) {
    g_object_unref(priv->location);
    priv->location = nullptr;
  }

  if (priv->auth_handler) {
    g_object_unref(priv->auth_handler);
    priv->auth_handler = nullptr;
  }

  G_OBJECT_CLASS(gaflight_server_options_parent_class)->dispose(object);
}

static void
gaflight_server_options_finalize(GObject *object)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(object);

  priv->options.~FlightServerOptions();

  G_OBJECT_CLASS(gaflight_server_options_parent_class)->finalize(object);
}

static void
gaflight_server_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_LOCATION:
    {
      priv->location = GAFLIGHT_LOCATION(g_value_dup_object(value));
      auto flight_location = gaflight_location_get_raw(priv->location);
      new(&(priv->options)) arrow::flight::FlightServerOptions(*flight_location);
    }
    break;
  case PROP_AUTH_HANDLER:
    if (priv->auth_handler != g_value_get_object(value)) {
      if (priv->auth_handler) {
        g_object_unref(priv->auth_handler);
      }
      priv->auth_handler =
        GAFLIGHT_SERVER_AUTH_HANDLER(g_value_dup_object(value));
      if (priv->auth_handler) {
        priv->options.auth_handler =
          gaflight_server_auth_handler_get_raw(priv->auth_handler);
      } else {
        priv->options.auth_handler = nullptr;
      }
    }
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_LOCATION:
    g_value_set_object(value, priv->location);
    break;
  case PROP_AUTH_HANDLER:
    g_value_set_object(value, priv->auth_handler);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_server_options_init(GAFlightServerOptions *object)
{
}

static void
gaflight_server_options_class_init(GAFlightServerOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflight_server_options_dispose;
  gobject_class->finalize = gaflight_server_options_finalize;
  gobject_class->set_property = gaflight_server_options_set_property;
  gobject_class->get_property = gaflight_server_options_get_property;

  GParamSpec *spec;
  /**
   * GAFlightServerOptions:location:
   *
   * The location to be listened.
   *
   * Since: 5.0.0
   */
  spec = g_param_spec_object("location",
                             "Location",
                             "The location to be listened",
                             GAFLIGHT_TYPE_LOCATION,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_LOCATION, spec);

  /**
   * GAFlightServerOptions:auth-handler:
   *
   * The authentication handler.
   *
   * Since: 12.0.0
   */
  spec = g_param_spec_object("auth-handler",
                             "Authentication handler",
                             "The authentication handler",
                             GAFLIGHT_TYPE_SERVER_AUTH_HANDLER,
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class, PROP_AUTH_HANDLER, spec);
}

/**
 * gaflight_server_options_new:
 * @location: A #GAFlightLocation to be listened.
 *
 * Returns: The newly created options for a server.
 *
 * Since: 5.0.0
 */
GAFlightServerOptions *
gaflight_server_options_new(GAFlightLocation *location)
{
  return static_cast<GAFlightServerOptions *>(
    g_object_new(GAFLIGHT_TYPE_SERVER_OPTIONS,
                 "location", location,
                 NULL));
}


G_DEFINE_INTERFACE(GAFlightServable,
                   gaflight_servable,
                   G_TYPE_OBJECT)

static void
gaflight_servable_default_init(GAFlightServableInterface *iface)
{
}


G_END_DECLS
namespace gaflight {
  class Server : public arrow::flight::FlightServerBase {
  public:
    explicit Server(GAFlightServer *gaserver) :
      FlightServerBase(),
      gaserver_(gaserver) {
    }

    ~Server() override = default;

    arrow::Status
    ListFlights(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::Criteria *criteria,
      std::unique_ptr<arrow::flight::FlightListing> *listing) override {
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      GAFlightCriteria *gacriteria = nullptr;
      if (criteria) {
        gacriteria = gaflight_criteria_new_raw(criteria);
      }
      GError *gerror = nullptr;
      auto gaflights = gaflight_server_list_flights(gaserver_,
                                                    gacontext,
                                                    gacriteria,
                                                    &gerror);
      if (gacriteria) {
        g_object_unref(gacriteria);
      }
      g_object_unref(gacontext);
      if (gerror) {
        return garrow_error_to_status(gerror,
                                      arrow::StatusCode::UnknownError,
                                      "[flight-server][list-flights]");
      }
      std::vector<arrow::flight::FlightInfo> flights;
      for (auto node = gaflights; node; node = node->next) {
        auto gaflight = GAFLIGHT_INFO(node->data);
        flights.push_back(*gaflight_info_get_raw(gaflight));
        g_object_unref(gaflight);
      }
      g_list_free(gaflights);
      *listing = std::make_unique<
        arrow::flight::SimpleFlightListing>(flights);
      return arrow::Status::OK();
    }

    arrow::Status
    GetFlightInfo(const arrow::flight::ServerCallContext &context,
                  const arrow::flight::FlightDescriptor &request,
                  std::unique_ptr<arrow::flight::FlightInfo> *info) override {
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto garequest = gaflight_descriptor_new_raw(&request);
      GError *gerror = nullptr;
      auto gainfo = gaflight_server_get_flight_info(gaserver_,
                                                    gacontext,
                                                    garequest,
                                                    &gerror);
      g_object_unref(garequest);
      g_object_unref(gacontext);
      if (gerror) {
        return garrow_error_to_status(gerror,
                                      arrow::StatusCode::UnknownError,
                                      "[flight-server][get-flight-info]");
      }
      *info = std::make_unique<arrow::flight::FlightInfo>(
        *gaflight_info_get_raw(gainfo));
      g_object_unref(gainfo);
      return arrow::Status::OK();
    }

    arrow::Status DoGet(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::Ticket &ticket,
      std::unique_ptr<arrow::flight::FlightDataStream> *stream) override {
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto gaticket = gaflight_ticket_new_raw(&ticket);
      GError *gerror = nullptr;
      auto gastream = gaflight_server_do_get(gaserver_,
                                             gacontext,
                                             gaticket,
                                             &gerror);
      g_object_unref(gaticket);
      g_object_unref(gacontext);
      if (gerror) {
        return garrow_error_to_status(gerror,
                                      arrow::StatusCode::UnknownError,
                                      "[flight-server][do-get]");
      }
      *stream = std::make_unique<DataStream>(gastream);
      return arrow::Status::OK();
    }

  private:
    GAFlightServer *gaserver_;
  };
};
G_BEGIN_DECLS

struct GAFlightServerPrivate {
  gaflight::Server server;
};

G_END_DECLS
static arrow::flight::FlightServerBase *
gaflight_server_servable_get_raw(GAFlightServable *servable);
G_BEGIN_DECLS

static void
gaflight_server_servable_interface_init(GAFlightServableInterface *iface)
{
  iface->get_raw = gaflight_server_servable_get_raw;
}

G_DEFINE_ABSTRACT_TYPE_WITH_CODE(GAFlightServer,
                                 gaflight_server,
                                 G_TYPE_OBJECT,
                                 G_ADD_PRIVATE(GAFlightServer);
                                 G_IMPLEMENT_INTERFACE(
                                   GAFLIGHT_TYPE_SERVABLE,
                                   gaflight_server_servable_interface_init))

#define GAFLIGHT_SERVER_GET_PRIVATE(object)         \
  static_cast<GAFlightServerPrivate *>(             \
    gaflight_server_get_instance_private(           \
      GAFLIGHT_SERVER(object)))

G_END_DECLS
static arrow::flight::FlightServerBase *
gaflight_server_servable_get_raw(GAFlightServable *servable)
{
  auto priv = GAFLIGHT_SERVER_GET_PRIVATE(servable);
  return &(priv->server);
}
G_BEGIN_DECLS

static void
gaflight_server_finalize(GObject *object)
{
  auto priv = GAFLIGHT_SERVER_GET_PRIVATE(object);
  priv->server.~Server();

  G_OBJECT_CLASS(gaflight_server_parent_class)->finalize(object);
}

static void
gaflight_server_init(GAFlightServer *object)
{
  auto priv = GAFLIGHT_SERVER_GET_PRIVATE(object);
  new(&(priv->server)) gaflight::Server(object);
}

static void
gaflight_server_class_init(GAFlightServerClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gaflight_server_finalize;
}

/**
 * gaflight_server_listen:
 * @server: A #GAFlightServer.
 * @options: A #GAFlightServerOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_server_listen(GAFlightServer *server,
                       GAFlightServerOptions *options,
                       GError **error)
{
  auto flight_server = gaflight_servable_get_raw(GAFLIGHT_SERVABLE(server));
  const auto flight_options = gaflight_server_options_get_raw(options);
  return garrow::check(error,
                       flight_server->Init(*flight_options),
                       "[flight-server][listen]");
}

/**
 * gaflight_server_new:
 * @server: A #GAFlightServer.
 *
 * Returns: The port number listening.
 *
 * Since: 5.0.0
 */
gint
gaflight_server_get_port(GAFlightServer *server)
{
  const auto flight_server =
    gaflight_servable_get_raw(GAFLIGHT_SERVABLE(server));
  return flight_server->port();
}

/**
 * gaflight_server_shutdown:
 * @server: A #GAFlightServer.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Shuts down the serve. This function can be called from signal
 * handler or another thread.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_server_shutdown(GAFlightServer *server,
                         GError **error)
{
  auto flight_server = gaflight_servable_get_raw(GAFLIGHT_SERVABLE(server));
  return garrow::check(error,
                       flight_server->Shutdown(),
                       "[flight-server][shutdown]");
}

/**
 * gaflight_server_list_flights:
 * @server: A #GAFlightServer.
 * @context: A #GAFlightServerCallContext.
 * @criteria: (nullable): A #GAFlightCriteria.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (element-type GAFlightInfo) (transfer full):
 *   #GList of #GAFlightInfo on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GList *
gaflight_server_list_flights(GAFlightServer *server,
                             GAFlightServerCallContext *context,
                             GAFlightCriteria *criteria,
                             GError **error)
{
  auto klass = GAFLIGHT_SERVER_GET_CLASS(server);
  if (!(klass && klass->list_flights)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "not implemented");
    return nullptr;
  }
  return (*(klass->list_flights))(server, context, criteria, error);
}

/**
 * gaflight_server_get_flight_info:
 * @server: A #GAFlightServer.
 * @context: A #GAFlightServerCallContext.
 * @request: A #GAFlightDescriptor.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): A #GAFlightInfo on success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightInfo *
gaflight_server_get_flight_info(GAFlightServer *server,
                                GAFlightServerCallContext *context,
                                GAFlightDescriptor *request,
                                GError **error)
{
  auto klass = GAFLIGHT_SERVER_GET_CLASS(server);
  if (!(klass && klass->get_flight_info)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "not implemented");
    return nullptr;
  }
  return (*(klass->get_flight_info))(server, context, request, error);
}

/**
 * gaflight_server_do_get:
 * @server: A #GAFlightServer.
 * @context: A #GAFlightServerCallContext.
 * @ticket: A #GAFlightTicket.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (transfer full): #GAFlightDataStream on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GAFlightDataStream *
gaflight_server_do_get(GAFlightServer *server,
                       GAFlightServerCallContext *context,
                       GAFlightTicket *ticket,
                       GError **error)
{
  auto klass = GAFLIGHT_SERVER_GET_CLASS(server);
  if (!(klass && klass->do_get)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "not implemented");
    return nullptr;
  }
  return (*(klass->do_get))(server, context, ticket, error);
}


G_END_DECLS


arrow::flight::FlightDataStream *
gaflight_data_stream_get_raw(GAFlightDataStream *stream)
{
  auto priv = GAFLIGHT_DATA_STREAM_GET_PRIVATE(stream);
  return priv->stream;
}

GAFlightServerCallContext *
gaflight_server_call_context_new_raw(
  const arrow::flight::ServerCallContext *flight_call_context)
{
  return GAFLIGHT_SERVER_CALL_CONTEXT(
    g_object_new(GAFLIGHT_TYPE_SERVER_CALL_CONTEXT,
                 "call-context", flight_call_context,
                 NULL));
}

const arrow::flight::ServerCallContext *
gaflight_server_call_context_get_raw(GAFlightServerCallContext *call_context)
{
  auto priv = GAFLIGHT_SERVER_CALL_CONTEXT_GET_PRIVATE(call_context);
  return priv->call_context;
}

GAFlightServerAuthSender *
gaflight_server_auth_sender_new_raw(
  arrow::flight::ServerAuthSender *flight_sender)
{
  return GAFLIGHT_SERVER_AUTH_SENDER(
    g_object_new(GAFLIGHT_TYPE_SERVER_AUTH_SENDER,
                 "sender", flight_sender,
                 nullptr));
}

arrow::flight::ServerAuthSender *
gaflight_server_auth_sender_get_raw(GAFlightServerAuthSender *sender)
{
  auto priv = GAFLIGHT_SERVER_AUTH_SENDER_GET_PRIVATE(sender);
  return priv->sender;
}

GAFlightServerAuthReader *
gaflight_server_auth_reader_new_raw(
  arrow::flight::ServerAuthReader *flight_reader)
{
  return GAFLIGHT_SERVER_AUTH_READER(
    g_object_new(GAFLIGHT_TYPE_SERVER_AUTH_READER,
                 "reader", flight_reader,
                 nullptr));
}

arrow::flight::ServerAuthReader *
gaflight_server_auth_reader_get_raw(GAFlightServerAuthReader *reader)
{
  auto priv = GAFLIGHT_SERVER_AUTH_READER_GET_PRIVATE(reader);
  return priv->reader;
}

std::shared_ptr<arrow::flight::ServerAuthHandler>
gaflight_server_auth_handler_get_raw(GAFlightServerAuthHandler *handler)
{
  auto priv = GAFLIGHT_SERVER_AUTH_HANDLER_GET_PRIVATE(handler);
  return priv->handler;
}

arrow::flight::FlightServerOptions *
gaflight_server_options_get_raw(GAFlightServerOptions *options)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}

arrow::flight::FlightServerBase *
gaflight_servable_get_raw(GAFlightServable *servable)
{
  auto *iface = GAFLIGHT_SERVABLE_GET_IFACE(servable);
  return iface->get_raw(servable);
}
