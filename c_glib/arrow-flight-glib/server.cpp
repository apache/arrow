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

#include <arrow/util/make_unique.h>

#include <arrow-glib/error.hpp>

#include <arrow-flight-glib/common.hpp>
#include <arrow-flight-glib/server.hpp>

G_BEGIN_DECLS

/**
 * SECTION: server
 * @section_id: server
 * @title: Server related classes
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightServerOptions is a class for options of each server.
 *
 * #GAFlightServer is a class to develop an Apache Arrow Flight server.
 *
 * Since: 5.0.0
 */


typedef struct GAFlightServerOptionsPrivate_ {
  arrow::flight::FlightServerOptions options;
  GAFlightLocation *location;
} GAFlightServerOptionsPrivate;

enum {
  PROP_LOCATION = 1,
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
    priv->location = NULL;
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
  spec = g_param_spec_object("location",
                             "Location",
                             "The location to be listened",
                             GAFLIGHT_TYPE_LOCATION,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_LOCATION, spec);
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


G_END_DECLS
namespace gaflight {
  class Server : public arrow::flight::FlightServerBase {
  public:
    Server(GAFlightServer *gaserver) : gaserver_(gaserver) {
    }

    arrow::Status ListFlights(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::Criteria *criteria,
      std::unique_ptr<arrow::flight::FlightListing> *listing) override {
      GError *gerror = NULL;
      auto gaflights = gaflight_server_list_flights(gaserver_,
                                                    NULL,
                                                    NULL,
                                                    &gerror);
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
      *listing = arrow::internal::make_unique<
        arrow::flight::SimpleFlightListing>(flights);
      return arrow::Status::OK();
    }

  private:
    GAFlightServer *gaserver_;
  };
};
G_BEGIN_DECLS

typedef struct GAFlightServerPrivate_ {
  gaflight::Server server;
} GAFlightServerPrivate;

G_DEFINE_ABSTRACT_TYPE_WITH_PRIVATE(GAFlightServer,
                                    gaflight_server,
                                    G_TYPE_OBJECT)

#define GAFLIGHT_SERVER_GET_PRIVATE(obj)         \
  static_cast<GAFlightServerPrivate *>(          \
    gaflight_server_get_instance_private(        \
      GAFLIGHT_SERVER(obj)))

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
  auto flight_server = gaflight_server_get_raw(server);
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
  const auto flight_server = gaflight_server_get_raw(server);
  return flight_server->port();
}

/**
 * gaflight_server_shutdown:
 * @server: A #GAFlightServer.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Shuts down the serve. This function can be called from signal
 * handler or another thread while gaflight_server_serve() blocks.
 *
 * Returns: %TRUE on success, %FALSE on error.
 *
 * Since: 5.0.0
 */
gboolean
gaflight_server_shutdown(GAFlightServer *server,
                         GError **error)
{
  auto flight_server = gaflight_server_get_raw(server);
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
 * Shuts down the serve. This function can be called from signal
 * handler or another thread while gaflight_server_serve() blocks.
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
    return NULL;
  }
  return (*(klass->list_flights))(server, context, criteria, error);
}


G_END_DECLS


arrow::flight::FlightServerOptions *
gaflight_server_options_get_raw(GAFlightServerOptions *options)
{
  auto priv = GAFLIGHT_SERVER_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}

arrow::flight::FlightServerBase *
gaflight_server_get_raw(GAFlightServer *server)
{
  auto priv = GAFLIGHT_SERVER_GET_PRIVATE(server);
  return &(priv->server);
}
