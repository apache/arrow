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

#include <arrow-glib/error.hpp>

#include <arrow-flight-glib/client.hpp>
#include <arrow-flight-glib/common.hpp>

G_BEGIN_DECLS

/**
 * SECTION: client
 * @section_id: client
 * @title: Client related classes
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightCallOptions is a class for options of each call.
 *
 * #GAFlightClientOptions is a class for options of each client.
 *
 * #GAFlightClient is a class for Apache Arrow Flight client.
 *
 * Since: 5.0.0
 */

typedef struct GAFlightCallOptionsPrivate_ {
  arrow::flight::FlightCallOptions options;
} GAFlightCallOptionsPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightCallOptions,
                           gaflight_call_options,
                           G_TYPE_OBJECT)

#define GAFLIGHT_CALL_OPTIONS_GET_PRIVATE(obj)        \
  static_cast<GAFlightCallOptionsPrivate *>(          \
    gaflight_call_options_get_instance_private(       \
      GAFLIGHT_CALL_OPTIONS(obj)))

static void
gaflight_call_options_finalize(GObject *object)
{
  auto priv = GAFLIGHT_CALL_OPTIONS_GET_PRIVATE(object);

  priv->options.~FlightCallOptions();

  G_OBJECT_CLASS(gaflight_call_options_parent_class)->finalize(object);
}

static void
gaflight_call_options_init(GAFlightCallOptions *object)
{
  auto priv = GAFLIGHT_CALL_OPTIONS_GET_PRIVATE(object);
  new(&priv->options) arrow::flight::FlightCallOptions;
}

static void
gaflight_call_options_class_init(GAFlightCallOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_call_options_finalize;
}

/**
 * gaflight_call_options_new:
 *
 * Returns: The newly created options for a call.
 *
 * Since: 5.0.0
 */
GAFlightCallOptions *
gaflight_call_options_new(void)
{
  return static_cast<GAFlightCallOptions *>(
    g_object_new(GAFLIGHT_TYPE_CALL_OPTIONS, NULL));
}


typedef struct GAFlightClientOptionsPrivate_ {
  arrow::flight::FlightClientOptions options;
} GAFlightClientOptionsPrivate;

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightClientOptions,
                           gaflight_client_options,
                           G_TYPE_OBJECT)

#define GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(obj)        \
  static_cast<GAFlightClientOptionsPrivate *>(          \
    gaflight_client_options_get_instance_private(       \
      GAFLIGHT_CLIENT_OPTIONS(obj)))

static void
gaflight_client_options_finalize(GObject *object)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(object);

  priv->options.~FlightClientOptions();

  G_OBJECT_CLASS(gaflight_client_options_parent_class)->finalize(object);
}

static void
gaflight_client_options_init(GAFlightClientOptions *object)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(object);
  new(&(priv->options)) arrow::flight::FlightClientOptions;
  priv->options = arrow::flight::FlightClientOptions::Defaults();
}

static void
gaflight_client_options_class_init(GAFlightClientOptionsClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_client_options_finalize;
}

/**
 * gaflight_client_options_new:
 *
 * Returns: The newly created options for a client.
 *
 * Since: 5.0.0
 */
GAFlightClientOptions *
gaflight_client_options_new(void)
{
  return static_cast<GAFlightClientOptions *>(
    g_object_new(GAFLIGHT_TYPE_CLIENT_OPTIONS, NULL));
}


typedef struct GAFlightClientPrivate_ {
  arrow::flight::FlightClient *client;
} GAFlightClientPrivate;

enum {
  PROP_CLIENT = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightClient,
                           gaflight_client,
                           G_TYPE_OBJECT)

#define GAFLIGHT_CLIENT_GET_PRIVATE(obj)         \
  static_cast<GAFlightClientPrivate *>(          \
    gaflight_client_get_instance_private(        \
      GAFLIGHT_CLIENT(obj)))

static void
gaflight_client_finalize(GObject *object)
{
  auto priv = GAFLIGHT_CLIENT_GET_PRIVATE(object);

  delete priv->client;

  G_OBJECT_CLASS(gaflight_client_parent_class)->finalize(object);
}

static void
gaflight_client_set_property(GObject *object,
                             guint prop_id,
                             const GValue *value,
                             GParamSpec *pspec)
{
  auto priv = GAFLIGHT_CLIENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CLIENT:
    priv->client =
      static_cast<arrow::flight::FlightClient *>(g_value_get_pointer(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_client_init(GAFlightClient *object)
{
}

static void
gaflight_client_class_init(GAFlightClientClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->finalize = gaflight_client_finalize;
  gobject_class->set_property = gaflight_client_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("client",
                              "Client",
                              "The raw arrow::flight::FlightClient *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CLIENT, spec);
}

/**
 * gaflight_client_new:
 * @location: A #GAFlightLocation to be connected.
 * @options: (nullable): A #GAFlightClientOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable): The newly created client, %NULL on error.
 *
 * Since: 5.0.0
 */
GAFlightClient *
gaflight_client_new(GAFlightLocation *location,
                    GAFlightClientOptions *options,
                    GError **error)
{
  const auto flight_location = gaflight_location_get_raw(location);
  std::unique_ptr<arrow::flight::FlightClient> flight_client;
  arrow::Status status;
  if (options) {
    const auto flight_options = gaflight_client_options_get_raw(options);
    status = arrow::flight::FlightClient::Connect(*flight_location,
                                                  *flight_options,
                                                  &flight_client);
  } else {
    status = arrow::flight::FlightClient::Connect(*flight_location,
                                                  &flight_client);
  }
  if (garrow::check(error, status, "[flight-client][new]")) {
    return gaflight_client_new_raw(flight_client.release());
  } else {
    return NULL;
  }
}


G_END_DECLS


arrow::flight::FlightClientOptions *
gaflight_client_options_get_raw(GAFlightClientOptions *options)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}

arrow::flight::FlightClient *
gaflight_client_get_raw(GAFlightClient *client)
{
  auto priv = GAFLIGHT_CLIENT_GET_PRIVATE(client);
  return priv->client;
}

GAFlightClient *
gaflight_client_new_raw(arrow::flight::FlightClient *flight_client)
{
  return GAFLIGHT_CLIENT(g_object_new(GAFLIGHT_TYPE_CLIENT,
                                      "client", flight_client,
                                      NULL));
}
