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

#include <arrow-glib/arrow-glib.hpp>
#include <arrow-flight-glib/arrow-flight-glib.hpp>

#include <arrow-flight-sql-glib/client.hpp>

G_BEGIN_DECLS

/**
 * SECTION: client
 * @section_id: client
 * @title: Client related classes
 * @include: arrow-flight-sql-glib/arrow-flight-sql-glib.h
 *
 * #GAFlightSQLClient is a class for Apache Arrow Flight SQL client.
 *
 * Since: 9.0.0
 */

struct GAFlightSQLClientPrivate {
  arrow::flight::sql::FlightSqlClient* client;
  GAFlightClient *flight_client;
};

enum {
  PROP_CLIENT = 1,
  PROP_FLIGHT_CLIENT,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightSQLClient,
                           gaflightsql_client,
                           G_TYPE_OBJECT)

#define GAFLIGHTSQL_CLIENT_GET_PRIVATE(object)      \
  static_cast<GAFlightSQLClientPrivate *>(          \
    gaflightsql_client_get_instance_private(        \
      GAFLIGHTSQL_CLIENT(object)))

static void
gaflightsql_client_dispose(GObject *object)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(object);

  if (priv->flight_client) {
    g_object_unref(priv->flight_client);
    priv->flight_client = nullptr;
  }

  G_OBJECT_CLASS(gaflightsql_client_parent_class)->dispose(object);
}

static void
gaflightsql_client_finalize(GObject *object)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(object);

  delete priv->client;

  G_OBJECT_CLASS(gaflightsql_client_parent_class)->finalize(object);
}

static void
gaflightsql_client_set_property(GObject *object,
                                guint prop_id,
                                const GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_CLIENT:
    priv->client =
      static_cast<arrow::flight::sql::FlightSqlClient *>(
        g_value_get_pointer(value));
    break;
  case PROP_FLIGHT_CLIENT:
    priv->flight_client = GAFLIGHT_CLIENT(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflightsql_client_get_property(GObject *object,
                                guint prop_id,
                                GValue *value,
                                GParamSpec *pspec)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_FLIGHT_CLIENT:
    g_value_set_object(value, priv->flight_client);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflightsql_client_init(GAFlightSQLClient *object)
{
}

static void
gaflightsql_client_class_init(GAFlightSQLClientClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflightsql_client_dispose;
  gobject_class->finalize = gaflightsql_client_finalize;
  gobject_class->set_property = gaflightsql_client_set_property;
  gobject_class->get_property = gaflightsql_client_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("client",
                              "Client",
                              "The raw arrow::flight::sql::FlightSqlClient *",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_CLIENT, spec);

  /**
   * GAFlightSQLClient:flight-client:
   *
   * The underlying Flight client.
   *
   * Since: 9.0.0
   */
  spec = g_param_spec_object("flight-client",
                             "Flight client",
                             "The underlying Flight client",
                             GAFLIGHT_TYPE_CLIENT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_FLIGHT_CLIENT, spec);
}

/**
 * gaflightsql_client_new:
 * @client: A #GAFlightClient to be used.
 *
 * Returns:: The newly created Flight SQL client.
 *
 * Since: 9.0.0
 */
GAFlightSQLClient *
gaflightsql_client_new(GAFlightClient *client)
{
  auto flight_client = gaflight_client_get_raw(client);
  auto flight_sql_client =
    new arrow::flight::sql::FlightSqlClient(flight_client);
  return gaflightsql_client_new_raw(flight_sql_client, client);
}

/**
 * gaflightsql_client_execute:
 * @client: A #GAFlightSQLClient.
 * @query: A query to be executed in the UTF-8 format.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GAFlightInfo describing
 *   where to access the dataset on success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightInfo *
gaflightsql_client_execute(GAFlightSQLClient *client,
                           const gchar *query,
                           GAFlightCallOptions *options,
                           GError **error)
{
  auto flight_sql_client = gaflightsql_client_get_raw(client);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_sql_client->Execute(*flight_options, query);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-client][execute]")) {
    return nullptr;
  }
  auto flight_info = std::move(*result);
  return gaflight_info_new_raw(flight_info.release());
}

/**
 * gaflightsql_client_do_get:
 * @client: A #GAFlightClient.
 * @ticket: A #GAFlightTicket.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The #GAFlightStreamReader to read record batched from the server
 *   on success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightStreamReader *
gaflightsql_client_do_get(GAFlightSQLClient *client,
                          GAFlightTicket *ticket,
                          GAFlightCallOptions *options,
                          GError **error)
{
  auto flight_sql_client = gaflightsql_client_get_raw(client);
  const auto flight_ticket = gaflight_ticket_get_raw(ticket);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_sql_client->DoGet(*flight_options, *flight_ticket);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-client][do-get]")) {
    return nullptr;
  }
  auto flight_reader = std::move(*result);
  return gaflight_stream_reader_new_raw(flight_reader.release());
}


G_END_DECLS


arrow::flight::sql::FlightSqlClient *
gaflightsql_client_get_raw(GAFlightSQLClient *client)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(client);
  return priv->client;
}

GAFlightSQLClient *
gaflightsql_client_new_raw(
  arrow::flight::sql::FlightSqlClient *flight_sql_client,
  GAFlightClient *client)
{
  return GAFLIGHTSQL_CLIENT(
    g_object_new(GAFLIGHTSQL_TYPE_CLIENT,
                 "client", flight_sql_client,
                 "flight_client", client,
                 nullptr));
}
