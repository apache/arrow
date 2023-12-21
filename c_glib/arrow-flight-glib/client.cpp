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

#include <arrow-flight-glib/client.hpp>
#include <arrow-flight-glib/common.hpp>

G_BEGIN_DECLS

/**
 * SECTION: client
 * @section_id: client
 * @title: Client related classes
 * @include: arrow-flight-glib/arrow-flight-glib.h
 *
 * #GAFlightStreamReader is a class for reading record batches from a
 * server.
 *
 * #GAFlightCallOptions is a class for options of each call.
 *
 * #GAFlightClientOptions is a class for options of each client.
 *
 * #GAFlightClient is a class for Apache Arrow Flight client.
 *
 * Since: 5.0.0
 */

G_DEFINE_TYPE(GAFlightStreamReader,
              gaflight_stream_reader,
              GAFLIGHT_TYPE_RECORD_BATCH_READER)

static void
gaflight_stream_reader_init(GAFlightStreamReader *object)
{
}

static void
gaflight_stream_reader_class_init(GAFlightStreamReaderClass *klass)
{
}

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

/**
 * gaflight_call_options_add_header:
 * @options: A #GAFlightCallOptions.
 * @name: A header name.
 * @value: A header value.
 *
 * Add a header.
 *
 * Since: 9.0.0
 */
void
gaflight_call_options_add_header(GAFlightCallOptions *options,
                                 const gchar *name,
                                 const gchar *value)
{
  auto flight_options = gaflight_call_options_get_raw(options);
  flight_options->headers.emplace_back(name, value);
}

/**
 * gaflight_call_options_clear_headers:
 * @options: A #GAFlightCallOptions.
 *
 * Clear all headers.
 *
 * Since: 9.0.0
 */
void
gaflight_call_options_clear_headers(GAFlightCallOptions *options)
{
  auto flight_options = gaflight_call_options_get_raw(options);
  flight_options->headers.clear();
}

/**
 * gaflight_call_options_foreach_header:
 * @options: A #GAFlightCallOptions.
 * @func: (scope call): The user's callback function.
 * @user_data: (closure): Data for @func.
 *
 * Iterates over all headers in the options.
 *
 * Since: 9.0.0
 */
void
gaflight_call_options_foreach_header(GAFlightCallOptions *options,
                                     GAFlightHeaderFunc func,
                                     gpointer user_data)
{
  auto flight_options = gaflight_call_options_get_raw(options);
  for (const auto &header : flight_options->headers) {
    auto &key = header.first;
    auto &value = header.second;
    func(key.c_str(), value.c_str(), user_data);
  }
}


struct GAFlightClientOptionsPrivate {
  arrow::flight::FlightClientOptions options;
};

enum {
  PROP_TLS_ROOT_CERTIFICATES = 1,
  PROP_OVERRIDE_HOST_NAME,
  PROP_CERTIFICATE_CHAIN,
  PROP_PRIVATE_KEY,
  PROP_WRITE_SIZE_LIMIT_BYTES,
  PROP_DISABLE_SERVER_VERIFICATION,
};

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
gaflight_client_options_set_property(GObject *object,
                                     guint prop_id,
                                     const GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TLS_ROOT_CERTIFICATES:
    priv->options.tls_root_certs = g_value_get_string(value);
    break;
  case PROP_OVERRIDE_HOST_NAME:
    priv->options.override_hostname = g_value_get_string(value);
    break;
  case PROP_CERTIFICATE_CHAIN:
    priv->options.cert_chain = g_value_get_string(value);
    break;
  case PROP_PRIVATE_KEY:
    priv->options.private_key = g_value_get_string(value);
    break;
  case PROP_WRITE_SIZE_LIMIT_BYTES:
    priv->options.write_size_limit_bytes = g_value_get_int64(value);
    break;
  case PROP_DISABLE_SERVER_VERIFICATION:
    priv->options.disable_server_verification = g_value_get_boolean(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_client_options_get_property(GObject *object,
                                     guint prop_id,
                                     GValue *value,
                                     GParamSpec *pspec)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_TLS_ROOT_CERTIFICATES:
    g_value_set_string(value, priv->options.tls_root_certs.c_str());
    break;
  case PROP_OVERRIDE_HOST_NAME:
    g_value_set_string(value, priv->options.override_hostname.c_str());
    break;
  case PROP_CERTIFICATE_CHAIN:
    g_value_set_string(value, priv->options.cert_chain.c_str());
    break;
  case PROP_PRIVATE_KEY:
    g_value_set_string(value, priv->options.private_key.c_str());
    break;
  case PROP_WRITE_SIZE_LIMIT_BYTES:
    g_value_set_int64(value, priv->options.write_size_limit_bytes);
    break;
  case PROP_DISABLE_SERVER_VERIFICATION:
    g_value_set_boolean(value, priv->options.disable_server_verification);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
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
  gobject_class->set_property = gaflight_client_options_set_property;
  gobject_class->get_property = gaflight_client_options_get_property;

  auto options = arrow::flight::FlightClientOptions::Defaults();
  GParamSpec *spec;
  /**
   * GAFlightClientOptions:tls-root-certificates:
   *
   * Root certificates to use for validating server certificates.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_string("tls-root-certificates",
                             nullptr,
                             nullptr,
                             options.tls_root_certs.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_TLS_ROOT_CERTIFICATES,
                                  spec);

  /**
   * GAFlightClientOptions:override-host-name:
   *
   * Override the host name checked by TLS. Use with caution.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_string("override-host-name",
                             nullptr,
                             nullptr,
                             options.override_hostname.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_OVERRIDE_HOST_NAME,
                                  spec);

  /**
   * GAFlightClientOptions:certificate-chain:
   *
   * The client certificate to use if using Mutual TLS.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_string("certificate-chain",
                             nullptr,
                             nullptr,
                             options.cert_chain.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_CERTIFICATE_CHAIN,
                                  spec);

  /**
   * GAFlightClientOptions:private-key:
   *
   * The private key associated with the client certificate for Mutual
   * TLS.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_string("private-key",
                             nullptr,
                             nullptr,
                             options.private_key.c_str(),
                             static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_PRIVATE_KEY,
                                  spec);

  /**
   * GAFlightClientOptions:write-size-limit-bytes:
   *
   * A soft limit on the number of bytes to write in a single batch
   * when sending Arrow data to a server.
   *
   * Used to help limit server memory consumption. Only enabled if
   * positive. When enabled, @GARROW_ERROR_IO may be yielded.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_int64("write-size-limit-bytes",
                            nullptr,
                            nullptr,
                            INT64_MIN,
                            INT64_MAX,
                            options.write_size_limit_bytes,
                            static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_WRITE_SIZE_LIMIT_BYTES,
                                  spec);

  /**
   * GAFlightClientOptions:disable-server-verification:
   *
   * Whether use TLS without validating the server certificate. Use
   * with caution.
   *
   * Since: 9.0.0
   */
  spec = g_param_spec_boolean("disable-server-verification",
                              NULL,
                              NULL,
                              options.disable_server_verification,
                              static_cast<GParamFlags>(G_PARAM_READWRITE));
  g_object_class_install_property(gobject_class,
                                  PROP_DISABLE_SERVER_VERIFICATION,
                                  spec);
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


struct GAFlightClientPrivate {
  std::shared_ptr<arrow::flight::FlightClient> client;
};

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

  priv->client.~shared_ptr();

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
      *(static_cast<std::shared_ptr<arrow::flight::FlightClient> *>(
          g_value_get_pointer(value)));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflight_client_init(GAFlightClient *object)
{
  auto priv = GAFLIGHT_CLIENT_GET_PRIVATE(object);
  new(&priv->client) std::shared_ptr<arrow::flight::FlightClient>;
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
                              "The raw std::shared_ptr<arrow::flight::FlightClient>",
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
  arrow::Result<std::unique_ptr<arrow::flight::FlightClient>> result;
  if (options) {
    const auto flight_options = gaflight_client_options_get_raw(options);
    result = arrow::flight::FlightClient::Connect(*flight_location, *flight_options);
  } else {
    result = arrow::flight::FlightClient::Connect(*flight_location);
  }
  if (garrow::check(error, result, "[flight-client][new]")) {
    std::shared_ptr<arrow::flight::FlightClient> flight_client =
      std::move(*result);
    return gaflight_client_new_raw(&flight_client);
  } else {
    return NULL;
  }
}

/**
 * gaflight_client_close:
 * @client: A #GAFlightClient.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 8.0.0
 */
gboolean
gaflight_client_close(GAFlightClient *client,
                      GError **error)
{
  auto flight_client = gaflight_client_get_raw(client);
  auto status = flight_client->Close();
  return garrow::check(error,
                       status,
                       "[flight-client][close]");
}

/**
 * gaflight_client_authenticate_basic_token:
 * @client: A #GAFlightClient.
 * @user: User name to be used.
 * @password: Password to be used.
 * @options: (nullable): A #GAFlightCallOptions.
 * @bearer_name: (out) (transfer full): Bearer token name on success.
 * @bearer_value: (out) (transfer full): Bearer token value on success.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Authenticates to the server using basic HTTP style authentication.
 *
 * Returns: %TRUE on success, %FALSE if there was an error.
 *
 * Since: 12.0.0
 */
gboolean
gaflight_client_authenticate_basic_token(GAFlightClient *client,
                                         const gchar *user,
                                         const gchar *password,
                                         GAFlightCallOptions *options,
                                         gchar **bearer_name,
                                         gchar **bearer_value,
                                         GError **error)
{
  auto flight_client = gaflight_client_get_raw(client);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_client->AuthenticateBasicToken(*flight_options,
                                                      user,
                                                      password);
  if (!garrow::check(error,
                     result,
                     "[flight-client][authenticate-basic-token]")) {
    return FALSE;
  }
  auto bearer_token = *result;
  *bearer_name = g_strndup(bearer_token.first.data(),
                           bearer_token.first.size());
  *bearer_value = g_strndup(bearer_token.second.data(),
                            bearer_token.second.size());
  return TRUE;
}

/**
 * gaflight_client_list_flights:
 * @client: A #GAFlightClient.
 * @criteria: (nullable): A #GAFlightCriteria.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (element-type GAFlightInfo) (transfer full):
 *   The returned list of #GAFlightInfo on success, %NULL on error.
 *
 * Since: 5.0.0
 */
GList *
gaflight_client_list_flights(GAFlightClient *client,
                             GAFlightCriteria *criteria,
                             GAFlightCallOptions *options,
                             GError **error)
{
  auto flight_client = gaflight_client_get_raw(client);
  arrow::flight::Criteria flight_default_criteria;
  auto flight_criteria = &flight_default_criteria;
  if (criteria) {
    flight_criteria = gaflight_criteria_get_raw(criteria);
  }
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  std::unique_ptr<arrow::flight::FlightListing> flight_listing;
  auto result = flight_client->ListFlights(*flight_options, *flight_criteria);
  auto status = std::move(result).Value(&flight_listing);
  if (!garrow::check(error,
                     status,
                     "[flight-client][list-flights]")) {
    return NULL;
  }
  GList *listing = NULL;
  std::unique_ptr<arrow::flight::FlightInfo> flight_info;
  while (true) {
    status = flight_listing->Next().Value(&flight_info);
    if (!garrow::check(error,
                       status,
                       "[flight-client][list-flights]")) {
      g_list_free_full(listing, g_object_unref);
      return NULL;
    }
    if (!flight_info) {
      break;
    }
    auto info = gaflight_info_new_raw(flight_info.release());
    listing = g_list_prepend(listing, info);
  }
  return g_list_reverse(listing);
}

/**
 * gaflight_client_get_flight_info:
 * @client: A #GAFlightClient.
 * @descriptor: A #GAFlightDescriptor to be processed.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The returned #GAFlightInfo on
 *   success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightInfo *
gaflight_client_get_flight_info(GAFlightClient *client,
                                GAFlightDescriptor *descriptor,
                                GAFlightCallOptions *options,
                                GError **error)
{
  auto flight_client = gaflight_client_get_raw(client);
  auto flight_descriptor = gaflight_descriptor_get_raw(descriptor);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_client->GetFlightInfo(*flight_options,
                                             *flight_descriptor);
  if (!garrow::check(error, result, "[flight-client][get-flight-info]")) {
    return NULL;
  }
  auto flight_info = std::move(*result);
  return gaflight_info_new_raw(flight_info.release());
}

/**
 * gaflight_client_do_get:
 * @client: A #GAFlightClient.
 * @ticket: A #GAFlightTicket.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full):
 *   The #GAFlightStreamReader to read record batched from the server
 *   on success, %NULL on error.
 *
 * Since: 6.0.0
 */
GAFlightStreamReader *
gaflight_client_do_get(GAFlightClient *client,
                       GAFlightTicket *ticket,
                       GAFlightCallOptions *options,
                       GError **error)
{
  auto flight_client = gaflight_client_get_raw(client);
  const auto flight_ticket = gaflight_ticket_get_raw(ticket);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_client->DoGet(*flight_options, *flight_ticket);
  if (!garrow::check(error,
                     result,
                     "[flight-client][do-get]")) {
    return nullptr;
  }
  auto flight_reader = std::move(*result);
  return gaflight_stream_reader_new_raw(flight_reader.release(), TRUE);
}


G_END_DECLS


GAFlightStreamReader *
gaflight_stream_reader_new_raw(
  arrow::flight::FlightStreamReader *flight_reader,
  gboolean is_owner)
{
  return GAFLIGHT_STREAM_READER(
    g_object_new(GAFLIGHT_TYPE_STREAM_READER,
                 "reader", flight_reader,
                 "is-owner", is_owner,
                 NULL));
}

arrow::flight::FlightCallOptions *
gaflight_call_options_get_raw(GAFlightCallOptions *options)
{
  auto priv = GAFLIGHT_CALL_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}

arrow::flight::FlightClientOptions *
gaflight_client_options_get_raw(GAFlightClientOptions *options)
{
  auto priv = GAFLIGHT_CLIENT_OPTIONS_GET_PRIVATE(options);
  return &(priv->options);
}

std::shared_ptr<arrow::flight::FlightClient>
gaflight_client_get_raw(GAFlightClient *client)
{
  auto priv = GAFLIGHT_CLIENT_GET_PRIVATE(client);
  return priv->client;
}

GAFlightClient *
gaflight_client_new_raw(
  std::shared_ptr<arrow::flight::FlightClient> *flight_client)
{
  return GAFLIGHT_CLIENT(g_object_new(GAFLIGHT_TYPE_CLIENT,
                                      "client", flight_client,
                                      NULL));
}
