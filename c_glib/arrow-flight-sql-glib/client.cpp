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
 * #GAFlightSQLPreparedStatement is a class for prepared statement.
 *
 * Since: 9.0.0
 */

struct GAFlightSQLPreparedStatementPrivate {
  std::shared_ptr<arrow::flight::sql::PreparedStatement> statement;
  GAFlightSQLClient *client;
};

enum {
  PROP_STATEMENT = 1,
  PROP_PREPARED_STATEMENT_CLIENT,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightSQLPreparedStatement,
                           gaflightsql_prepared_statement,
                           G_TYPE_OBJECT)

#define GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object)      \
  static_cast<GAFlightSQLPreparedStatementPrivate *>(           \
    gaflightsql_prepared_statement_get_instance_private(        \
      GAFLIGHTSQL_PREPARED_STATEMENT(object)))

static void
gaflightsql_prepared_statement_dispose(GObject *object)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object);

  if (priv->client) {
    g_object_unref(priv->client);
    priv->client = nullptr;
  }

  G_OBJECT_CLASS(gaflightsql_prepared_statement_parent_class)->dispose(object);
}

static void
gaflightsql_prepared_statement_finalize(GObject *object)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object);
  priv->statement.~shared_ptr();
  G_OBJECT_CLASS(gaflightsql_prepared_statement_parent_class)->finalize(object);
}

static void
gaflightsql_prepared_statement_set_property(GObject *object,
                                            guint prop_id,
                                            const GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_STATEMENT:
    priv->statement =
      *static_cast<std::shared_ptr<arrow::flight::sql::PreparedStatement> *>(
        g_value_get_pointer(value));
    break;
  case PROP_PREPARED_STATEMENT_CLIENT:
    priv->client = GAFLIGHTSQL_CLIENT(g_value_dup_object(value));
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflightsql_prepared_statement_get_property(GObject *object,
                                            guint prop_id,
                                            GValue *value,
                                            GParamSpec *pspec)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_PREPARED_STATEMENT_CLIENT:
    g_value_set_object(value, priv->client);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflightsql_prepared_statement_init(GAFlightSQLPreparedStatement *object)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(object);
  new(&priv->statement) std::shared_ptr<arrow::flight::sql::PreparedStatement>;
}

static void
gaflightsql_prepared_statement_class_init(
  GAFlightSQLPreparedStatementClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);

  gobject_class->dispose = gaflightsql_prepared_statement_dispose;
  gobject_class->finalize = gaflightsql_prepared_statement_finalize;
  gobject_class->set_property = gaflightsql_prepared_statement_set_property;
  gobject_class->get_property = gaflightsql_prepared_statement_get_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("statement",
                              nullptr,
                              nullptr,
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_STATEMENT, spec);

  /**
   * GAFlightSQLPreparedStatement:client:
   *
   * The underlying Flight SQL client.
   *
   * Since: 14.0.0
   */
  spec = g_param_spec_object("client",
                             nullptr,
                             nullptr,
                             GAFLIGHTSQL_TYPE_CLIENT,
                             static_cast<GParamFlags>(G_PARAM_READWRITE |
                                                      G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class,
                                  PROP_PREPARED_STATEMENT_CLIENT,
                                  spec);
}

/**
 * gaflightsql_prepared_statement_execute:
 * @statement: A #GAFlightSQLPreparedStatement.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GAFlightInfo describing
 *   where to access the dataset on success, %NULL on error.
 *
 * Since: 14.0.0
 */
GAFlightInfo *
gaflightsql_prepared_statement_execute(GAFlightSQLPreparedStatement *statement,
                                       GAFlightCallOptions *options,
                                       GError **error)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_sql_statement->Execute(*flight_options);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-prepared-statement][execute]")) {
    return nullptr;
  }
  auto flight_info = std::move(*result);
  return gaflight_info_new_raw(flight_info.release());
}

/**
 * gaflightsql_prepared_statement_execute_update:
 * @statement: A #GAFlightSQLPreparedStatement.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The number of changed records.
 *
 * Since: 14.0.0
 */
gint64
gaflightsql_prepared_statement_execute_update(
  GAFlightSQLPreparedStatement *statement,
  GAFlightCallOptions *options,
  GError **error)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  auto result = flight_sql_statement->ExecuteUpdate(*flight_options);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-prepared-statement][execute-update]")) {
    return 0;
  }
  return *result;
}

/**
 * gaflightsql_prepared_statement_get_parameter_schema:
 * @statement: A #GAFlightSQLPreparedStatement.
 *
 * Returns: (nullable) (transfer full): The #GArrowSchema for parameter.
 *
 * Since: 14.0.0
 */
GArrowSchema *
gaflightsql_prepared_statement_get_parameter_schema(
  GAFlightSQLPreparedStatement *statement)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  auto arrow_schema = flight_sql_statement->parameter_schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * gaflightsql_prepared_statement_get_dataset_schema:
 * @statement: A #GAFlightSQLPreparedStatement.
 *
 * Returns: (nullable) (transfer full): The #GArrowSchema for dataset.
 *
 * Since: 14.0.0
 */
GArrowSchema *
gaflightsql_prepared_statement_get_dataset_schema(
  GAFlightSQLPreparedStatement *statement)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  auto arrow_schema = flight_sql_statement->dataset_schema();
  return garrow_schema_new_raw(&arrow_schema);
}

/**
 * gaflightsql_prepared_statement_set_record_batch:
 * @statement: A #GAFlightSQLPreparedStatement.
 * @record_batch: A #GArrowRecordBatch that contains the parameters that
 *   will be bound.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 14.0.0
 */
gboolean
gaflightsql_prepared_statement_set_record_batch(
  GAFlightSQLPreparedStatement *statement,
  GArrowRecordBatch *record_batch,
  GError **error)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  auto arrow_record_batch = garrow_record_batch_get_raw(record_batch);
  return garrow::check(error,
                       flight_sql_statement->SetParameters(arrow_record_batch),
                       "[flight-sql-prepared-statement][set-record-batch]");
}

/**
 * gaflightsql_prepared_statement_set_record_batch_reader:
 * @statement: A #GAFlightSQLPreparedStatement.
 * @reader: A #GArrowRecordBatchReader that contains the parameters that
 *   will be bound.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * Since: 14.0.0
 */
gboolean
gaflightsql_prepared_statement_set_record_batch_reader(
  GAFlightSQLPreparedStatement *statement,
  GArrowRecordBatchReader *reader,
  GError **error)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  auto arrow_reader = garrow_record_batch_reader_get_raw(reader);
  return garrow::check(error,
                       flight_sql_statement->SetParameters(arrow_reader),
                       "[flight-sql-prepared-statement][set-record-batch-reader]");
}

/**
 * gaflightsql_prepared_statement_close:
 * @statement: A #GAFlightSQLPreparedStatement.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: %TRUE on success, %FALSE otherwise.
 *
 * After this, the prepared statement may not be used anymore.
 *
 * Since: 14.0.0
 */
gboolean
gaflightsql_prepared_statement_close(GAFlightSQLPreparedStatement *statement,
                                     GAFlightCallOptions *options,
                                     GError **error)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  arrow::flight::FlightCallOptions flight_default_options;
  auto flight_options = &flight_default_options;
  if (options) {
    flight_options = gaflight_call_options_get_raw(options);
  }
  return garrow::check(error,
                       flight_sql_statement->Close(*flight_options),
                       "[flight-sql-prepared-statement][close]");
}

/**
 * gaflightsql_prepared_statement_is_closed:
 * @statement: A #GAFlightSQLPreparedStatement.
 *
 * Returns: Whether the prepared statement is closed or not.
 *
 * Since: 14.0.0
 */
gboolean
gaflightsql_prepared_statement_is_closed(GAFlightSQLPreparedStatement *statement)
{
  auto flight_sql_statement = gaflightsql_prepared_statement_get_raw(statement);
  return flight_sql_statement->IsClosed();
}


struct GAFlightSQLClientPrivate {
  arrow::flight::sql::FlightSqlClient *client;
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
 * gaflightsql_client_execute_update:
 * @client: A #GAFlightSQLClient.
 * @query: A query to be executed in the UTF-8 format.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: The number of changed records.
 *
 * Since: 13.0.0
 */
gint64
gaflightsql_client_execute_update(GAFlightSQLClient *client,
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
  auto result = flight_sql_client->ExecuteUpdate(*flight_options, query);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-client][execute-update]")) {
    return 0;
  }
  return *result;
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
  return gaflight_stream_reader_new_raw(flight_reader.release(), TRUE);
}

/**
 * gaflightsql_client_prepare:
 * @client: A #GAFlightSQLClient.
 * @query: A query to be prepared in the UTF-8 format.
 * @options: (nullable): A #GAFlightCallOptions.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): The #GAFlightSQLPreparedStatement
 *   on success, %NULL on error.
 *
 * Since: 14.0.0
 */
GAFlightSQLPreparedStatement *
gaflightsql_client_prepare(GAFlightSQLClient *client,
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
  auto result = flight_sql_client->Prepare(*flight_options, query);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-client][prepare]")) {
    return nullptr;
  }
  auto flight_sql_statement = std::move(*result);
  return gaflightsql_prepared_statement_new_raw(&flight_sql_statement,
                                                client);
}


G_END_DECLS


GAFlightSQLPreparedStatement *
gaflightsql_prepared_statement_new_raw(
  std::shared_ptr<arrow::flight::sql::PreparedStatement> *flight_sql_statement,
  GAFlightSQLClient *client)
{
  return GAFLIGHTSQL_PREPARED_STATEMENT(
    g_object_new(GAFLIGHTSQL_TYPE_PREPARED_STATEMENT,
                 "statement", flight_sql_statement,
                 "client", client,
                 nullptr));
}

std::shared_ptr<arrow::flight::sql::PreparedStatement>
gaflightsql_prepared_statement_get_raw(GAFlightSQLPreparedStatement *statement)
{
  auto priv = GAFLIGHTSQL_PREPARED_STATEMENT_GET_PRIVATE(statement);
  return priv->statement;
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

arrow::flight::sql::FlightSqlClient *
gaflightsql_client_get_raw(GAFlightSQLClient *client)
{
  auto priv = GAFLIGHTSQL_CLIENT_GET_PRIVATE(client);
  return priv->client;
}
