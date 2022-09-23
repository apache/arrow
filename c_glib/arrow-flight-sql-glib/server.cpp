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

#include <arrow-flight-sql-glib/server.hpp>

G_BEGIN_DECLS

/**
 * SECTION: server
 * @section_id: server
 * @title: Server related classes
 * @include: arrow-flight-sql-glib/arrow-flight-sql-glib.h
 *
 * #GAFlightSQLServer is a class to develop an Apache Arrow Flight SQL
 * server.
 *
 * Since: 9.0.0
 */


struct GAFlightSQLCommandPrivate {
  void *command;
};

enum {
  PROP_COMMAND = 1,
};

G_DEFINE_TYPE_WITH_PRIVATE(GAFlightSQLCommand,
                           gaflightsql_command,
                           G_TYPE_OBJECT)

#define GAFLIGHTSQL_COMMAND_GET_PRIVATE(object)         \
  static_cast<GAFlightSQLCommandPrivate *>(             \
    gaflightsql_command_get_instance_private(           \
      GAFLIGHTSQL_COMMAND(object)))

static void
gaflightsql_command_set_property(GObject *object,
                                 guint prop_id,
                                 const GValue *value,
                                 GParamSpec *pspec)
{
  auto priv = GAFLIGHTSQL_COMMAND_GET_PRIVATE(object);

  switch (prop_id) {
  case PROP_COMMAND:
    priv->command = g_value_get_pointer(value);
    break;
  default:
    G_OBJECT_WARN_INVALID_PROPERTY_ID(object, prop_id, pspec);
    break;
  }
}

static void
gaflightsql_command_init(GAFlightSQLCommand *object)
{
}

static void
gaflightsql_command_class_init(GAFlightSQLCommandClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->set_property = gaflightsql_command_set_property;

  GParamSpec *spec;
  spec = g_param_spec_pointer("command",
                              "Command",
                              "The raw command struct",
                              static_cast<GParamFlags>(G_PARAM_WRITABLE |
                                                       G_PARAM_CONSTRUCT_ONLY));
  g_object_class_install_property(gobject_class, PROP_COMMAND, spec);
}


G_DEFINE_TYPE(GAFlightSQLStatementQuery,
              gaflightsql_statement_query,
              GAFLIGHTSQL_TYPE_COMMAND)

static void
gaflightsql_statement_query_init(GAFlightSQLStatementQuery *object)
{
}

static void
gaflightsql_statement_query_class_init(GAFlightSQLStatementQueryClass *klass)
{
}

/**
 * gaflightsql_statement_query_get_query:
 * @command: A #GAFlightSQLStatementQuery.
 *
 * Returns: The query to be executed.
 *
 * Since: 9.0.0
 */
const gchar *
gaflightsql_statement_query_get_query(GAFlightSQLStatementQuery *command)
{
  auto statement_query = gaflightsql_statement_query_get_raw(command);
  return statement_query->query.c_str();
}


G_DEFINE_TYPE(GAFlightSQLStatementQueryTicket,
              gaflightsql_statement_query_ticket,
              GAFLIGHTSQL_TYPE_COMMAND)

#define GAFLIGHTSQL_STATEMENT_QUERY_TICKET_GET_PRIVATE(object) \
  static_cast<GAFlightSQLStatementQueryTicketPrivate *>(       \
    gaflightsql_statement_query_ticket_get_instance_private(   \
      GAFLIGHTSQL_STATEMENT_QUERY_TICKET(object)))

static void
gaflightsql_statement_query_ticket_init(GAFlightSQLStatementQueryTicket *object)
{
}

static void
gaflightsql_statement_query_ticket_class_init(
  GAFlightSQLStatementQueryTicketClass *klass)
{
}

/**
 * gaflightsql_statement_query_ticket_generate_handle:
 * @query: A query to be executed.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A handle for the given @query
 *   as #GBytes, %NULL on error.
 *
 * Since: 9.0.0
 */
GBytes *
gaflightsql_statement_query_ticket_generate_handle(const gchar *query,
                                                   GError **error)
{
  auto result = arrow::flight::sql::CreateStatementQueryTicket(query);
  if (!garrow::check(error,
                     result,
                     "[flight-sql-statement-query-ticket][new]")) {
    return nullptr;
  }
  auto flight_sql_handle = std::move(*result);
  return g_bytes_new(flight_sql_handle.data(),
                     flight_sql_handle.size());
}

/**
 * gaflightsql_statement_query_ticket_get_handle:
 * @command: A #GAFlightSQLStatementQuery.
 *
 * Returns: (transfer full): The handle to identify the query to be
 *   executed.
 *
 * Since: 9.0.0
 */
GBytes *
gaflightsql_statement_query_ticket_get_handle(
  GAFlightSQLStatementQueryTicket *command)
{
  auto statement_query_ticket =
    gaflightsql_statement_query_ticket_get_raw(command);
  auto &handle = statement_query_ticket->statement_handle;
  return g_bytes_new_static(handle.data(), handle.size());
}


G_END_DECLS
namespace gaflightsql {
  class Server : public arrow::flight::sql::FlightSqlServerBase {
  public:
    explicit Server(GAFlightSQLServer *gaserver) :
      FlightSqlServerBase(),
      gaserver_(gaserver) {
    }

    ~Server() override = default;

    arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>>
    GetFlightInfoStatement(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::sql::StatementQuery& command,
      const arrow::flight::FlightDescriptor& descriptor) override {
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto gacommand = gaflightsql_statement_query_new_raw(&command);
      auto gadescriptor = gaflight_descriptor_new_raw(&descriptor);
      GError *gerror = nullptr;
      auto gainfo = gaflightsql_server_get_flight_info_statement(gaserver_,
                                                                 gacontext,
                                                                 gacommand,
                                                                 gadescriptor,
                                                                 &gerror);
      g_object_unref(gadescriptor);
      g_object_unref(gacommand);
      g_object_unref(gacontext);
      if (gerror) {
        auto context = "[flight-sql-server][get-flight-info-statement]";
        return garrow_error_to_status(gerror,
                                      arrow::StatusCode::UnknownError,
                                      context);
      }
      return std::make_unique<arrow::flight::FlightInfo>(
        *gaflight_info_get_raw(gainfo));
    }

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>>
    DoGetStatement(
      const arrow::flight::ServerCallContext &context,
      const arrow::flight::sql::StatementQueryTicket& command) override {
      auto gacontext = gaflight_server_call_context_new_raw(&context);
      auto gacommand = gaflightsql_statement_query_ticket_new_raw(&command);
      GError *gerror = nullptr;
      auto gastream = gaflightsql_server_do_get_statement(gaserver_,
                                                          gacontext,
                                                          gacommand,
                                                          &gerror);
      g_object_unref(gacommand);
      g_object_unref(gacontext);
      if (gerror) {
        return garrow_error_to_status(gerror,
                                      arrow::StatusCode::UnknownError,
                                      "[flight-sql-server][do-get-statement]");
      }
      return std::make_unique<gaflight::DataStream>(gastream);
    }

  private:
    GAFlightSQLServer *gaserver_;
  };
};
G_BEGIN_DECLS

struct GAFlightSQLServerPrivate {
  gaflightsql::Server server;
};

G_END_DECLS
static arrow::flight::FlightServerBase *
gaflightsql_server_servable_get_raw(GAFlightServable *servable);
G_BEGIN_DECLS

static void
gaflightsql_server_servable_interface_init(GAFlightServableInterface *iface)
{
  iface->get_raw = gaflightsql_server_servable_get_raw;
}

G_DEFINE_ABSTRACT_TYPE_WITH_CODE(GAFlightSQLServer,
                                 gaflightsql_server,
                                 GAFLIGHT_TYPE_SERVER,
                                 G_ADD_PRIVATE(GAFlightSQLServer);
                                 G_IMPLEMENT_INTERFACE(
                                   GAFLIGHT_TYPE_SERVABLE,
                                   gaflightsql_server_servable_interface_init))

#define GAFLIGHTSQL_SERVER_GET_PRIVATE(object)   \
  static_cast<GAFlightSQLServerPrivate *>(       \
    gaflightsql_server_get_instance_private(     \
      GAFLIGHTSQL_SERVER(object)))

G_END_DECLS
static arrow::flight::FlightServerBase *
gaflightsql_server_servable_get_raw(GAFlightServable *servable)
{
  auto priv = GAFLIGHTSQL_SERVER_GET_PRIVATE(servable);
  return &(priv->server);
}
G_BEGIN_DECLS

static void
gaflightsql_server_finalize(GObject *object)
{
  auto priv = GAFLIGHTSQL_SERVER_GET_PRIVATE(object);
  priv->server.~Server();

  G_OBJECT_CLASS(gaflightsql_server_parent_class)->finalize(object);
}

static void
gaflightsql_server_init(GAFlightSQLServer *object)
{
  auto priv = GAFLIGHTSQL_SERVER_GET_PRIVATE(object);
  new(&(priv->server)) gaflightsql::Server(object);
}

static void
gaflightsql_server_class_init(GAFlightSQLServerClass *klass)
{
  auto gobject_class = G_OBJECT_CLASS(klass);
  gobject_class->finalize = gaflightsql_server_finalize;
}

/**
 * gaflightsql_server_get_flight_info_statement:
 * @server: A #GAFlightSQLServer.
 * @context: A #GAFlightServerCallContext.
 * @command: A #GAFlightSQLStatementQuery to be executed.
 * @descriptor: A #GAFlightDescriptor.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A #GAFlightInfo for executing
 *   a SQL query on success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightInfo *
gaflightsql_server_get_flight_info_statement(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLStatementQuery *command,
  GAFlightDescriptor *descriptor,
  GError **error)
{
  auto klass = GAFLIGHTSQL_SERVER_GET_CLASS(server);
  if (!(klass && klass->get_flight_info_statement)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "not implemented");
    return NULL;
  }
  return (*(klass->get_flight_info_statement))(server,
                                               context,
                                               command,
                                               descriptor,
                                               error);
}

/**
 * gaflightsql_server_do_get_statement:
 * @server: A #GAFlightServer.
 * @context: A #GAFlightServerCallContext.
 * @ticket: A #GAFlightSQLStatementQueryTicket.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: (nullable) (transfer full): A #GAFlightDataStream
 *   containing the query results on success, %NULL on error.
 *
 * Since: 9.0.0
 */
GAFlightDataStream *
gaflightsql_server_do_get_statement(GAFlightSQLServer *server,
                                    GAFlightServerCallContext *context,
                                    GAFlightSQLStatementQueryTicket *ticket,
                                    GError **error)
{
  auto klass = GAFLIGHTSQL_SERVER_GET_CLASS(server);
  if (!(klass && klass->do_get_statement)) {
    g_set_error(error,
                GARROW_ERROR,
                GARROW_ERROR_NOT_IMPLEMENTED,
                "not implemented");
    return nullptr;
  }
  return (*(klass->do_get_statement))(server, context, ticket, error);
}


G_END_DECLS


GAFlightSQLStatementQuery *
gaflightsql_statement_query_new_raw(
  const arrow::flight::sql::StatementQuery *flight_command)
{
  return GAFLIGHTSQL_STATEMENT_QUERY(
    g_object_new(GAFLIGHTSQL_TYPE_STATEMENT_QUERY,
                 "command", flight_command,
                 nullptr));
}

const arrow::flight::sql::StatementQuery *
gaflightsql_statement_query_get_raw(GAFlightSQLStatementQuery *command)
{
  auto priv = GAFLIGHTSQL_COMMAND_GET_PRIVATE(command);
  return static_cast<const arrow::flight::sql::StatementQuery *>(priv->command);
}

GAFlightSQLStatementQueryTicket *
gaflightsql_statement_query_ticket_new_raw(
  const arrow::flight::sql::StatementQueryTicket *flight_command)
{
  return GAFLIGHTSQL_STATEMENT_QUERY_TICKET(
    g_object_new(GAFLIGHTSQL_TYPE_STATEMENT_QUERY_TICKET,
                 "command", flight_command,
                 nullptr));
}

const arrow::flight::sql::StatementQueryTicket *
gaflightsql_statement_query_ticket_get_raw(
  GAFlightSQLStatementQueryTicket *command)
{
  auto priv = GAFLIGHTSQL_COMMAND_GET_PRIVATE(command);
  return static_cast<const arrow::flight::sql::StatementQueryTicket *>(
    priv->command);
}
