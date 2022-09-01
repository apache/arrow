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

#pragma once

#include <arrow-flight-glib/server.h>

G_BEGIN_DECLS


#define GAFLIGHTSQL_TYPE_COMMAND (gaflightsql_command_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLCommand,
                         gaflightsql_command,
                         GAFLIGHTSQL,
                         COMMAND,
                         GObject)
struct _GAFlightSQLCommandClass
{
  GObjectClass parent_class;
};


#define GAFLIGHTSQL_TYPE_STATEMENT_QUERY (gaflightsql_statement_query_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLStatementQuery,
                         gaflightsql_statement_query,
                         GAFLIGHTSQL,
                         STATEMENT_QUERY,
                         GAFlightSQLCommand)
struct _GAFlightSQLStatementQueryClass
{
  GAFlightSQLCommandClass parent_class;
};

GARROW_AVAILABLE_IN_9_0
const gchar *
gaflightsql_statement_query_get_query(GAFlightSQLStatementQuery *command);


#define GAFLIGHTSQL_TYPE_STATEMENT_QUERY_TICKET         \
  (gaflightsql_statement_query_ticket_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLStatementQueryTicket,
                         gaflightsql_statement_query_ticket,
                         GAFLIGHTSQL,
                         STATEMENT_QUERY_TICKET,
                         GAFlightSQLCommand)
struct _GAFlightSQLStatementQueryTicketClass
{
  GAFlightSQLCommandClass parent_class;
};

GARROW_AVAILABLE_IN_9_0
GBytes *
gaflightsql_statement_query_ticket_generate_handle(const gchar *query,
                                                   GError **error);
GARROW_AVAILABLE_IN_9_0
GBytes *
gaflightsql_statement_query_ticket_get_handle(
  GAFlightSQLStatementQueryTicket *command);


#define GAFLIGHTSQL_TYPE_SERVER (gaflightsql_server_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLServer,
                         gaflightsql_server,
                         GAFLIGHTSQL,
                         SERVER,
                         GAFlightServer)
/**
 * GAFlightSQLServerClass:
 * @get_flight_info_statement: A virtual function to implement
 *   `GetFlightInfoStatment` API that gets a #GAFlightInfo for executing a
 *   SQL query.
 * @do_get_statement: A virtual function to implement `DoGetStatement` API
 *   that gets a #GAFlightDataStream containing the query results.
 *
 * Since: 9.0.0
 */
struct _GAFlightSQLServerClass
{
  GAFlightServerClass parent_class;

  GAFlightInfo *(*get_flight_info_statement)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLStatementQuery *command,
    GAFlightDescriptor *descriptor,
    GError **error);
  GAFlightDataStream *(*do_get_statement)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLStatementQueryTicket *ticket,
    GError **error);
};

GARROW_AVAILABLE_IN_9_0
GAFlightInfo *
gaflightsql_server_get_flight_info_statement(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLStatementQuery *command,
  GAFlightDescriptor *descriptor,
  GError **error);
GARROW_AVAILABLE_IN_9_0
GAFlightDataStream *
gaflightsql_server_do_get_statement(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLStatementQueryTicket *ticket,
  GError **error);

G_END_DECLS
