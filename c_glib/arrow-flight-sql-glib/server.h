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


#define GAFLIGHTSQL_TYPE_STATEMENT_UPDATE (gaflightsql_statement_update_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLStatementUpdate,
                         gaflightsql_statement_update,
                         GAFLIGHTSQL,
                         STATEMENT_UPDATE,
                         GAFlightSQLCommand)
struct _GAFlightSQLStatementUpdateClass
{
  GAFlightSQLCommandClass parent_class;
};

GARROW_AVAILABLE_IN_13_0
const gchar *
gaflightsql_statement_update_get_query(GAFlightSQLStatementUpdate *command);


#define GAFLIGHTSQL_TYPE_PREPARED_STATEMENT_UPDATE      \
  (gaflightsql_prepared_statement_update_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLPreparedStatementUpdate,
                         gaflightsql_prepared_statement_update,
                         GAFLIGHTSQL,
                         PREPARED_STATEMENT_UPDATE,
                         GAFlightSQLCommand)
struct _GAFlightSQLPreparedStatementUpdateClass
{
  GAFlightSQLCommandClass parent_class;
};

GARROW_AVAILABLE_IN_14_0
GBytes *
gaflightsql_prepared_statement_update_get_handle(
  GAFlightSQLPreparedStatementUpdate *command);


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


#define GAFLIGHTSQL_TYPE_CREATE_PREPARED_STATEMENT_REQUEST      \
  (gaflightsql_create_prepared_statement_request_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLCreatePreparedStatementRequest,
                         gaflightsql_create_prepared_statement_request,
                         GAFLIGHTSQL,
                         CREATE_PREPARED_STATEMENT_REQUEST,
                         GObject)
struct _GAFlightSQLCreatePreparedStatementRequestClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_14_0
const gchar *
gaflightsql_create_prepared_statement_request_get_query(
  GAFlightSQLCreatePreparedStatementRequest *request);

GARROW_AVAILABLE_IN_14_0
const gchar *
gaflightsql_create_prepared_statement_request_get_transaction_id(
  GAFlightSQLCreatePreparedStatementRequest *request);


#define GAFLIGHTSQL_TYPE_CREATE_PREPARED_STATEMENT_RESULT       \
  (gaflightsql_create_prepared_statement_result_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLCreatePreparedStatementResult,
                         gaflightsql_create_prepared_statement_result,
                         GAFLIGHTSQL,
                         CREATE_PREPARED_STATEMENT_RESULT,
                         GObject)
struct _GAFlightSQLCreatePreparedStatementResultClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_14_0
GAFlightSQLCreatePreparedStatementResult *
gaflightsql_create_prepared_statement_result_new(void);
GARROW_AVAILABLE_IN_14_0
void
gaflightsql_create_prepared_statement_result_set_dataset_schema(
  GAFlightSQLCreatePreparedStatementResult *result,
  GArrowSchema *schema);
GARROW_AVAILABLE_IN_14_0
GArrowSchema *
gaflightsql_create_prepared_statement_result_get_dataset_schema(
  GAFlightSQLCreatePreparedStatementResult *result);
GARROW_AVAILABLE_IN_14_0
void
gaflightsql_create_prepared_statement_result_set_parameter_schema(
  GAFlightSQLCreatePreparedStatementResult *result,
  GArrowSchema *schema);
GARROW_AVAILABLE_IN_14_0
GArrowSchema *
gaflightsql_create_prepared_statement_result_get_parameter_schema(
  GAFlightSQLCreatePreparedStatementResult *result);
GARROW_AVAILABLE_IN_14_0
void
gaflightsql_create_prepared_statement_result_set_handle(
  GAFlightSQLCreatePreparedStatementResult *result,
  GBytes *handle);
GARROW_AVAILABLE_IN_14_0
GBytes *
gaflightsql_create_prepared_statement_result_get_handle(
  GAFlightSQLCreatePreparedStatementResult *result);


#define GAFLIGHTSQL_TYPE_CLOSE_PREPARED_STATEMENT_REQUEST      \
  (gaflightsql_close_prepared_statement_request_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLClosePreparedStatementRequest,
                         gaflightsql_close_prepared_statement_request,
                         GAFLIGHTSQL,
                         CLOSE_PREPARED_STATEMENT_REQUEST,
                         GObject)
struct _GAFlightSQLClosePreparedStatementRequestClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_14_0
GBytes *
gaflightsql_close_prepared_statement_request_get_handle(
  GAFlightSQLClosePreparedStatementRequest *request);


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
 * @do_put_command_statement_update: A virtual function to implement
 *   `DoPutCommandStatementUpdate` API that executes an update SQL statement.
 * @do_put_prepared_statement_update: A virtual function to implement
 *   `DoPutPreparedStatementUpdate` API that executes an update prepared
 *   statement.
 * @create_prepared_statement: A virtual function to implement
 *   `CreatePreparedStatement` API that creates a prepared statement
 * @close_prepared_statement: A virtual function to implement
 *   `ClosePreparedStatement` API that closes a prepared statement.
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
  gint64 (*do_put_command_statement_update)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLStatementUpdate *command,
    GError **error);
  gint64 (*do_put_prepared_statement_update)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLPreparedStatementUpdate *command,
    GAFlightMessageReader *reader,
    GError **error);
  GAFlightSQLCreatePreparedStatementResult *(*create_prepared_statement)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLCreatePreparedStatementRequest *request,
    GError **error);
  void (*close_prepared_statement)(
    GAFlightSQLServer *server,
    GAFlightServerCallContext *context,
    GAFlightSQLClosePreparedStatementRequest *request,
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
GARROW_AVAILABLE_IN_13_0
gint64
gaflightsql_server_do_put_command_statement_update(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLStatementUpdate *command,
  GError **error);
/* We can restore this after we bump version to 14.0.0-SNAPSHOT. */
/* GARROW_AVAILABLE_IN_14_0 */
gint64
gaflightsql_server_do_put_prepared_statement_update(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLPreparedStatementUpdate *command,
  GAFlightMessageReader *reader,
  GError **error);
/* We can restore this after we bump version to 14.0.0-SNAPSHOT. */
/* GARROW_AVAILABLE_IN_14_0 */
GAFlightSQLCreatePreparedStatementResult *
gaflightsql_server_create_prepared_statement(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLCreatePreparedStatementRequest *request,
  GError **error);
/* We can restore this after we bump version to 14.0.0-SNAPSHOT. */
/* GARROW_AVAILABLE_IN_14_0 */
void
gaflightsql_server_close_prepared_statement(
  GAFlightSQLServer *server,
  GAFlightServerCallContext *context,
  GAFlightSQLClosePreparedStatementRequest *request,
  GError **error);

G_END_DECLS
