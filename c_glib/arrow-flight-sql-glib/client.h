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

#include <arrow-flight-glib/arrow-flight-glib.h>

G_BEGIN_DECLS


#define GAFLIGHTSQL_TYPE_PREPARED_STATEMENT     \
  (gaflightsql_prepared_statement_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLPreparedStatement,
                         gaflightsql_prepared_statement,
                         GAFLIGHTSQL,
                         PREPARED_STATEMENT,
                         GObject)
struct _GAFlightSQLPreparedStatementClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_14_0
GAFlightInfo *
gaflightsql_prepared_statement_execute(
  GAFlightSQLPreparedStatement *statement,
  GAFlightCallOptions *options,
  GError **error);

GARROW_AVAILABLE_IN_14_0
gint64
gaflightsql_prepared_statement_execute_update(
  GAFlightSQLPreparedStatement *statement,
  GAFlightCallOptions *options,
  GError **error);

GARROW_AVAILABLE_IN_14_0
GArrowSchema *
gaflightsql_prepared_statement_get_parameter_schema(
  GAFlightSQLPreparedStatement *statement);

GARROW_AVAILABLE_IN_14_0
GArrowSchema *
gaflightsql_prepared_statement_get_dataset_schema(
  GAFlightSQLPreparedStatement *statement);

GARROW_AVAILABLE_IN_14_0
gboolean
gaflightsql_prepared_statement_set_record_batch(
  GAFlightSQLPreparedStatement *statement,
  GArrowRecordBatch *record_batch,
  GError **error);

GARROW_AVAILABLE_IN_14_0
gboolean
gaflightsql_prepared_statement_set_record_batch_reader(
  GAFlightSQLPreparedStatement *statement,
  GArrowRecordBatchReader *reader,
  GError **error);

GARROW_AVAILABLE_IN_14_0
gboolean
gaflightsql_prepared_statement_close(
  GAFlightSQLPreparedStatement *statement,
  GAFlightCallOptions *options,
  GError **error);

GARROW_AVAILABLE_IN_14_0
gboolean
gaflightsql_prepared_statement_is_closed(
  GAFlightSQLPreparedStatement *statement);


#define GAFLIGHTSQL_TYPE_CLIENT (gaflightsql_client_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightSQLClient,
                         gaflightsql_client,
                         GAFLIGHTSQL,
                         CLIENT,
                         GObject)
struct _GAFlightSQLClientClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_9_0
GAFlightSQLClient *
gaflightsql_client_new(GAFlightClient *client);

GARROW_AVAILABLE_IN_9_0
GAFlightInfo *
gaflightsql_client_execute(GAFlightSQLClient *client,
                           const gchar *query,
                           GAFlightCallOptions *options,
                           GError **error);

GARROW_AVAILABLE_IN_13_0
gint64
gaflightsql_client_execute_update(GAFlightSQLClient *client,
                                  const gchar *query,
                                  GAFlightCallOptions *options,
                                  GError **error);

GARROW_AVAILABLE_IN_9_0
GAFlightStreamReader *
gaflightsql_client_do_get(GAFlightSQLClient *client,
                          GAFlightTicket *ticket,
                          GAFlightCallOptions *options,
                          GError **error);

GARROW_AVAILABLE_IN_14_0
GAFlightSQLPreparedStatement *
gaflightsql_client_prepare(GAFlightSQLClient *client,
                           const gchar *query,
                           GAFlightCallOptions *options,
                           GError **error);


G_END_DECLS
