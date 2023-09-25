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

#include <arrow/flight/sql/server.h>

#include <arrow-flight-sql-glib/server.h>


GAFlightSQLStatementQuery *
gaflightsql_statement_query_new_raw(
  const arrow::flight::sql::StatementQuery *flight_command);
const arrow::flight::sql::StatementQuery *
gaflightsql_statement_query_get_raw(
  GAFlightSQLStatementQuery *command);

GAFlightSQLStatementUpdate *
gaflightsql_statement_update_new_raw(
  const arrow::flight::sql::StatementUpdate *flight_command);
const arrow::flight::sql::StatementUpdate *
gaflightsql_statement_update_get_raw(
  GAFlightSQLStatementUpdate *command);

GAFlightSQLPreparedStatementUpdate *
gaflightsql_prepared_statement_update_new_raw(
  const arrow::flight::sql::PreparedStatementUpdate *flight_command);
const arrow::flight::sql::PreparedStatementUpdate *
gaflightsql_prepared_statement_update_get_raw(
  GAFlightSQLPreparedStatementUpdate *command);

GAFlightSQLStatementQueryTicket *
gaflightsql_statement_query_ticket_new_raw(
  const arrow::flight::sql::StatementQueryTicket *flight_command);
const arrow::flight::sql::StatementQueryTicket *
gaflightsql_statement_query_ticket_get_raw(
  GAFlightSQLStatementQueryTicket *command);

GAFlightSQLCreatePreparedStatementRequest *
gaflightsql_create_prepared_statement_request_new_raw(
  const arrow::flight::sql::ActionCreatePreparedStatementRequest *flight_request);
const arrow::flight::sql::ActionCreatePreparedStatementRequest *
gaflightsql_create_prepared_statement_request_get_raw(
  GAFlightSQLCreatePreparedStatementRequest *request);

GAFlightSQLClosePreparedStatementRequest *
gaflightsql_close_prepared_statement_request_new_raw(
  const arrow::flight::sql::ActionClosePreparedStatementRequest *flight_request);
const arrow::flight::sql::ActionClosePreparedStatementRequest *
gaflightsql_close_prepared_statement_request_get_raw(
  GAFlightSQLClosePreparedStatementRequest *request);
