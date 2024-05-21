/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.sql;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.sql.impl.FlightSql;

/**
 * A {@link FlightSqlProducer} that throws on all FlightSql-specific operations.
 */
public class NoOpFlightSqlProducer implements FlightSqlProducer {
  @Override
  public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
                                      CallContext context, StreamListener<Result> listener) {
    listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
                                     CallContext context, StreamListener<Result> listener) {
    listener.onError(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
                                           CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                                   CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
                                         CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamStatement(FlightSql.TicketStatementQuery ticket,
                                 CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                         CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                     FlightStream flightStream, StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                   CallContext context, FlightStream flightStream,
                                                   StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                                  FlightStream flightStream, StreamListener<PutResult> ackStream) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context,
                                         FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context,
                               ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                          CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request,
                                          CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request,
                                         CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamSchemas(FlightSql.CommandGetDbSchemas command,
                               CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request,
                                        CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamTables(FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                            FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request,
                                             CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command,
                                   CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request,
                                              CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request,
                                              CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request,
                                                CallContext context, FlightDescriptor descriptor) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }

  @Override
  public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command,
                                    CallContext context, ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                    ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                      ServerStreamListener listener) {
    listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
    throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
  }
}
