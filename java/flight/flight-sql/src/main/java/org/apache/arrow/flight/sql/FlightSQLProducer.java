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

import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_ACTIONS;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_CLOSEPREPAREDSTATEMENT;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETCATALOGS;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETPREPAREDSTATEMENT;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETSCHEMAS;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETSQLINFO;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETTABLES;
import static org.apache.arrow.flight.sql.FlightSQLUtils.FLIGHT_SQL_GETTABLETYPES;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetCatalogsRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetPreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetSchemasRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetTablesRequest;
import org.apache.arrow.flight.sql.impl.FlightSQL.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSQL.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSQL.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSQL.CommandStatementUpdate;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

/**
 * API to Implement an Arrow Flight SQL producer.
 */
public abstract class FlightSQLProducer implements FlightProducer, AutoCloseable {

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSQLUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getFlightInfoStatement(FlightSQLUtils.unpackOrThrow(command, CommandStatementQuery.class), descriptor,
              context);

    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return getFlightInfoPreparedStatement(
              FlightSQLUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class), descriptor, context);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Get information about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoStatement(CommandStatementQuery command, FlightDescriptor descriptor,
          CallContext context);

  /**
   * Get information about a particular prepared statement data stream.
   *
   * @param command    The prepared statement to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  public abstract FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery command,
          FlightDescriptor descriptor, CallContext context);

  @Override
  public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSQLUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getSchemaStatement(FlightSQLUtils.unpackOrThrow(command, CommandStatementQuery.class), descriptor,
              context);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Get schema about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Schema for the stream.
   */
  public abstract SchemaResult getSchemaStatement(CommandStatementQuery command, FlightDescriptor descriptor,
          CallContext context);

  @Override
  public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    final Any command = FlightSQLUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

    if (command.is(CommandStatementUpdate.class)) {
      return acceptPutStatement(
              FlightSQLUtils.unpackOrThrow(command, CommandStatementUpdate.class),
              context, flightStream, ackStream);

    } else if (command.is(CommandPreparedStatementUpdate.class)) {
      return acceptPutPreparedStatementUpdate(
              FlightSQLUtils.unpackOrThrow(command, CommandPreparedStatementUpdate.class),
              context, flightStream, ackStream);

    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return acceptPutPreparedStatementQuery(
              FlightSQLUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class),
              context, flightStream, ackStream);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Accept uploaded data for a particular SQL query based data stream. PutResults must be in the form of a
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.DoPutUpdateResult}.
   *
   * @param command      The sql command to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutStatement(CommandStatementUpdate command, CallContext context,
          FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * Accept uploaded data for a particular prepared statement data stream. PutResults must be in the form of a
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.DoPutUpdateResult}.
   *
   * @param command      The prepared statement to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command,
          CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * Accept uploaded parameter values for a particular prepared statement query.
   *
   * @param command      The prepared statement the parameter values will bind to.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  public abstract Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command,
          CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream);

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

    if (action.getType().equals(FLIGHT_SQL_GETSQLINFO.getType())) {
      getSqlInfo(context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_GETCATALOGS.getType())) {
      final ActionGetCatalogsRequest request = FlightSQLUtils.unpackAndParseOrThrow(action.getBody(),
              ActionGetCatalogsRequest.class);
      getCatalogs(request, context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_GETSCHEMAS.getType())) {
      final ActionGetSchemasRequest request = FlightSQLUtils.unpackAndParseOrThrow(action.getBody(),
              ActionGetSchemasRequest.class);
      getSchemas(request, context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_GETTABLES.getType())) {
      final ActionGetTablesRequest request = FlightSQLUtils.unpackAndParseOrThrow(action.getBody(),
              ActionGetTablesRequest.class);
      getTables(request, context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_GETTABLETYPES.getType())) {
      getTableTypes(context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_GETPREPAREDSTATEMENT.getType())) {
      final ActionGetPreparedStatementRequest request = FlightSQLUtils.unpackAndParseOrThrow(action.getBody(),
              ActionGetPreparedStatementRequest.class);
      getPreparedStatement(request, context, listener);

    } else if (action.getType().equals(FLIGHT_SQL_CLOSEPREPAREDSTATEMENT.getType())) {
      final ActionClosePreparedStatementRequest request = FlightSQLUtils.unpackAndParseOrThrow(action.getBody(),
              ActionClosePreparedStatementRequest.class);
      closePreparedStatement(request, context, listener);
    }
  }

  /**
   * Returns the SQL Info of the server by returning a
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetSQLInfoResult} in a {@link Result}.
   *
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getSqlInfo(CallContext context, StreamListener<Result> listener);

  /**
   * Returns the available catalogs by returning a stream of
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetCatalogsResult} objects in {@link Result} objects.
   *
   * @param request  request filter parameters.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getCatalogs(ActionGetCatalogsRequest request, CallContext context,
          StreamListener<Result> listener);

  /**
   * Returns the available schemas by returning a stream of
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetSchemasResult} objects in {@link Result} objects.
   *
   * @param request  request filter parameters.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getSchemas(ActionGetSchemasRequest request, CallContext context,
          StreamListener<Result> listener);

  /**
   * Returns the available table types by returning a stream of
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetTableTypesResult} objects in {@link Result} objects.
   *
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getTableTypes(CallContext context, StreamListener<Result> listener);

  /**
   * Returns the available tables by returning a stream of
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetTablesResult} objects in {@link Result} objects.
   *
   * @param request  request filter parameters.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getTables(ActionGetTablesRequest request, CallContext context, StreamListener<Result> listener);

  /**
   * Creates a prepared statement on the server and returns a handle and metadata for in a
   * {@link org.apache.arrow.flight.sql.impl.FlightSQL.ActionGetPreparedStatementResult} object in a {@link Result}
   * object.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void getPreparedStatement(ActionGetPreparedStatementRequest request, CallContext context,
          StreamListener<Result> listener);

  /**
   * Closes a prepared statement on the server. No result is expected.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  public abstract void closePreparedStatement(ActionClosePreparedStatementRequest request, CallContext context,
          StreamListener<Result> listener);

  @Override
  public void listActions(CallContext context, StreamListener<ActionType> listener) {
    FLIGHT_SQL_ACTIONS.forEach(action -> listener.onNext(action));
    listener.onCompleted();
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    final Any command;

    try {
      command = Any.parseFrom(ticket.getBytes());
    } catch (InvalidProtocolBufferException e) {
      listener.error(e);
      return;
    }

    if (command.is(CommandStatementQuery.class)) {
      getStreamStatement(FlightSQLUtils.unpackOrThrow(command, CommandStatementQuery.class),
              context, ticket, listener);

    } else if (command.is(CommandPreparedStatementQuery.class)) {
      getStreamPreparedStatement(FlightSQLUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class),
              context, ticket, listener);
    }

    throw Status.INVALID_ARGUMENT.asRuntimeException();
  }

  /**
   * Return data for a SQL query based data stream.
   *
   * @param command  The sql command to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamStatement(CommandStatementQuery command, CallContext context, Ticket ticket,
          ServerStreamListener listener);

  /**
   * Return data for a particular prepared statement query instance.
   *
   * @param command  The prepared statement to generate the data stream.
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  public abstract void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context,
          Ticket ticket, ServerStreamListener listener);
}
