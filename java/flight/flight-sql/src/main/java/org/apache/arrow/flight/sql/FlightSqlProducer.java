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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import static org.apache.arrow.vector.complex.MapVector.DATA_VECTOR_NAME;
import static org.apache.arrow.vector.complex.MapVector.KEY_NAME;
import static org.apache.arrow.vector.complex.MapVector.VALUE_NAME;
import static org.apache.arrow.vector.types.Types.MinorType.BIGINT;
import static org.apache.arrow.vector.types.Types.MinorType.BIT;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.apache.arrow.vector.types.Types.MinorType.LIST;
import static org.apache.arrow.vector.types.Types.MinorType.STRUCT;
import static org.apache.arrow.vector.types.Types.MinorType.UINT4;
import static org.apache.arrow.vector.types.Types.MinorType.VARCHAR;

import java.util.List;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * API to Implement an Arrow Flight SQL producer.
 */
public interface FlightSqlProducer extends FlightProducer, AutoCloseable {
  /**
   * Depending on the provided command, method either:
   * 1. Return information about a SQL query, or
   * 2. Return information about a prepared statement. In this case, parameters binding is allowed.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return information about the given SQL query, or the given prepared statement.
   */
  @Override
  default FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getFlightInfoStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, descriptor);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return getFlightInfoPreparedStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class), context, descriptor);
    } else if (command.is(CommandGetCatalogs.class)) {
      return getFlightInfoCatalogs(
          FlightSqlUtils.unpackOrThrow(command, CommandGetCatalogs.class), context, descriptor);
    } else if (command.is(CommandGetDbSchemas.class)) {
      return getFlightInfoSchemas(
          FlightSqlUtils.unpackOrThrow(command, CommandGetDbSchemas.class), context, descriptor);
    } else if (command.is(CommandGetTables.class)) {
      return getFlightInfoTables(
          FlightSqlUtils.unpackOrThrow(command, CommandGetTables.class), context, descriptor);
    } else if (command.is(CommandGetTableTypes.class)) {
      return getFlightInfoTableTypes(
          FlightSqlUtils.unpackOrThrow(command, CommandGetTableTypes.class), context, descriptor);
    } else if (command.is(CommandGetSqlInfo.class)) {
      return getFlightInfoSqlInfo(
          FlightSqlUtils.unpackOrThrow(command, CommandGetSqlInfo.class), context, descriptor);
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      return getFlightInfoPrimaryKeys(
          FlightSqlUtils.unpackOrThrow(command, CommandGetPrimaryKeys.class), context, descriptor);
    } else if (command.is(CommandGetExportedKeys.class)) {
      return getFlightInfoExportedKeys(
          FlightSqlUtils.unpackOrThrow(command, CommandGetExportedKeys.class), context, descriptor);
    } else if (command.is(CommandGetImportedKeys.class)) {
      return getFlightInfoImportedKeys(
          FlightSqlUtils.unpackOrThrow(command, CommandGetImportedKeys.class), context, descriptor);
    } else if (command.is(CommandGetCrossReference.class)) {
      return getFlightInfoCrossReference(
          FlightSqlUtils.unpackOrThrow(command, CommandGetCrossReference.class), context, descriptor);
    } else if (command.is(CommandGetXdbcTypeInfo.class)) {
      return getFlightInfoTypeInfo(
          FlightSqlUtils.unpackOrThrow(command, CommandGetXdbcTypeInfo.class), context, descriptor);
    }

    throw CallStatus.INVALID_ARGUMENT.withDescription("The defined request is invalid.").toRuntimeException();
  }

  /**
   * Returns the schema of the result produced by the SQL query.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return the result set schema.
   */
  @Override
  default SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
    final Any command = FlightSqlUtils.parseOrThrow(descriptor.getCommand());

    if (command.is(CommandStatementQuery.class)) {
      return getSchemaStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementQuery.class), context, descriptor);
    } else if (command.is(CommandGetCatalogs.class)) {
      return new SchemaResult(Schemas.GET_CATALOGS_SCHEMA);
    } else if (command.is(CommandGetDbSchemas.class)) {
      return new SchemaResult(Schemas.GET_SCHEMAS_SCHEMA);
    } else if (command.is(CommandGetTables.class)) {
      return new SchemaResult(Schemas.GET_TABLES_SCHEMA);
    } else if (command.is(CommandGetTableTypes.class)) {
      return new SchemaResult(Schemas.GET_TABLE_TYPES_SCHEMA);
    } else if (command.is(CommandGetSqlInfo.class)) {
      return new SchemaResult(Schemas.GET_SQL_INFO_SCHEMA);
    } else if (command.is(CommandGetXdbcTypeInfo.class)) {
      return new SchemaResult(Schemas.GET_TYPE_INFO_SCHEMA);
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      return new SchemaResult(Schemas.GET_PRIMARY_KEYS_SCHEMA);
    } else if (command.is(CommandGetImportedKeys.class)) {
      return new SchemaResult(Schemas.GET_IMPORTED_KEYS_SCHEMA);
    } else if (command.is(CommandGetExportedKeys.class)) {
      return new SchemaResult(Schemas.GET_EXPORTED_KEYS_SCHEMA);
    } else if (command.is(CommandGetCrossReference.class)) {
      return new SchemaResult(Schemas.GET_CROSS_REFERENCE_SCHEMA);
    }

    throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid command provided.").toRuntimeException();
  }

  /**
   * Depending on the provided command, method either:
   * 1. Return data for a stream produced by executing the provided SQL query, or
   * 2. Return data for a prepared statement. In this case, parameters binding is allowed.
   *
   * @param context  Per-call context.
   * @param ticket   The application-defined ticket identifying this stream.
   * @param listener An interface for sending data back to the client.
   */
  @Override
  default void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    final Any command;

    try {
      command = Any.parseFrom(ticket.getBytes());
    } catch (InvalidProtocolBufferException e) {
      listener.error(e);
      return;
    }

    if (command.is(TicketStatementQuery.class)) {
      getStreamStatement(
          FlightSqlUtils.unpackOrThrow(command, TicketStatementQuery.class), context, listener);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      getStreamPreparedStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class), context, listener);
    } else if (command.is(CommandGetCatalogs.class)) {
      getStreamCatalogs(context, listener);
    } else if (command.is(CommandGetDbSchemas.class)) {
      getStreamSchemas(FlightSqlUtils.unpackOrThrow(command, CommandGetDbSchemas.class), context, listener);
    } else if (command.is(CommandGetTables.class)) {
      getStreamTables(FlightSqlUtils.unpackOrThrow(command, CommandGetTables.class), context, listener);
    } else if (command.is(CommandGetTableTypes.class)) {
      getStreamTableTypes(context, listener);
    } else if (command.is(CommandGetSqlInfo.class)) {
      getStreamSqlInfo(FlightSqlUtils.unpackOrThrow(command, CommandGetSqlInfo.class), context, listener);
    } else if (command.is(CommandGetPrimaryKeys.class)) {
      getStreamPrimaryKeys(FlightSqlUtils.unpackOrThrow(command, CommandGetPrimaryKeys.class), context, listener);
    } else if (command.is(CommandGetExportedKeys.class)) {
      getStreamExportedKeys(FlightSqlUtils.unpackOrThrow(command, CommandGetExportedKeys.class), context, listener);
    } else if (command.is(CommandGetImportedKeys.class)) {
      getStreamImportedKeys(FlightSqlUtils.unpackOrThrow(command, CommandGetImportedKeys.class), context, listener);
    } else if (command.is(CommandGetCrossReference.class)) {
      getStreamCrossReference(FlightSqlUtils.unpackOrThrow(command, CommandGetCrossReference.class), context, listener);
    } else if (command.is(CommandGetXdbcTypeInfo.class)) {
      getStreamTypeInfo(FlightSqlUtils.unpackOrThrow(command, CommandGetXdbcTypeInfo.class), context, listener);
    } else {
      throw CallStatus.INVALID_ARGUMENT.withDescription("The defined request is invalid.").toRuntimeException();
    }
  }

  /**
   * Depending on the provided command, method either:
   * 1. Execute provided SQL query as an update statement, or
   * 2. Execute provided update SQL query prepared statement. In this case, parameters binding
   * is allowed, or
   * 3. Binds parameters to the provided prepared statement.
   *
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The data stream listener for update result acknowledgement.
   * @return a Runnable to process the stream.
   */
  @Override
  default Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
    final Any command = FlightSqlUtils.parseOrThrow(flightStream.getDescriptor().getCommand());

    if (command.is(CommandStatementUpdate.class)) {
      return acceptPutStatement(
          FlightSqlUtils.unpackOrThrow(command, CommandStatementUpdate.class),
          context, flightStream, ackStream);
    } else if (command.is(CommandPreparedStatementUpdate.class)) {
      return acceptPutPreparedStatementUpdate(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementUpdate.class),
          context, flightStream, ackStream);
    } else if (command.is(CommandPreparedStatementQuery.class)) {
      return acceptPutPreparedStatementQuery(
          FlightSqlUtils.unpackOrThrow(command, CommandPreparedStatementQuery.class),
          context, flightStream, ackStream);
    }

    throw CallStatus.INVALID_ARGUMENT.withDescription("The defined request is invalid.").toRuntimeException();
  }

  /**
   * Lists all available Flight SQL actions.
   *
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  @Override
  default void listActions(CallContext context, StreamListener<ActionType> listener) {
    FlightSqlUtils.FLIGHT_SQL_ACTIONS.forEach(listener::onNext);
    listener.onCompleted();
  }

  /**
   * Performs the requested Flight SQL action.
   *
   * @param context  Per-call context.
   * @param action   Client-supplied parameters.
   * @param listener A stream of responses.
   */
  @Override
  default void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    final String actionType = action.getType();
    if (actionType.equals(FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType())) {
      final ActionCreatePreparedStatementRequest request = FlightSqlUtils.unpackAndParseOrThrow(action.getBody(),
          ActionCreatePreparedStatementRequest.class);
      createPreparedStatement(request, context, listener);
    } else if (actionType.equals(FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType())) {
      final ActionClosePreparedStatementRequest request = FlightSqlUtils.unpackAndParseOrThrow(action.getBody(),
          ActionClosePreparedStatementRequest.class);
      closePreparedStatement(request, context, listener);
    } else {
      throw CallStatus.INVALID_ARGUMENT.withDescription("Invalid action provided.").toRuntimeException();
    }
  }

  /**
   * Creates a prepared statement on the server and returns a handle and metadata for in a
   * {@link ActionCreatePreparedStatementResult} object in a {@link Result}
   * object.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  void createPreparedStatement(ActionCreatePreparedStatementRequest request, CallContext context,
                               StreamListener<Result> listener);

  /**
   * Closes a prepared statement on the server. No result is expected.
   *
   * @param request  The sql command to generate the prepared statement.
   * @param context  Per-call context.
   * @param listener A stream of responses.
   */
  void closePreparedStatement(ActionClosePreparedStatementRequest request, CallContext context,
                              StreamListener<Result> listener);

  /**
   * Gets information about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoStatement(CommandStatementQuery command, CallContext context,
                                    FlightDescriptor descriptor);

  /**
   * Gets information about a particular prepared statement data stream.
   *
   * @param command    The prepared statement to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery command,
                                            CallContext context, FlightDescriptor descriptor);

  /**
   * Gets schema about a particular SQL query based data stream.
   *
   * @param command    The sql command to generate the data stream.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Schema for the stream.
   */
  SchemaResult getSchemaStatement(CommandStatementQuery command, CallContext context,
                                  FlightDescriptor descriptor);

  /**
   * Returns data for a SQL query based data stream.
   * @param ticket   Ticket message containing the statement handle.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamStatement(TicketStatementQuery ticket, CallContext context,
                          ServerStreamListener listener);

  /**
   * Returns data for a particular prepared statement query instance.
   *
   * @param command  The prepared statement to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamPreparedStatement(CommandPreparedStatementQuery command, CallContext context,
                                  ServerStreamListener listener);

  /**
   * Accepts uploaded data for a particular SQL query based data stream.
   * <p>`PutResult`s must be in the form of a {@link DoPutUpdateResult}.
   *
   * @param command      The sql command to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  Runnable acceptPutStatement(CommandStatementUpdate command, CallContext context,
                              FlightStream flightStream, StreamListener<PutResult> ackStream);

  /**
   * Accepts uploaded data for a particular prepared statement data stream.
   * <p>`PutResult`s must be in the form of a {@link DoPutUpdateResult}.
   *
   * @param command      The prepared statement to generate the data stream.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate command,
                                            CallContext context, FlightStream flightStream,
                                            StreamListener<PutResult> ackStream);

  /**
   * Accepts uploaded parameter values for a particular prepared statement query.
   *
   * @param command      The prepared statement the parameter values will bind to.
   * @param context      Per-call context.
   * @param flightStream The data stream being uploaded.
   * @param ackStream    The result data stream.
   * @return A runnable to process the stream.
   */
  Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery command,
                                           CallContext context, FlightStream flightStream,
                                           StreamListener<PutResult> ackStream);

  /**
   * Returns the SQL Info of the server by returning a
   * {@link CommandGetSqlInfo} in a {@link Result}.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo request, CallContext context,
                                  FlightDescriptor descriptor);

  /**
   * Returns data for SQL info based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamSqlInfo(CommandGetSqlInfo command, CallContext context, ServerStreamListener listener);


  /**
   * Returns a description of all the data types supported by source.
   * 
   * @param request     request filter parameters.
   * @param descriptor  The descriptor identifying the data stream.
   * @return  Metadata about the stream.
   */
  FlightInfo getFlightInfoTypeInfo(CommandGetXdbcTypeInfo request, CallContext context,
                                   FlightDescriptor descriptor);

  /**
   * Returns data for type info based data stream.
   *
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamTypeInfo(CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener);

  /**
   * Returns the available catalogs by returning a stream of
   * {@link CommandGetCatalogs} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoCatalogs(CommandGetCatalogs request, CallContext context,
                                   FlightDescriptor descriptor);

  /**
   * Returns data for catalogs based data stream.
   *
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamCatalogs(CallContext context, ServerStreamListener listener);

  /**
   * Returns the available schemas by returning a stream of
   * {@link CommandGetDbSchemas} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoSchemas(CommandGetDbSchemas request, CallContext context,
                                  FlightDescriptor descriptor);

  /**
   * Returns data for schemas based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamSchemas(CommandGetDbSchemas command, CallContext context, ServerStreamListener listener);

  /**
   * Returns the available tables by returning a stream of
   * {@link CommandGetTables} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoTables(CommandGetTables request, CallContext context,
                                 FlightDescriptor descriptor);

  /**
   * Returns data for tables based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamTables(CommandGetTables command, CallContext context, ServerStreamListener listener);

  /**
   * Returns the available table types by returning a stream of
   * {@link CommandGetTableTypes} objects in {@link Result} objects.
   *
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoTableTypes(CommandGetTableTypes request, CallContext context,
                                     FlightDescriptor descriptor);

  /**
   * Returns data for table types based data stream.
   *
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamTableTypes(CallContext context, ServerStreamListener listener);

  /**
   * Returns the available primary keys by returning a stream of
   * {@link CommandGetPrimaryKeys} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoPrimaryKeys(CommandGetPrimaryKeys request, CallContext context,
                                      FlightDescriptor descriptor);

  /**
   * Returns data for primary keys based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamPrimaryKeys(CommandGetPrimaryKeys command, CallContext context,
                            ServerStreamListener listener);

  /**
   * Retrieves a description of the foreign key columns that reference the given table's primary key columns
   * {@link CommandGetExportedKeys} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoExportedKeys(CommandGetExportedKeys request, CallContext context,
                                       FlightDescriptor descriptor);

  /**
   * Retrieves a description of the primary key columns that are referenced by given table's foreign key columns
   * {@link CommandGetImportedKeys} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoImportedKeys(CommandGetImportedKeys request, CallContext context,
                                       FlightDescriptor descriptor);

  /**
   * Retrieve a description of the foreign key columns that reference the given table's primary key columns
   * {@link CommandGetCrossReference} objects in {@link Result} objects.
   *
   * @param request    request filter parameters.
   * @param context    Per-call context.
   * @param descriptor The descriptor identifying the data stream.
   * @return Metadata about the stream.
   */
  FlightInfo getFlightInfoCrossReference(CommandGetCrossReference request, CallContext context,
                                         FlightDescriptor descriptor);

  /**
   * Returns data for foreign keys based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamExportedKeys(CommandGetExportedKeys command, CallContext context,
                             ServerStreamListener listener);

  /**
   * Returns data for foreign keys based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamImportedKeys(CommandGetImportedKeys command, CallContext context,
                             ServerStreamListener listener);

  /**
   * Returns data for cross reference based data stream.
   *
   * @param command  The command to generate the data stream.
   * @param context  Per-call context.
   * @param listener An interface for sending data back to the client.
   */
  void getStreamCrossReference(CommandGetCrossReference command, CallContext context,
                             ServerStreamListener listener);


  /**
   * Default schema templates for the {@link FlightSqlProducer}.
   */
  final class Schemas {
    public static final Schema GET_TABLES_SCHEMA = new Schema(asList(
        Field.nullable("catalog_name", VARCHAR.getType()),
        Field.nullable("db_schema_name", VARCHAR.getType()),
        Field.notNullable("table_name", VARCHAR.getType()),
        Field.notNullable("table_type", VARCHAR.getType()),
        Field.notNullable("table_schema", MinorType.VARBINARY.getType())));
    public static final Schema GET_TABLES_SCHEMA_NO_SCHEMA = new Schema(asList(
        Field.nullable("catalog_name", VARCHAR.getType()),
        Field.nullable("db_schema_name", VARCHAR.getType()),
        Field.notNullable("table_name", VARCHAR.getType()),
        Field.notNullable("table_type", VARCHAR.getType())));
    public static final Schema GET_CATALOGS_SCHEMA = new Schema(
        singletonList(Field.notNullable("catalog_name", VARCHAR.getType())));
    public static final Schema GET_TABLE_TYPES_SCHEMA =
        new Schema(singletonList(Field.notNullable("table_type", VARCHAR.getType())));
    public static final Schema GET_SCHEMAS_SCHEMA =
        new Schema(asList(
            Field.nullable("catalog_name", VARCHAR.getType()),
            Field.notNullable("db_schema_name", VARCHAR.getType())));
    private static final Schema GET_IMPORTED_EXPORTED_AND_CROSS_REFERENCE_KEYS_SCHEMA =
        new Schema(asList(
            Field.nullable("pk_catalog_name", VARCHAR.getType()),
            Field.nullable("pk_db_schema_name", VARCHAR.getType()),
            Field.notNullable("pk_table_name", VARCHAR.getType()),
            Field.notNullable("pk_column_name", VARCHAR.getType()),
            Field.nullable("fk_catalog_name", VARCHAR.getType()),
            Field.nullable("fk_db_schema_name", VARCHAR.getType()),
            Field.notNullable("fk_table_name", VARCHAR.getType()),
            Field.notNullable("fk_column_name", VARCHAR.getType()),
            Field.notNullable("key_sequence", INT.getType()),
            Field.nullable("fk_key_name", VARCHAR.getType()),
            Field.nullable("pk_key_name", VARCHAR.getType()),
            Field.notNullable("update_rule", MinorType.UINT1.getType()),
            Field.notNullable("delete_rule", MinorType.UINT1.getType())));
    public static final Schema GET_IMPORTED_KEYS_SCHEMA = GET_IMPORTED_EXPORTED_AND_CROSS_REFERENCE_KEYS_SCHEMA;
    public static final Schema GET_EXPORTED_KEYS_SCHEMA = GET_IMPORTED_EXPORTED_AND_CROSS_REFERENCE_KEYS_SCHEMA;
    public static final Schema GET_CROSS_REFERENCE_SCHEMA = GET_IMPORTED_EXPORTED_AND_CROSS_REFERENCE_KEYS_SCHEMA;
    private static final List<Field> GET_SQL_INFO_DENSE_UNION_SCHEMA_FIELDS = asList(
        Field.notNullable("string_value", VARCHAR.getType()),
        Field.notNullable("bool_value", BIT.getType()),
        Field.notNullable("bigint_value", BIGINT.getType()),
        Field.notNullable("int32_bitmask", INT.getType()),
        new Field(
            "string_list", FieldType.notNullable(LIST.getType()),
            singletonList(Field.nullable("item", VARCHAR.getType()))),
        new Field(
            "int32_to_int32_list_map", FieldType.notNullable(new ArrowType.Map(false)),
            singletonList(new Field(DATA_VECTOR_NAME, new FieldType(false, STRUCT.getType(), null),
                ImmutableList.of(
                    Field.notNullable(KEY_NAME, INT.getType()),
                    new Field(
                        VALUE_NAME, FieldType.nullable(LIST.getType()),
                        singletonList(Field.nullable("item", INT.getType()))))))));
    public static final Schema GET_SQL_INFO_SCHEMA =
        new Schema(asList(
            Field.notNullable("info_name", UINT4.getType()),
            new Field("value",
                FieldType.notNullable(
                    new Union(UnionMode.Dense, range(0, GET_SQL_INFO_DENSE_UNION_SCHEMA_FIELDS.size()).toArray())),
                GET_SQL_INFO_DENSE_UNION_SCHEMA_FIELDS)));
    public static final Schema GET_TYPE_INFO_SCHEMA =
        new Schema(asList(
            Field.notNullable("type_name", VARCHAR.getType()),
            Field.notNullable("data_type", INT.getType()),
            Field.nullable("column_size", INT.getType()),
            Field.nullable("literal_prefix", VARCHAR.getType()),
            Field.nullable("literal_suffix", VARCHAR.getType()),
            new Field(
                "create_params", FieldType.nullable(LIST.getType()),
                singletonList(Field.notNullable("item", VARCHAR.getType()))),
            Field.notNullable("nullable", INT.getType()),
            Field.notNullable("case_sensitive", BIT.getType()),
            Field.notNullable("searchable", INT.getType()),
            Field.nullable("unsigned_attribute", BIT.getType()),
            Field.notNullable("fixed_prec_scale", BIT.getType()),
            Field.nullable("auto_increment", BIT.getType()),
            Field.nullable("local_type_name", VARCHAR.getType()),
            Field.nullable("minimum_scale", INT.getType()),
            Field.nullable("maximum_scale", INT.getType()),
            Field.notNullable("sql_data_type", INT.getType()),
            Field.nullable("datetime_subcode", INT.getType()),
            Field.nullable("num_prec_radix", INT.getType()),
            Field.nullable("interval_precision", INT.getType())
        ));
    public static final Schema GET_PRIMARY_KEYS_SCHEMA =
        new Schema(asList(
            Field.nullable("catalog_name", VARCHAR.getType()),
            Field.nullable("db_schema_name", VARCHAR.getType()),
            Field.notNullable("table_name", VARCHAR.getType()),
            Field.notNullable("column_name", VARCHAR.getType()),
            Field.notNullable("key_sequence", INT.getType()),
            Field.nullable("key_name", VARCHAR.getType())));

    private Schemas() {
      // Prevent instantiation.
    }
  }
}
