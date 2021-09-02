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

package org.apache.arrow.driver.jdbc.test.adhoc;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.util.UUID.randomUUID;

import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import org.apache.arrow.flight.sql.impl.FlightSql.TicketStatementQuery;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

/**
 * An ad-hoc {@link FlightSqlProducer} for tests.
 */
public final class MockFlightSqlProducer implements FlightSqlProducer {

  private static final IpcOption DEFAULT_OPTION = IpcOption.DEFAULT;

  private final Map<String, Entry<Schema, List<UUID>>> queryResults = new HashMap<>();
  private final Map<UUID, Consumer<ServerStreamListener>> resultProviders = new HashMap<>();
  private final Map<ByteString, String> preparedStatements = new HashMap<>();

  /**
   * Adds support for a new query.
   *
   * @param sqlCommand      the SQL command under which to register the new query.
   * @param schema          the schema to use for the query result.
   * @param resultProviders the result provider for this query.
   */
  public void addQuery(final String sqlCommand, final Schema schema,
                       final List<Consumer<ServerStreamListener>> resultProviders) {
    final int providers = resultProviders.size();
    final List<UUID> uuids =
        IntStream.range(0, providers)
            .mapToObj(index -> new UUID(sqlCommand.hashCode(), Integer.hashCode(index)))
            .collect(Collectors.toList());
    queryResults.put(sqlCommand, new SimpleImmutableEntry<>(schema, uuids));
    IntStream.range(0, providers)
        .forEach(index -> this.resultProviders.put(uuids.get(index), resultProviders.get(index)));
  }


  @Override
  public void createPreparedStatement(ActionCreatePreparedStatementRequest request,
                                      CallContext callContext, StreamListener<Result> listener) {
    try {
      final ByteString preparedStatementHandle = copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
      final String query = request.getQuery();

      final Entry<Schema, List<UUID>> entry = queryResults.get(query);
      if (entry == null) {
        listener.onError(CallStatus.INVALID_ARGUMENT.withDescription("Query not found").toRuntimeException());
        return;
      }

      preparedStatements.put(preparedStatementHandle, query);

      final Schema datasetSchema = entry.getKey();
      final ByteString datasetSchemaBytes =
          ByteString.copyFrom(MessageSerializer.serializeMetadata(datasetSchema, DEFAULT_OPTION));

      final FlightSql.ActionCreatePreparedStatementResult result =
          FlightSql.ActionCreatePreparedStatementResult.newBuilder()
              .setDatasetSchema(datasetSchemaBytes)
              .setPreparedStatementHandle(preparedStatementHandle)
              .build();
      listener.onNext(new Result(pack(result).toByteArray()));
    } catch (final Throwable t) {
      listener.onError(t);
    } finally {
      listener.onCompleted();
    }
  }

  @Override
  public void closePreparedStatement(ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
                                     CallContext callContext, StreamListener<Result> streamListener) {
    // TODO Implement this method.
    streamListener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(final CommandStatementQuery commandStatementQuery,
                                           final CallContext callContext,
                                           final FlightDescriptor flightDescriptor) {
    final String query = commandStatementQuery.getQuery();
    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query), String.format("Query not registered: <%s>.", query));
    final List<FlightEndpoint> endpoints =
        queryInfo.getValue().stream()
            .map(UUID::toString)
            .map(ByteString::copyFromUtf8)
            .map(TicketStatementQuery.newBuilder()::setStatementHandle)
            .map(TicketStatementQuery.Builder::build)
            .map(Any::pack)
            .map(Any::toByteArray)
            .map(Ticket::new)
            .map(FlightEndpoint::new)
            .collect(Collectors.toList());
    return new FlightInfo(queryInfo.getKey(), flightDescriptor, endpoints, -1, -1);
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(CommandPreparedStatementQuery commandPreparedStatementQuery,
                                                   CallContext callContext, FlightDescriptor flightDescriptor) {
    final ByteString preparedStatementHandle = commandPreparedStatementQuery.getPreparedStatementHandle();

    final String query = preparedStatements.get(preparedStatementHandle);
    if (query == null) {
      throw CallStatus.NOT_FOUND.toRuntimeException();
    }

    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query), String.format("Query not registered: <%s>.", query));
    final List<FlightEndpoint> endpoints =
        queryInfo.getValue().stream()
            .map(UUID::toString)
            .map(ByteString::copyFromUtf8)
            .map(CommandPreparedStatementQuery.newBuilder()::setPreparedStatementHandle)
            .map(CommandPreparedStatementQuery.Builder::build)
            .map(Any::pack)
            .map(Any::toByteArray)
            .map(Ticket::new)
            .map(FlightEndpoint::new)
            .collect(Collectors.toList());
    return new FlightInfo(queryInfo.getKey(), flightDescriptor, endpoints, -1, -1);
  }

  @Override
  public SchemaResult getSchemaStatement(CommandStatementQuery commandStatementQuery,
                                         CallContext callContext, FlightDescriptor flightDescriptor) {
    final String query = commandStatementQuery.getQuery();
    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query), String.format("Query not registered: <%s>.", query));

    return new SchemaResult(queryInfo.getKey());
  }

  @Override
  public void getStreamStatement(final TicketStatementQuery ticketStatementQuery, final CallContext callContext,
                                 final Ticket ticket, final ServerStreamListener serverStreamListener) {
    final UUID uuid = UUID.fromString(ticketStatementQuery.getStatementHandle().toStringUtf8());
    Preconditions.checkNotNull(
            resultProviders.get(uuid),
            "No consumer was registered for the specified UUID: <%s>.", uuid)
        .accept(serverStreamListener);
  }

  @Override
  public void getStreamPreparedStatement(CommandPreparedStatementQuery commandPreparedStatementQuery,
                                         CallContext callContext, Ticket ticket,
                                         ServerStreamListener serverStreamListener) {
    final UUID uuid = UUID.fromString(commandPreparedStatementQuery.getPreparedStatementHandle().toStringUtf8());
    Preconditions.checkNotNull(
            resultProviders.get(uuid),
            "No consumer was registered for the specified UUID: <%s>.", uuid)
        .accept(serverStreamListener);
  }

  @Override
  public Runnable acceptPutStatement(CommandStatementUpdate commandStatementUpdate, CallContext callContext,
                                     FlightStream flightStream, StreamListener<PutResult> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(CommandPreparedStatementUpdate commandPreparedStatementUpdate,
                                                   CallContext callContext, FlightStream flightStream,
                                                   StreamListener<PutResult> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(CommandPreparedStatementQuery commandPreparedStatementQuery,
                                                  CallContext callContext, FlightStream flightStream,
                                                  StreamListener<PutResult> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamSqlInfo(CommandGetSqlInfo commandGetSqlInfo, CallContext callContext,
                               Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(CommandGetCatalogs commandGetCatalogs, CallContext callContext,
                                          FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamCatalogs(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSchemas(CommandGetSchemas commandGetSchemas, CallContext callContext,
                                         FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamSchemas(CommandGetSchemas commandGetSchemas, CallContext callContext,
                               Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTables(CommandGetTables commandGetTables, CallContext callContext,
                                        FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamTables(CommandGetTables commandGetTables, CallContext callContext,
                              Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(CommandGetTableTypes commandGetTableTypes, CallContext callContext,
                                            FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamTableTypes(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys, CallContext callContext,
                                             FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamPrimaryKeys(CommandGetPrimaryKeys commandGetPrimaryKeys, CallContext callContext,
                                   Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(CommandGetExportedKeys commandGetExportedKeys, CallContext callContext,
                                              FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(CommandGetImportedKeys commandGetImportedKeys, CallContext callContext,
                                              FlightDescriptor flightDescriptor) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamExportedKeys(CommandGetExportedKeys commandGetExportedKeys, CallContext callContext,
                                    Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void getStreamImportedKeys(CommandGetImportedKeys commandGetImportedKeys, CallContext callContext,
                                    Ticket ticket, ServerStreamListener serverStreamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void close() {
    // TODO No-op.
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }
}
