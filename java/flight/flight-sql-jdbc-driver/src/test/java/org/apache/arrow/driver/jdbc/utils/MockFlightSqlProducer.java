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

package org.apache.arrow.driver.jdbc.utils;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.arrow.flight.Action;
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
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.Meta.StatementType;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

/**
 * An ad-hoc {@link FlightSqlProducer} for tests.
 */
public final class MockFlightSqlProducer implements FlightSqlProducer {

  private final Map<String, Entry<Schema, List<UUID>>> queryResults = new HashMap<>();
  private final Map<UUID, Consumer<ServerStreamListener>> selectResultProviders = new HashMap<>();
  private final Map<ByteString, String> preparedStatements = new HashMap<>();
  private final Map<Message, Consumer<ServerStreamListener>> catalogQueriesResults =
      new HashMap<>();
  private final Map<String, BiConsumer<FlightStream, StreamListener<PutResult>>>
      updateResultProviders =
      new HashMap<>();
  private SqlInfoBuilder sqlInfoBuilder = new SqlInfoBuilder();

  private final Map<String, Integer> actionTypeCounter = new HashMap<>();

  private static FlightInfo getFightInfoExportedAndImportedKeys(final Message message,
                                                                final FlightDescriptor descriptor) {
    return getFlightInfo(message, Schemas.GET_IMPORTED_KEYS_SCHEMA, descriptor);
  }

  private static FlightInfo getFlightInfo(final Message message, final Schema schema,
                                          final FlightDescriptor descriptor) {
    return new FlightInfo(
        schema,
        descriptor,
        Collections.singletonList(new FlightEndpoint(new Ticket(Any.pack(message).toByteArray()))),
        -1, -1);
  }

  public static ByteBuffer serializeSchema(final Schema schema) {
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);

      return ByteBuffer.wrap(outputStream.toByteArray());
    } catch (final IOException e) {
      throw new RuntimeException("Failed to serialize schema", e);
    }
  }

  /**
   * Registers a new {@link StatementType#SELECT} SQL query.
   *
   * @param sqlCommand      the SQL command under which to register the new query.
   * @param schema          the schema to use for the query result.
   * @param resultProviders the result provider for this query.
   */
  public void addSelectQuery(final String sqlCommand, final Schema schema,
                             final List<Consumer<ServerStreamListener>> resultProviders) {
    final int providers = resultProviders.size();
    final List<UUID> uuids =
        IntStream.range(0, providers)
            .mapToObj(index -> new UUID(sqlCommand.hashCode(), Integer.hashCode(index)))
            .collect(toList());
    queryResults.put(sqlCommand, new SimpleImmutableEntry<>(schema, uuids));
    IntStream.range(0, providers)
        .forEach(
            index -> this.selectResultProviders.put(uuids.get(index), resultProviders.get(index)));
  }

  /**
   * Registers a new {@link StatementType#UPDATE} SQL query.
   *
   * @param sqlCommand  the SQL command.
   * @param updatedRows the number of rows affected.
   */
  public void addUpdateQuery(final String sqlCommand, final long updatedRows) {
    addUpdateQuery(sqlCommand, ((flightStream, putResultStreamListener) -> {
      final DoPutUpdateResult result =
          DoPutUpdateResult.newBuilder().setRecordCount(updatedRows).build();
      try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
           final ArrowBuf buffer = allocator.buffer(result.getSerializedSize())) {
        buffer.writeBytes(result.toByteArray());
        putResultStreamListener.onNext(PutResult.metadata(buffer));
      } catch (final Throwable throwable) {
        putResultStreamListener.onError(throwable);
      } finally {
        putResultStreamListener.onCompleted();
      }
    }));
  }

  /**
   * Adds a catalog query to the results.
   *
   * @param message         the {@link Message} corresponding to the catalog query request type to register.
   * @param resultsProvider the results provider.
   */
  public void addCatalogQuery(final Message message,
                              final Consumer<ServerStreamListener> resultsProvider) {
    catalogQueriesResults.put(message, resultsProvider);
  }

  /**
   * Registers a new {@link StatementType#UPDATE} SQL query.
   *
   * @param sqlCommand      the SQL command.
   * @param resultsProvider consumer for producing update results.
   */
  void addUpdateQuery(final String sqlCommand,
                      final BiConsumer<FlightStream, StreamListener<PutResult>> resultsProvider) {
    Preconditions.checkState(
        updateResultProviders.putIfAbsent(sqlCommand, resultsProvider) == null,
        format("Attempted to overwrite pre-existing query: <%s>.", sqlCommand));
  }

  @Override
  public void createPreparedStatement(final ActionCreatePreparedStatementRequest request,
                                      final CallContext callContext,
                                      final StreamListener<Result> listener) {
    try {
      final ByteString preparedStatementHandle =
          copyFrom(randomUUID().toString().getBytes(StandardCharsets.UTF_8));
      final String query = request.getQuery();

      final ActionCreatePreparedStatementResult.Builder resultBuilder =
          ActionCreatePreparedStatementResult.newBuilder()
              .setPreparedStatementHandle(preparedStatementHandle);

      final Entry<Schema, List<UUID>> entry = queryResults.get(query);
      if (entry != null) {
        preparedStatements.put(preparedStatementHandle, query);

        final Schema datasetSchema = entry.getKey();
        final ByteString datasetSchemaBytes =
            ByteString.copyFrom(serializeSchema(datasetSchema));

        resultBuilder.setDatasetSchema(datasetSchemaBytes);
      } else if (updateResultProviders.containsKey(query)) {
        preparedStatements.put(preparedStatementHandle, query);

      } else {
        listener.onError(
            CallStatus.INVALID_ARGUMENT.withDescription("Query not found").toRuntimeException());
        return;
      }

      listener.onNext(new Result(pack(resultBuilder.build()).toByteArray()));
    } catch (final Throwable t) {
      listener.onError(t);
    } finally {
      listener.onCompleted();
    }
  }

  @Override
  public void closePreparedStatement(
      final ActionClosePreparedStatementRequest actionClosePreparedStatementRequest,
      final CallContext callContext, final StreamListener<Result> streamListener) {
    // TODO Implement this method.
    streamListener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(final CommandStatementQuery commandStatementQuery,
                                           final CallContext callContext,
                                           final FlightDescriptor flightDescriptor) {
    final String query = commandStatementQuery.getQuery();
    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query),
            format("Query not registered: <%s>.", query));
    final List<FlightEndpoint> endpoints =
        queryInfo.getValue().stream()
            .map(TicketConversionUtils::getTicketBytesFromUuid)
            .map(TicketConversionUtils::getTicketStatementQueryFromHandle)
            .map(TicketConversionUtils::getEndpointFromMessage)
            .collect(toList());
    return new FlightInfo(queryInfo.getKey(), flightDescriptor, endpoints, -1, -1);
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(
      final CommandPreparedStatementQuery commandPreparedStatementQuery,
      final CallContext callContext,
      final FlightDescriptor flightDescriptor) {
    final ByteString preparedStatementHandle =
        commandPreparedStatementQuery.getPreparedStatementHandle();

    final String query = Preconditions.checkNotNull(
        preparedStatements.get(preparedStatementHandle),
        format("No query registered under handle: <%s>.", preparedStatementHandle));
    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query),
            format("Query not registered: <%s>.", query));
    final List<FlightEndpoint> endpoints =
        queryInfo.getValue().stream()
            .map(TicketConversionUtils::getTicketBytesFromUuid)
            .map(TicketConversionUtils::getCommandPreparedStatementQueryFromHandle)
            .map(TicketConversionUtils::getEndpointFromMessage)
            .collect(toList());
    return new FlightInfo(queryInfo.getKey(), flightDescriptor, endpoints, -1, -1);
  }

  @Override
  public SchemaResult getSchemaStatement(final CommandStatementQuery commandStatementQuery,
                                         final CallContext callContext,
                                         final FlightDescriptor flightDescriptor) {
    final String query = commandStatementQuery.getQuery();
    final Entry<Schema, List<UUID>> queryInfo =
        Preconditions.checkNotNull(queryResults.get(query),
            format("Query not registered: <%s>.", query));

    return new SchemaResult(queryInfo.getKey());
  }

  @Override
  public void getStreamStatement(final TicketStatementQuery ticketStatementQuery,
                                 final CallContext callContext,
                                 final ServerStreamListener serverStreamListener) {
    final UUID uuid = UUID.fromString(ticketStatementQuery.getStatementHandle().toStringUtf8());
    Preconditions.checkNotNull(
            selectResultProviders.get(uuid),
            "No consumer was registered for the specified UUID: <%s>.", uuid)
        .accept(serverStreamListener);
  }

  @Override
  public void getStreamPreparedStatement(
      final CommandPreparedStatementQuery commandPreparedStatementQuery,
      final CallContext callContext,
      final ServerStreamListener serverStreamListener) {
    final UUID uuid =
        UUID.fromString(commandPreparedStatementQuery.getPreparedStatementHandle().toStringUtf8());
    Preconditions.checkNotNull(
            selectResultProviders.get(uuid),
            "No consumer was registered for the specified UUID: <%s>.", uuid)
        .accept(serverStreamListener);
  }

  @Override
  public Runnable acceptPutStatement(final CommandStatementUpdate commandStatementUpdate,
                                     final CallContext callContext,
                                     final FlightStream flightStream,
                                     final StreamListener<PutResult> streamListener) {
    return () -> {
      final String query = commandStatementUpdate.getQuery();
      final BiConsumer<FlightStream, StreamListener<PutResult>> resultProvider =
          Preconditions.checkNotNull(
              updateResultProviders.get(query),
              format("No consumer found for query: <%s>.", query));
      resultProvider.accept(flightStream, streamListener);
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(
      final CommandPreparedStatementUpdate commandPreparedStatementUpdate,
      final CallContext callContext, final FlightStream flightStream,
      final StreamListener<PutResult> streamListener) {
    final ByteString handle = commandPreparedStatementUpdate.getPreparedStatementHandle();
    final String query = Preconditions.checkNotNull(
        preparedStatements.get(handle),
        format("No query registered under handle: <%s>.", handle));
    return acceptPutStatement(
        CommandStatementUpdate.newBuilder().setQuery(query).build(), callContext, flightStream,
        streamListener);
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(
      final CommandPreparedStatementQuery commandPreparedStatementQuery,
      final CallContext callContext, final FlightStream flightStream,
      final StreamListener<PutResult> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(final CommandGetSqlInfo commandGetSqlInfo,
                                         final CallContext callContext,
                                         final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetSqlInfo, Schemas.GET_SQL_INFO_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamSqlInfo(final CommandGetSqlInfo commandGetSqlInfo,
                               final CallContext callContext,
                               final ServerStreamListener serverStreamListener) {
    sqlInfoBuilder.send(commandGetSqlInfo.getInfoList(), serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                          FlightDescriptor descriptor) {
    // TODO Implement this
    return null;
  }

  @Override
  public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                ServerStreamListener listener) {
    // TODO Implement this
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(final CommandGetCatalogs commandGetCatalogs,
                                          final CallContext callContext,
                                          final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetCatalogs, Schemas.GET_CATALOGS_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamCatalogs(final CallContext callContext,
                                final ServerStreamListener serverStreamListener) {
    final CommandGetCatalogs command = CommandGetCatalogs.getDefaultInstance();
    getStreamCatalogFunctions(command, serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoSchemas(final CommandGetDbSchemas commandGetSchemas,
                                         final CallContext callContext,
                                         final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetSchemas, Schemas.GET_SCHEMAS_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamSchemas(final CommandGetDbSchemas commandGetSchemas,
                               final CallContext callContext,
                               final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetSchemas, serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoTables(final CommandGetTables commandGetTables,
                                        final CallContext callContext,
                                        final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetTables, Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamTables(final CommandGetTables commandGetTables,
                              final CallContext callContext,
                              final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetTables, serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(final CommandGetTableTypes commandGetTableTypes,
                                            final CallContext callContext,
                                            final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetTableTypes, Schemas.GET_TABLE_TYPES_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamTableTypes(final CallContext callContext,
                                  final ServerStreamListener serverStreamListener) {
    final CommandGetTableTypes command = CommandGetTableTypes.getDefaultInstance();
    getStreamCatalogFunctions(command, serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(final CommandGetPrimaryKeys commandGetPrimaryKeys,
                                             final CallContext callContext,
                                             final FlightDescriptor flightDescriptor) {
    return getFlightInfo(commandGetPrimaryKeys, Schemas.GET_PRIMARY_KEYS_SCHEMA, flightDescriptor);
  }

  @Override
  public void getStreamPrimaryKeys(final CommandGetPrimaryKeys commandGetPrimaryKeys,
                                   final CallContext callContext,
                                   final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetPrimaryKeys, serverStreamListener);
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(final CommandGetExportedKeys commandGetExportedKeys,
                                              final CallContext callContext,
                                              final FlightDescriptor flightDescriptor) {
    return getFightInfoExportedAndImportedKeys(commandGetExportedKeys, flightDescriptor);
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(final CommandGetImportedKeys commandGetImportedKeys,
                                              final CallContext callContext,
                                              final FlightDescriptor flightDescriptor) {
    return getFightInfoExportedAndImportedKeys(commandGetImportedKeys, flightDescriptor);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(
      final CommandGetCrossReference commandGetCrossReference,
      final CallContext callContext,
      final FlightDescriptor flightDescriptor) {
    return getFightInfoExportedAndImportedKeys(commandGetCrossReference, flightDescriptor);
  }

  @Override
  public void getStreamExportedKeys(final CommandGetExportedKeys commandGetExportedKeys,
                                    final CallContext callContext,
                                    final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetExportedKeys, serverStreamListener);
  }

  @Override
  public void getStreamImportedKeys(final CommandGetImportedKeys commandGetImportedKeys,
                                    final CallContext callContext,
                                    final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetImportedKeys, serverStreamListener);
  }

  @Override
  public void getStreamCrossReference(final CommandGetCrossReference commandGetCrossReference,
                                      final CallContext callContext,
                                      final ServerStreamListener serverStreamListener) {
    getStreamCatalogFunctions(commandGetCrossReference, serverStreamListener);
  }

  @Override
  public void close() {
    // TODO No-op.
  }

  @Override
  public void listFlights(final CallContext callContext, final Criteria criteria,
                          final StreamListener<FlightInfo> streamListener) {
    // TODO Implement this method.
    throw CallStatus.UNIMPLEMENTED.toRuntimeException();
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    FlightSqlProducer.super.doAction(context, action, listener);
    actionTypeCounter.put(action.getType(), actionTypeCounter.getOrDefault(action.getType(), 0) + 1);
  }

  /**
   * Clear the `actionTypeCounter` map and restore to its default state. Intended to be used in tests.
   */
  public void clearActionTypeCounter() {
    actionTypeCounter.clear();
  }

  public Map<String, Integer> getActionTypeCounter() {
    return actionTypeCounter;
  }


  private void getStreamCatalogFunctions(final Message ticket,
                                         final ServerStreamListener serverStreamListener) {
    Preconditions.checkNotNull(
            catalogQueriesResults.get(ticket),
            format("Query not registered for ticket: <%s>", ticket))
        .accept(serverStreamListener);
  }

  public SqlInfoBuilder getSqlInfoBuilder() {
    return sqlInfoBuilder;
  }

  private static final class TicketConversionUtils {
    private TicketConversionUtils() {
      // Prevent instantiation.
    }

    private static ByteString getTicketBytesFromUuid(final UUID uuid) {
      return ByteString.copyFromUtf8(uuid.toString());
    }

    private static TicketStatementQuery getTicketStatementQueryFromHandle(final ByteString handle) {
      return TicketStatementQuery.newBuilder().setStatementHandle(handle).build();
    }

    private static CommandPreparedStatementQuery getCommandPreparedStatementQueryFromHandle(
        final ByteString handle) {
      return CommandPreparedStatementQuery.newBuilder().setPreparedStatementHandle(handle).build();
    }

    private static FlightEndpoint getEndpointFromMessage(final Message message) {
      return new FlightEndpoint(new Ticket(Any.pack(message).toByteArray()));
    }
  }
}
