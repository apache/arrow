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

import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;

import io.grpc.Status;

/**
 * Flight client with Flight SQL semantics.
 */
public class FlightSqlClient {
  private FlightClient client;

  public FlightSqlClient(FlightClient client) {
    this.client = client;
  }

  /**
   * Execute a query on the server.
   *
   * @param query The query to execute.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo execute(String query) {
    final CommandStatementQuery.Builder builder = CommandStatementQuery.newBuilder();
    builder.setQuery(query);
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Execute an update query on the server.
   *
   * @param query The query to execute.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public long executeUpdate(String query) {
    final CommandStatementUpdate.Builder builder = CommandStatementUpdate.newBuilder();
    builder.setQuery(query);

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    final SyncPutListener putListener = new SyncPutListener();
    client.startPut(descriptor, VectorSchemaRoot.of(), putListener);

    try {
      final PutResult read = putListener.read();
      try (final ArrowBuf metadata = read.getApplicationMetadata()) {
        final DoPutUpdateResult doPutUpdateResult = DoPutUpdateResult.parseFrom(metadata.nioBuffer());
        return doPutUpdateResult.getRecordCount();
      }
    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Request a list of catalogs.
   *
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getCatalogs() {
    final CommandGetCatalogs.Builder builder = CommandGetCatalogs.newBuilder();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request a list of schemas.
   *
   * @param catalog             The catalog.
   * @param schemaFilterPattern The schema filter pattern.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSchemas(final String catalog, final String schemaFilterPattern) {
    final CommandGetSchemas.Builder builder = CommandGetSchemas.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schemaFilterPattern != null) {
      builder.setSchemaFilterPattern(StringValue.newBuilder().setValue(schemaFilterPattern).build());
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Get schema for a stream.
   *
   * @param descriptor The descriptor for the stream.
   * @param options    RPC-layer hints for this call.
   */
  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... options) {
    return this.client.getSchema(descriptor, options);
  }

  /**
   * Retrieve a stream from the server.
   *
   * @param ticket  The ticket granting access to the data stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightStream getStream(Ticket ticket, CallOption... options) {
    return this.client.getStream(ticket, options);
  }

  /**
   * Request a set of Flight SQL metadata.
   *
   * @param info The set of metadata to retrieve. None to retrieve all metadata.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final @Nullable int... info) {
    final CommandGetSqlInfo.Builder builder = CommandGetSqlInfo.newBuilder();
    for (final int pieceOfInfo : Objects.isNull(info) ? new int[0] : info) {
      builder.addInfo(pieceOfInfo);
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request a list of tables.
   *
   * @param catalog             The catalog.
   * @param schemaFilterPattern The schema filter pattern.
   * @param tableFilterPattern  The table filter pattern.
   * @param tableTypes          The table types to include.
   * @param includeSchema       True to include the schema upon return, false to not include the schema.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTables(final @Nullable String catalog, final @Nullable String schemaFilterPattern,
                              final @Nullable String tableFilterPattern, final List<String> tableTypes,
                              final boolean includeSchema) {
    final CommandGetTables.Builder builder = CommandGetTables.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schemaFilterPattern != null) {
      builder.setSchemaFilterPattern(StringValue.newBuilder().setValue(schemaFilterPattern).build());
    }

    if (tableFilterPattern != null) {
      builder.setTableNameFilterPattern(StringValue.newBuilder().setValue(tableFilterPattern).build());
    }

    if (tableTypes != null) {
      builder.addAllTableTypes(tableTypes);
    }
    builder.setIncludeSchema(includeSchema);

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request the primary keys for a table.
   *
   * @param catalog The catalog.
   * @param schema  The schema.
   * @param table   The table.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getPrimaryKeys(final @Nullable String catalog, final @Nullable String schema,
                                   final @Nullable String table) {
    final CommandGetPrimaryKeys.Builder builder = CommandGetPrimaryKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    if (table != null) {
      builder.setTable(StringValue.newBuilder().setValue(table).build());
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request to get info about keys on a table. The table, which exports the foreign keys, parameter must be specified.
   *
   * @param catalog The foreign key table catalog.
   * @param schema  The foreign key table schema.
   * @param table   The foreign key table.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getExportedKeys(String catalog, String schema, String table) {
    if (null == table) {
      throw Status.INVALID_ARGUMENT.asRuntimeException();
    }

    final CommandGetExportedKeys.Builder builder = CommandGetExportedKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    builder.setTable(table).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request to get info about keys on a table. The table, which imports the foreign keys, parameter must be specified.
   *
   * @param catalog The primary key table catalog.
   * @param schema  The primary key table schema.
   * @param table   The primary key table.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getImportedKeys(String catalog, String schema, String table) {
    if (null == table) {
      throw Status.INVALID_ARGUMENT.asRuntimeException();
    }

    final CommandGetImportedKeys.Builder builder = CommandGetImportedKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    builder.setTable(table).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request a list of table types.
   *
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTableTypes() {
    final CommandGetTableTypes.Builder builder = CommandGetTableTypes.newBuilder();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Create a prepared statement on the server.
   *
   * @param query The query to prepare.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(String query) {
    return new PreparedStatement(client, query);
  }

  /**
   * Helper class to encapsulate Flight SQL prepared statement logic.
   */
  public static class PreparedStatement implements Closeable {
    private final FlightClient client;
    private final ActionCreatePreparedStatementResult preparedStatementResult;
    private AtomicLong invocationCount;
    private boolean isClosed;
    private Schema resultSetSchema = null;
    private Schema parameterSchema = null;
    private VectorSchemaRoot parameterBindingRoot;

    /**
     * Constructor.
     *
     * @param client The client. FlightSqlPreparedStatement does not maintain this resource.
     * @param sql    The query.
     */
    public PreparedStatement(FlightClient client, String sql) {
      this.client = client;

      final Iterator<Result> preparedStatementResults = client.doAction(new Action(
          FlightSqlUtils.FLIGHT_SQL_CREATEPREPAREDSTATEMENT.getType(),
          Any.pack(ActionCreatePreparedStatementRequest
              .newBuilder()
              .setQuery(sql)
              .build())
              .toByteArray()));

      preparedStatementResult = FlightSqlUtils.unpackAndParseOrThrow(
          preparedStatementResults.next().getBody(),
          ActionCreatePreparedStatementResult.class);

      invocationCount = new AtomicLong(0);
      isClosed = false;
    }

    /**
     * Set the {@link VectorSchemaRoot} containing the parameter binding from a preparedStatemnt
     * operation.
     *
     * @param parameterBindingRoot  a {@link VectorSchemaRoot} object contain the values to be used in the
     *                              PreparedStatement setters.
     */
    public void setParameters(VectorSchemaRoot parameterBindingRoot) {
      this.parameterBindingRoot = parameterBindingRoot;
    }

    /**
     * Returns the Schema of the resultset.
     *
     * @return the Schema of the resultset.
     */
    public Schema getResultSetSchema() {
      if (resultSetSchema == null && preparedStatementResult.getDatasetSchema() != null) {
        resultSetSchema = Schema.deserialize(preparedStatementResult.getDatasetSchema().asReadOnlyByteBuffer());
      }
      return resultSetSchema;
    }

    /**
     * Returns the Schema of the parameters.
     *
     * @return the Schema of the parameters.
     */
    public Schema getParameterSchema() {
      if (parameterSchema == null && preparedStatementResult.getParameterSchema() != null) {
        parameterSchema = Schema.deserialize(preparedStatementResult.getParameterSchema().asReadOnlyByteBuffer());
      }
      return parameterSchema;
    }

    /**
     * Executes the prepared statement query on the server.
     *
     * @return a FlightInfo object representing the stream(s) to fetch.
     * @throws IOException if the PreparedStatement is closed.
     */
    public FlightInfo execute() throws IOException {
      if (isClosed) {
        throw new IllegalStateException("Prepared statement has already been closed on the server.");
      }

      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
              .setClientExecutionHandle(
                  ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(invocationCount.getAndIncrement())))
              .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
              .build())
              .toByteArray());

      if (parameterBindingRoot != null) {
        final SyncPutListener putListener = new SyncPutListener();

        FlightClient.ClientStreamListener listener = client.startPut(descriptor, this.parameterBindingRoot, putListener);

        listener.putNext();
        listener.completed();
      }

      return client.getInfo(descriptor);
    }

    /**
     * Executes the prepared statement update on the server.
     */
    public long executeUpdate() {
      if (isClosed) {
        throw new IllegalStateException("Prepared statement has already been closed on the server.");
      }

      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(FlightSql.CommandPreparedStatementUpdate.newBuilder()
              .setClientExecutionHandle(
                  ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(invocationCount.getAndIncrement())))
              .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
              .build())
              .toByteArray());

      if (this.parameterBindingRoot == null) {
        this.parameterBindingRoot = VectorSchemaRoot.of();
      }

      final SyncPutListener putListener = new SyncPutListener();
      final FlightClient.ClientStreamListener listener =
          client.startPut(descriptor, this.parameterBindingRoot, putListener);

      listener.putNext();
      listener.completed();

      try {
        final PutResult read = putListener.read();
        try (final ArrowBuf metadata = read.getApplicationMetadata()) {
          final FlightSql.DoPutUpdateResult doPutUpdateResult =
              FlightSql.DoPutUpdateResult.parseFrom(metadata.nioBuffer());
          return doPutUpdateResult.getRecordCount();
        }
      } catch (InterruptedException | InvalidProtocolBufferException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      isClosed = true;
      final Iterator<Result> closePreparedStatementResults = client.doAction(new Action(
          FlightSqlUtils.FLIGHT_SQL_CLOSEPREPAREDSTATEMENT.getType(),
          Any.pack(ActionClosePreparedStatementRequest
              .newBuilder()
              .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
              .build())
              .toByteArray()));
      closePreparedStatementResults.forEachRemaining(result -> {
      });
    }

    /**
     * Returns if the prepared statement is already closed.
     *
     * @return true if the prepared statement is already closed.
     */
    public boolean isClosed() {
      return isClosed;
    }
  }
}
