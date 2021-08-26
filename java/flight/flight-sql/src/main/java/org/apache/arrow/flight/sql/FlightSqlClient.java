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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;

/**
 * Flight client with Flight SQL semantics.
 */
public class FlightSqlClient {
  private final FlightClient client;

  public FlightSqlClient(final FlightClient client) {
    this.client = Objects.requireNonNull(client, "Client cannot be null!");
  }

  /**
   * Execute a query on the server.
   *
   * @param query   The query to execute.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo execute(final String query, final CallOption... options) {
    final CommandStatementQuery.Builder builder = CommandStatementQuery.newBuilder();
    builder.setQuery(query);
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Execute an update query on the server.
   *
   * @param query   The query to execute.
   * @param options RPC-layer hints for this call.
   * @return the number of rows affected.
   */
  public long executeUpdate(final String query, final CallOption... options) {
    final CommandStatementUpdate.Builder builder = CommandStatementUpdate.newBuilder();
    builder.setQuery(query);

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    final SyncPutListener putListener = new SyncPutListener();
    client.startPut(descriptor, VectorSchemaRoot.of(), putListener, options);

    try {
      final PutResult read = putListener.read();
      try (final ArrowBuf metadata = read.getApplicationMetadata()) {
        final DoPutUpdateResult doPutUpdateResult = DoPutUpdateResult.parseFrom(metadata.nioBuffer());
        return doPutUpdateResult.getRecordCount();
      }
    } catch (final InterruptedException | ExecutionException e) {
      throw CallStatus.CANCELLED.withCause(e).toRuntimeException();
    } catch (final InvalidProtocolBufferException e) {
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    }
  }

  /**
   * Request a list of catalogs.
   *
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getCatalogs(final CallOption... options) {
    final CommandGetCatalogs.Builder builder = CommandGetCatalogs.newBuilder();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Request a list of schemas.
   *
   * @param catalog             The catalog.
   * @param schemaFilterPattern The schema filter pattern.
   * @param options             RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSchemas(final String catalog, final String schemaFilterPattern, final CallOption... options) {
    final CommandGetSchemas.Builder builder = CommandGetSchemas.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schemaFilterPattern != null) {
      builder.setSchemaFilterPattern(StringValue.newBuilder().setValue(schemaFilterPattern).build());
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Get schema for a stream.
   *
   * @param descriptor The descriptor for the stream.
   * @param options    RPC-layer hints for this call.
   */
  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... options) {
    return client.getSchema(descriptor, options);
  }

  /**
   * Retrieve a stream from the server.
   *
   * @param ticket  The ticket granting access to the data stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightStream getStream(Ticket ticket, CallOption... options) {
    return client.getStream(ticket, options);
  }

  /**
   * Request a set of Flight SQL metadata.
   *
   * @param info The set of metadata to retrieve. None to retrieve all metadata.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final FlightSql.SqlInfo... info) {
    return getSqlInfo(info, new CallOption[0]);
  }

  /**
   * Request a set of Flight SQL metadata.
   *
   * @param info    The set of metadata to retrieve. None to retrieve all metadata.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final FlightSql.SqlInfo[] info, final CallOption... options) {
    final int[] infoNumbers = Arrays.stream(info).mapToInt(FlightSql.SqlInfo::getNumber).toArray();
    return getSqlInfo(infoNumbers, options);
  }

  /**
   * Request a set of Flight SQL metadata.
   * Use this method if you would like to retrieve custom metadata, where the custom metadata key values start
   * from 10_000.
   *
   * @param info    The set of metadata to retrieve. None to retrieve all metadata.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final int[] info, final CallOption... options) {
    return getSqlInfo(Arrays.stream(info).boxed().collect(Collectors.toList()), options);
  }

  /**
   * Request a set of Flight SQL metadata.
   * Use this method if you would like to retrieve custom metadata, where the custom metadata key values start
   * from 10_000.
   *
   * @param info    The set of metadata to retrieve. None to retrieve all metadata.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final Iterable<Integer> info, final CallOption... options) {
    final CommandGetSqlInfo.Builder builder = CommandGetSqlInfo.newBuilder();
    builder.addAllInfo(info);
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Request a list of tables.
   *
   * @param catalog             The catalog.
   * @param schemaFilterPattern The schema filter pattern.
   * @param tableFilterPattern  The table filter pattern.
   * @param tableTypes          The table types to include.
   * @param includeSchema       True to include the schema upon return, false to not include the schema.
   * @param options             RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTables(final String catalog, final String schemaFilterPattern,
                              final String tableFilterPattern, final List<String> tableTypes,
                              final boolean includeSchema, final CallOption... options) {
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
    return client.getInfo(descriptor, options);
  }

  /**
   * Request the primary keys for a table.
   *
   * @param catalog The catalog.
   * @param schema  The schema.
   * @param table   The table.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getPrimaryKeys(final String catalog, final String schema,
                                   final String table, final CallOption... options) {
    final CommandGetPrimaryKeys.Builder builder = CommandGetPrimaryKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    Objects.requireNonNull(table);
    builder.setTable(table).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Retrieves a description about the foreign key columns that reference the primary key columns of the given table.
   *
   * @param catalog The foreign key table catalog.
   * @param schema  The foreign key table schema.
   * @param table   The foreign key table. Cannot be null.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getExportedKeys(String catalog, String schema, String table, final CallOption... options) {
    Objects.requireNonNull(table, "Table cannot be null.");

    final CommandGetExportedKeys.Builder builder = CommandGetExportedKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    Objects.requireNonNull(table);
    builder.setTable(table).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Retrieves the foreign key columns for the given table.
   *
   * @param catalog The primary key table catalog.
   * @param schema  The primary key table schema.
   * @param table   The primary key table. Cannot be null.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getImportedKeys(final String catalog, final String schema, final String table,
                                    final CallOption... options) {
    Objects.requireNonNull(table, "Table cannot be null.");

    final CommandGetImportedKeys.Builder builder = CommandGetImportedKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(StringValue.newBuilder().setValue(catalog).build());
    }

    if (schema != null) {
      builder.setSchema(StringValue.newBuilder().setValue(schema).build());
    }

    Objects.requireNonNull(table);
    builder.setTable(table).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Request a list of table types.
   *
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTableTypes(final CallOption... options) {
    final CommandGetTableTypes.Builder builder = CommandGetTableTypes.newBuilder();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Create a prepared statement on the server.
   *
   * @param query   The query to prepare.
   * @param options RPC-layer hints for this call.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(final String query, final CallOption... options) {
    return new PreparedStatement(client, query, options);
  }

  /**
   * Helper class to encapsulate Flight SQL prepared statement logic.
   */
  public static class PreparedStatement implements AutoCloseable {
    private final FlightClient client;
    private final ActionCreatePreparedStatementResult preparedStatementResult;
    private VectorSchemaRoot parameterBindingRoot;
    private boolean isClosed;
    private Schema resultSetSchema;
    private Schema parameterSchema;

    /**
     * Constructor.
     *
     * @param client  The client. PreparedStatement does not maintain this resource.
     * @param sql     The query.
     * @param options RPC-layer hints for this call.
     */
    public PreparedStatement(final FlightClient client, final String sql, final CallOption... options) {
      this.client = client;
      final Action action = new Action(
          FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
          Any.pack(ActionCreatePreparedStatementRequest
                  .newBuilder()
                  .setQuery(sql)
                  .build())
              .toByteArray());
      final Iterator<Result> preparedStatementResults = client.doAction(action, options);

      preparedStatementResult = FlightSqlUtils.unpackAndParseOrThrow(
          preparedStatementResults.next().getBody(),
          ActionCreatePreparedStatementResult.class);

      isClosed = false;
    }

    /**
     * Set the {@link #parameterBindingRoot} containing the parameter binding from a {@link PreparedStatement}
     * operation.
     *
     * @param parameterBindingRoot a {@code VectorSchemaRoot} object containing the values to be used in the
     *                             {@code PreparedStatement} setters.
     */
    public void setParameters(final VectorSchemaRoot parameterBindingRoot) {
      if (this.parameterBindingRoot != null) {
        if (this.parameterBindingRoot.equals(parameterBindingRoot)) {
          return;
        }
        this.parameterBindingRoot.close();
      }
      this.parameterBindingRoot = parameterBindingRoot;
    }

    /**
     * Closes the {@link #parameterBindingRoot}, which contains the parameter binding from
     * a {@link PreparedStatement} operation, releasing its resources.
     */
    public void clearParameters() {
      if (parameterBindingRoot != null) {
        parameterBindingRoot.close();
      }
    }

    /**
     * Returns the Schema of the resultset.
     *
     * @return the Schema of the resultset.
     */
    public Schema getResultSetSchema() {
      if (resultSetSchema == null) {
        final ByteString bytes = preparedStatementResult.getDatasetSchema();
        resultSetSchema = bytes.isEmpty() ?
            new Schema(Collections.emptyList()) :
            MessageSerializer.deserializeSchema(Message.getRootAsMessage(bytes.asReadOnlyByteBuffer()));
      }
      return resultSetSchema;
    }

    /**
     * Returns the Schema of the parameters.
     *
     * @return the Schema of the parameters.
     */
    public Schema getParameterSchema() {
      if (parameterSchema == null) {
        final ByteString bytes = preparedStatementResult.getParameterSchema();
        parameterSchema = bytes.isEmpty() ?
            new Schema(Collections.emptyList()) :
            MessageSerializer.deserializeSchema(Message.getRootAsMessage(bytes.asReadOnlyByteBuffer()));
      }
      return parameterSchema;
    }

    /**
     * Executes the prepared statement query on the server.
     *
     * @param options RPC-layer hints for this call.
     * @return a FlightInfo object representing the stream(s) to fetch.
     */
    public FlightInfo execute(final CallOption... options) throws SQLException {
      checkOpen();

      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
                  .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                  .build())
              .toByteArray());

      if (parameterBindingRoot != null && parameterBindingRoot.getRowCount() > 0) {
        final SyncPutListener putListener = new SyncPutListener();

        FlightClient.ClientStreamListener listener =
            client.startPut(descriptor, parameterBindingRoot, putListener, options);

        listener.putNext();
        listener.completed();
      }

      return client.getInfo(descriptor, options);
    }

    /**
     * Checks whether this client is open.
     *
     * @throws IllegalStateException if client is closed.
     */
    protected final void checkOpen() {
      Preconditions.checkState(!isClosed, "Statement closed");
    }

    /**
     * Executes the prepared statement update on the server.
     *
     * @param options RPC-layer hints for this call.
     * @return the count of updated records
     */
    public long executeUpdate(final CallOption... options) {
      checkOpen();
      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(FlightSql.CommandPreparedStatementUpdate.newBuilder()
                  .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                  .build())
              .toByteArray());
      setParameters(parameterBindingRoot == null ? VectorSchemaRoot.of() : parameterBindingRoot);
      final SyncPutListener putListener = new SyncPutListener();
      final FlightClient.ClientStreamListener listener =
          client.startPut(descriptor, parameterBindingRoot, putListener, options);
      listener.putNext();
      listener.completed();
      try {
        final PutResult read = putListener.read();
        try (final ArrowBuf metadata = read.getApplicationMetadata()) {
          final FlightSql.DoPutUpdateResult doPutUpdateResult =
              FlightSql.DoPutUpdateResult.parseFrom(metadata.nioBuffer());
          return doPutUpdateResult.getRecordCount();
        }
      } catch (final InterruptedException | ExecutionException e) {
        throw CallStatus.CANCELLED.withCause(e).toRuntimeException();
      } catch (final InvalidProtocolBufferException e) {
        throw CallStatus.INVALID_ARGUMENT.withCause(e).toRuntimeException();
      }
    }

    /**
     * Closes the client.
     *
     * @param options RPC-layer hints for this call.
     */
    public void close(final CallOption... options) {
      if (isClosed) {
        return;
      }
      isClosed = true;
      final Action action = new Action(
          FlightSqlUtils.FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.getType(),
          Any.pack(ActionClosePreparedStatementRequest.newBuilder()
                  .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                  .build())
              .toByteArray());
      final Iterator<Result> closePreparedStatementResults = client.doAction(action, options);
      closePreparedStatementResults.forEachRemaining(result -> {
      });
      if (parameterBindingRoot != null) {
        parameterBindingRoot.close();
      }
    }

    @Override
    public void close() {
      close(new CallOption[0]);
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
