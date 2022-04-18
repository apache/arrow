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
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCrossReference;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetDbSchemas;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSqlInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandGetXdbcTypeInfo;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementQuery;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Flight client with Flight SQL semantics.
 */
public class FlightSqlClient implements AutoCloseable {
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
   * @param catalog               The catalog.
   * @param dbSchemaFilterPattern The schema filter pattern.
   * @param options               RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSchemas(final String catalog, final String dbSchemaFilterPattern, final CallOption... options) {
    final CommandGetDbSchemas.Builder builder = CommandGetDbSchemas.newBuilder();

    if (catalog != null) {
      builder.setCatalog(catalog);
    }

    if (dbSchemaFilterPattern != null) {
      builder.setDbSchemaFilterPattern(dbSchemaFilterPattern);
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
  public FlightInfo getSqlInfo(final SqlInfo... info) {
    return getSqlInfo(info, new CallOption[0]);
  }

  /**
   * Request a set of Flight SQL metadata.
   *
   * @param info    The set of metadata to retrieve. None to retrieve all metadata.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(final SqlInfo[] info, final CallOption... options) {
    final int[] infoNumbers = Arrays.stream(info).mapToInt(SqlInfo::getNumber).toArray();
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
   * Request the information about the data types supported related to
   * a filter data type.
   *
   * @param dataType  the data type to be used as filter.
   * @param options   RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getXdbcTypeInfo(final int dataType, final CallOption... options) {
    final CommandGetXdbcTypeInfo.Builder builder = CommandGetXdbcTypeInfo.newBuilder();

    builder.setDataType(dataType);

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Request the information about all the data types supported.
   *
   * @param options   RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getXdbcTypeInfo(final CallOption... options) {
    final CommandGetXdbcTypeInfo.Builder builder = CommandGetXdbcTypeInfo.newBuilder();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Request a list of tables.
   *
   * @param catalog               The catalog.
   * @param dbSchemaFilterPattern The schema filter pattern.
   * @param tableFilterPattern    The table filter pattern.
   * @param tableTypes            The table types to include.
   * @param includeSchema         True to include the schema upon return, false to not include the schema.
   * @param options               RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTables(final String catalog, final String dbSchemaFilterPattern,
                              final String tableFilterPattern, final List<String> tableTypes,
                              final boolean includeSchema, final CallOption... options) {
    final CommandGetTables.Builder builder = CommandGetTables.newBuilder();

    if (catalog != null) {
      builder.setCatalog(catalog);
    }

    if (dbSchemaFilterPattern != null) {
      builder.setDbSchemaFilterPattern(dbSchemaFilterPattern);
    }

    if (tableFilterPattern != null) {
      builder.setTableNameFilterPattern(tableFilterPattern);
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
   * @param tableRef  An object which hold info about catalog, dbSchema and table.
   * @param options   RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getPrimaryKeys(final TableRef tableRef, final CallOption... options) {
    final CommandGetPrimaryKeys.Builder builder = CommandGetPrimaryKeys.newBuilder();

    if (tableRef.getCatalog() != null) {
      builder.setCatalog(tableRef.getCatalog());
    }

    if (tableRef.getDbSchema() != null) {
      builder.setDbSchema(tableRef.getDbSchema());
    }

    Objects.requireNonNull(tableRef.getTable());
    builder.setTable(tableRef.getTable()).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Retrieves a description about the foreign key columns that reference the primary key columns of the given table.
   *
   * @param tableRef  An object which hold info about catalog, dbSchema and table.
   * @param options   RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getExportedKeys(final TableRef tableRef, final CallOption... options) {
    Objects.requireNonNull(tableRef.getTable(), "Table cannot be null.");

    final CommandGetExportedKeys.Builder builder = CommandGetExportedKeys.newBuilder();

    if (tableRef.getCatalog() != null) {
      builder.setCatalog(tableRef.getCatalog());
    }

    if (tableRef.getDbSchema() != null) {
      builder.setDbSchema(tableRef.getDbSchema());
    }

    Objects.requireNonNull(tableRef.getTable());
    builder.setTable(tableRef.getTable()).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Retrieves the foreign key columns for the given table.
   *
   * @param tableRef  An object which hold info about catalog, dbSchema and table.
   * @param options   RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getImportedKeys(final TableRef tableRef,
                                    final CallOption... options) {
    Objects.requireNonNull(tableRef.getTable(), "Table cannot be null.");

    final CommandGetImportedKeys.Builder builder = CommandGetImportedKeys.newBuilder();

    if (tableRef.getCatalog() != null) {
      builder.setCatalog(tableRef.getCatalog());
    }

    if (tableRef.getDbSchema() != null) {
      builder.setDbSchema(tableRef.getDbSchema());
    }

    Objects.requireNonNull(tableRef.getTable());
    builder.setTable(tableRef.getTable()).build();

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Retrieves a description of the foreign key columns that reference the given table's
   * primary key columns (the foreign keys exported by a table).
   *
   * @param pkTableRef    An object which hold info about catalog, dbSchema and table from a primary table.
   * @param fkTableRef    An object which hold info about catalog, dbSchema and table from a foreign table.
   * @param options       RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getCrossReference(final TableRef pkTableRef,
                                      final TableRef fkTableRef, final CallOption... options) {
    Objects.requireNonNull(pkTableRef.getTable(), "Parent Table cannot be null.");
    Objects.requireNonNull(fkTableRef.getTable(), "Foreign Table cannot be null.");

    final CommandGetCrossReference.Builder builder = CommandGetCrossReference.newBuilder();

    if (pkTableRef.getCatalog() != null) {
      builder.setPkCatalog(pkTableRef.getCatalog());
    }

    if (pkTableRef.getDbSchema() != null) {
      builder.setPkDbSchema(pkTableRef.getDbSchema());
    }

    if (fkTableRef.getCatalog() != null) {
      builder.setFkCatalog(fkTableRef.getCatalog());
    }

    if (fkTableRef.getDbSchema() != null) {
      builder.setFkDbSchema(fkTableRef.getDbSchema());
    }

    builder.setPkTable(pkTableRef.getTable());
    builder.setFkTable(fkTableRef.getTable());

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

  @Override
  public void close() throws SQLException {
    try {
      AutoCloseables.close(client);
    } catch (final Exception e) {
      throw new SQLException(e);
    }
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
        resultSetSchema = deserializeSchema(bytes);
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
        parameterSchema = deserializeSchema(bytes);
      }
      return parameterSchema;
    }

    private Schema deserializeSchema(final ByteString bytes) {
      try {
        return bytes.isEmpty() ?
            new Schema(Collections.emptyList()) :
            MessageSerializer.deserializeSchema(
                new ReadChannel(Channels.newChannel(
                    new ByteArrayInputStream(bytes.toByteArray()))));
      } catch (final IOException e) {
        throw new RuntimeException("Failed to deserialize schema", e);
      }
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
        listener.getResult();
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
          .command(Any.pack(CommandPreparedStatementUpdate.newBuilder()
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
          final DoPutUpdateResult doPutUpdateResult =
              DoPutUpdateResult.parseFrom(metadata.nioBuffer());
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
