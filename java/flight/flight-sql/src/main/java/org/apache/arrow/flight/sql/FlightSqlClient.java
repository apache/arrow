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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

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
    final FlightSql.CommandStatementQuery.Builder builder = FlightSql.CommandStatementQuery.newBuilder();
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
    final FlightSql.CommandStatementUpdate.Builder builder = FlightSql.CommandStatementUpdate.newBuilder();
    builder.setQuery(query);
    return 0; // TODO
  }

  /**
   * Request a list of catalogs.
   *
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getCatalogs() {
    final FlightSql.CommandGetCatalogs.Builder builder = FlightSql.CommandGetCatalogs.newBuilder();
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
  public FlightInfo getSchemas(String catalog, String schemaFilterPattern) {
    final FlightSql.CommandGetSchemas.Builder builder = FlightSql.CommandGetSchemas.newBuilder();

    if (catalog != null) {
      builder.setCatalog(catalog);
    }

    if (schemaFilterPattern != null) {
      builder.setSchemaFilterPattern(schemaFilterPattern);
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Get schema for a stream.
   * @param descriptor The descriptor for the stream.
   * @param options RPC-layer hints for this call.
   */
  public SchemaResult getSchema(FlightDescriptor descriptor, CallOption... options) {
    return this.client.getSchema(descriptor, options);
  }

  /**
   * Retrieve a stream from the server.
   * @param ticket The ticket granting access to the data stream.
   * @param options RPC-layer hints for this call.
   */
  public FlightStream getStream(Ticket ticket, CallOption... options) {
    return this.client.getStream(ticket, options);
  }

  /**
   * Request a set of Flight SQL metadata.
   *
   * @param info             The set of metadata to retrieve. None to retrieve all metadata.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getSqlInfo(String... info) {
    final FlightSql.CommandGetSqlInfo.Builder builder = FlightSql.CommandGetSqlInfo.newBuilder();

    if (info != null && 0 != info.length) {
      builder.addAllInfo(Arrays.asList(info));
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
  public FlightInfo getTables(String catalog, String schemaFilterPattern,
          String tableFilterPattern, List<String> tableTypes, boolean includeSchema) {
    final FlightSql.CommandGetTables.Builder builder = FlightSql.CommandGetTables.newBuilder();

    if (catalog != null) {
      builder.setCatalog(catalog);
    }

    if (schemaFilterPattern != null) {
      builder.setSchemaFilterPattern(schemaFilterPattern);
    }

    if (tableFilterPattern != null) {
      builder.setTableNameFilterPattern(tableFilterPattern);
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
   * @param catalog             The catalog.
   * @param schema              The schema.
   * @param table               The table.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getPrimaryKeys(String catalog, String schema, String table) {
    final FlightSql.CommandGetPrimaryKeys.Builder builder = FlightSql.CommandGetPrimaryKeys.newBuilder();

    if (catalog != null) {
      builder.setCatalog(catalog);
    }

    if (schema != null) {
      builder.setSchema(schema);
    }

    builder.setTable(table);
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request the foreign keys for a table.
   *
   * One of pkTable or fkTable must be specified, both cannot be null.
   *
   * @param pkCatalog             The primary key table catalog.
   * @param pkSchema              The primary key table schema.
   * @param pkTable               The primary key table.
   * @param fkCatalog             The foreign key table catalog.
   * @param fkSchema              The foreign key table schema.
   * @param fkTable               The foreign key table.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getForeignKeys(String pkCatalog, String pkSchema, String pkTable,
                                   String fkCatalog, String fkSchema, String fkTable) {
    if (null == pkTable && null == fkTable) {
      throw Status.INVALID_ARGUMENT.asRuntimeException();
    }

    final FlightSql.CommandGetForeignKeys.Builder builder = FlightSql.CommandGetForeignKeys.newBuilder();

    if (pkCatalog != null) {
      builder.setPkCatalog(pkCatalog);
    }

    if (pkSchema != null) {
      builder.setPkSchema(pkSchema);
    }

    if (pkTable != null) {
      builder.setPkTable(pkTable);
    }

    if (fkCatalog != null) {
      builder.setFkCatalog(fkCatalog);
    }

    if (fkSchema != null) {
      builder.setFkSchema(fkSchema);
    }

    if (fkTable != null) {
      builder.setFkTable(fkTable);
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor);
  }

  /**
   * Request a list of table types.
   *
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo getTableTypes() {
    final FlightSql.CommandGetTableTypes.Builder builder = FlightSql.CommandGetTableTypes.newBuilder();
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
          Any.pack(FlightSql.ActionCreatePreparedStatementRequest
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

      return client.getInfo(descriptor);
    }

    /**
     * Executes the prepared statement update on the server.
     *
     * @return the number of rows updated.
     */
    public long executeUpdate() {
      throw Status.UNIMPLEMENTED.asRuntimeException();
    }

    // TODO: Set parameter values

    @Override
    public void close() {
      isClosed = true;
      final Iterator<Result> closePreparedStatementResults = client.doAction(new Action(
          FlightSqlUtils.FLIGHT_SQL_CLOSEPREPAREDSTATEMENT.getType(),
          Any.pack(FlightSql.ActionClosePreparedStatementRequest
              .newBuilder()
              .setPreparedStatementHandleBytes(preparedStatementResult.getPreparedStatementHandle())
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
