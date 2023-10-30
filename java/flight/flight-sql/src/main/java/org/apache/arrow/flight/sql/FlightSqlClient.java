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

import static org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginSavepointResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionBeginTransactionResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCancelQueryResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedSubstraitPlanRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionEndSavepointRequest;
import static org.apache.arrow.flight.sql.impl.FlightSql.ActionEndTransactionRequest;
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
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementSubstraitPlan;
import static org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementUpdate;
import static org.apache.arrow.flight.sql.impl.FlightSql.DoPutUpdateResult;
import static org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
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
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
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
    return execute(query, /*transaction*/ null, options);
  }

  /**
   * Execute a query on the server.
   *
   * @param query The query to execute.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo execute(final String query, Transaction transaction, final CallOption... options) {
    final CommandStatementQuery.Builder builder = CommandStatementQuery.newBuilder().setQuery(query);
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Execute a Substrait plan on the server.
   *
   * @param plan The Substrait plan to execute.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo executeSubstrait(SubstraitPlan plan, CallOption... options) {
    return executeSubstrait(plan, /*transaction*/ null, options);
  }

  /**
   * Execute a Substrait plan on the server.
   *
   * @param plan The Substrait plan to execute.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return a FlightInfo object representing the stream(s) to fetch.
   */
  public FlightInfo executeSubstrait(SubstraitPlan plan, Transaction transaction, CallOption... options) {
    final CommandStatementSubstraitPlan.Builder builder = CommandStatementSubstraitPlan.newBuilder();
    builder.getPlanBuilder().setPlan(ByteString.copyFrom(plan.getPlan())).setVersion(plan.getVersion());
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Get the schema of the result set of a query.
   */
  public SchemaResult getExecuteSchema(String query, Transaction transaction, CallOption... options) {
    final CommandStatementQuery.Builder builder = CommandStatementQuery.newBuilder();
    builder.setQuery(query);
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getSchema(descriptor, options);
  }

  /**
   * Get the schema of the result set of a query.
   */
  public SchemaResult getExecuteSchema(String query, CallOption... options) {
    return getExecuteSchema(query, /*transaction*/null, options);
  }

  /**
   * Get the schema of the result set of a Substrait plan.
   */
  public SchemaResult getExecuteSubstraitSchema(SubstraitPlan plan, Transaction transaction,
                                                final CallOption... options) {
    final CommandStatementSubstraitPlan.Builder builder = CommandStatementSubstraitPlan.newBuilder();
    builder.getPlanBuilder().setPlan(ByteString.copyFrom(plan.getPlan())).setVersion(plan.getVersion());
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getSchema(descriptor, options);
  }

  /**
   * Get the schema of the result set of a Substrait plan.
   */
  public SchemaResult getExecuteSubstraitSchema(SubstraitPlan substraitPlan, final CallOption... options) {
    return getExecuteSubstraitSchema(substraitPlan, /*transaction*/null, options);
  }

  /**
   * Execute an update query on the server.
   *
   * @param query   The query to execute.
   * @param options RPC-layer hints for this call.
   * @return the number of rows affected.
   */
  public long executeUpdate(final String query, final CallOption... options) {
    return executeUpdate(query, /*transaction*/ null, options);
  }

  /**
   * Execute an update query on the server.
   *
   * @param query   The query to execute.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return the number of rows affected.
   */
  public long executeUpdate(final String query, Transaction transaction, final CallOption... options) {
    final CommandStatementUpdate.Builder builder = CommandStatementUpdate.newBuilder().setQuery(query);
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    try (final SyncPutListener putListener = new SyncPutListener()) {
      final FlightClient.ClientStreamListener listener =
          client.startPut(descriptor, VectorSchemaRoot.of(), putListener, options);
      try (final PutResult result = putListener.read()) {
        final DoPutUpdateResult doPutUpdateResult = DoPutUpdateResult.parseFrom(
            result.getApplicationMetadata().nioBuffer());
        return doPutUpdateResult.getRecordCount();
      } finally {
        listener.getResult();
      }
    } catch (final InterruptedException | ExecutionException e) {
      throw CallStatus.CANCELLED.withCause(e).toRuntimeException();
    } catch (final InvalidProtocolBufferException e) {
      throw CallStatus.INTERNAL.withCause(e).toRuntimeException();
    }
  }

  /**
   * Execute an update query on the server.
   *
   * @param plan The Substrait plan to execute.
   * @param options RPC-layer hints for this call.
   * @return the number of rows affected.
   */
  public long executeSubstraitUpdate(SubstraitPlan plan, CallOption... options) {
    return executeSubstraitUpdate(plan, /*transaction*/ null, options);
  }

  /**
   * Execute an update query on the server.
   *
   * @param plan The Substrait plan to execute.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return the number of rows affected.
   */
  public long executeSubstraitUpdate(SubstraitPlan plan, Transaction transaction, CallOption... options) {
    final CommandStatementSubstraitPlan.Builder builder = CommandStatementSubstraitPlan.newBuilder();
    builder.getPlanBuilder().setPlan(ByteString.copyFrom(plan.getPlan())).setVersion(plan.getVersion());
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    try (final SyncPutListener putListener = new SyncPutListener()) {
      final FlightClient.ClientStreamListener listener =
          client.startPut(descriptor, VectorSchemaRoot.of(), putListener, options);
      try (final PutResult result = putListener.read()) {
        final DoPutUpdateResult doPutUpdateResult = DoPutUpdateResult.parseFrom(
            result.getApplicationMetadata().nioBuffer());
        return doPutUpdateResult.getRecordCount();
      } finally {
        listener.getResult();
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
   * Get the schema of {@link #getCatalogs(CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_CATALOGS_SCHEMA}.
   */
  public SchemaResult getCatalogsSchema(final CallOption... options) {
    final CommandGetCatalogs command = CommandGetCatalogs.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getSchemas(String, String, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_SCHEMAS_SCHEMA}.
   */
  public SchemaResult getSchemasSchema(final CallOption... options) {
    final CommandGetDbSchemas command = CommandGetDbSchemas.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getSqlInfo(SqlInfo...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_SQL_INFO_SCHEMA}.
   */
  public SchemaResult getSqlInfoSchema(final CallOption... options) {
    final CommandGetSqlInfo command = CommandGetSqlInfo.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getXdbcTypeInfo(CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_TYPE_INFO_SCHEMA}.
   */
  public SchemaResult getXdbcTypeInfoSchema(final CallOption... options) {
    final CommandGetXdbcTypeInfo command = CommandGetXdbcTypeInfo.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getTables(String, String, String, List, boolean, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_TABLES_SCHEMA} or
   * {@link FlightSqlProducer.Schemas#GET_TABLES_SCHEMA_NO_SCHEMA}.
   */
  public SchemaResult getTablesSchema(boolean includeSchema, final CallOption... options) {
    final CommandGetTables command = CommandGetTables.newBuilder().setIncludeSchema(includeSchema).build();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
    builder.setTable(tableRef.getTable());

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Get the schema of {@link #getPrimaryKeys(TableRef, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_PRIMARY_KEYS_SCHEMA}.
   */
  public SchemaResult getPrimaryKeysSchema(final CallOption... options) {
    final CommandGetPrimaryKeys command = CommandGetPrimaryKeys.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
    builder.setTable(tableRef.getTable());

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Get the schema of {@link #getExportedKeys(TableRef, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_EXPORTED_KEYS_SCHEMA}.
   */
  public SchemaResult getExportedKeysSchema(final CallOption... options) {
    final CommandGetExportedKeys command = CommandGetExportedKeys.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
    builder.setTable(tableRef.getTable());

    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(builder.build()).toByteArray());
    return client.getInfo(descriptor, options);
  }

  /**
   * Get the schema of {@link #getImportedKeys(TableRef, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_IMPORTED_KEYS_SCHEMA}.
   */
  public SchemaResult getImportedKeysSchema(final CallOption... options) {
    final CommandGetImportedKeys command = CommandGetImportedKeys.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getCrossReference(TableRef, TableRef, CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_CROSS_REFERENCE_SCHEMA}.
   */
  public SchemaResult getCrossReferenceSchema(final CallOption... options) {
    final CommandGetCrossReference command = CommandGetCrossReference.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
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
   * Get the schema of {@link #getTableTypes(CallOption...)} from the server.
   *
   * <p>Should be identical to {@link FlightSqlProducer.Schemas#GET_TABLE_TYPES_SCHEMA}.
   */
  public SchemaResult getTableTypesSchema(final CallOption... options) {
    final CommandGetTableTypes command = CommandGetTableTypes.getDefaultInstance();
    final FlightDescriptor descriptor = FlightDescriptor.command(Any.pack(command).toByteArray());
    return client.getSchema(descriptor, options);
  }

  /**
   * Create a prepared statement for a SQL query on the server.
   *
   * @param query   The query to prepare.
   * @param options RPC-layer hints for this call.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(String query, CallOption... options) {
    return prepare(query, /*transaction*/ null, options);
  }

  /**
   * Create a prepared statement for a SQL query on the server.
   *
   * @param query The query to prepare.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(String query, Transaction transaction, CallOption... options) {
    ActionCreatePreparedStatementRequest.Builder builder =
        ActionCreatePreparedStatementRequest.newBuilder().setQuery(query);
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    return new PreparedStatement(client,
        new Action(
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_STATEMENT.getType(),
            Any.pack(builder.build()).toByteArray()),
        options);
  }

  /**
   * Create a prepared statement for a Substrait plan on the server.
   *
   * @param plan    The query to prepare.
   * @param options RPC-layer hints for this call.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(SubstraitPlan plan, CallOption... options) {
    return prepare(plan, /*transaction*/ null, options);
  }

  /**
   * Create a prepared statement for a Substrait plan on the server.
   *
   * @param plan The query to prepare.
   * @param transaction The transaction that this query is part of.
   * @param options RPC-layer hints for this call.
   * @return The representation of the prepared statement which exists on the server.
   */
  public PreparedStatement prepare(SubstraitPlan plan, Transaction transaction, CallOption... options) {
    ActionCreatePreparedSubstraitPlanRequest.Builder builder =
        ActionCreatePreparedSubstraitPlanRequest.newBuilder();
    builder.getPlanBuilder().setPlan(ByteString.copyFrom(plan.getPlan())).setVersion(plan.getVersion());
    if (transaction != null) {
      builder.setTransactionId(ByteString.copyFrom(transaction.getTransactionId()));
    }
    return new PreparedStatement(client,
        new Action(
            FlightSqlUtils.FLIGHT_SQL_CREATE_PREPARED_SUBSTRAIT_PLAN.getType(),
            Any.pack(builder.build()).toByteArray()),
        options);
  }

  /** Begin a transaction. */
  public Transaction beginTransaction(CallOption... options) {
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_BEGIN_TRANSACTION.getType(),
        Any.pack(ActionBeginTransactionRequest.getDefaultInstance()).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    final ActionBeginTransactionResult result = FlightSqlUtils.unpackAndParseOrThrow(
        preparedStatementResults.next().getBody(),
        ActionBeginTransactionResult.class);
    preparedStatementResults.forEachRemaining((ignored) -> { });
    if (result.getTransactionId().isEmpty()) {
      throw CallStatus.INTERNAL.withDescription("Server returned an empty transaction ID").toRuntimeException();
    }
    return new Transaction(result.getTransactionId().toByteArray());
  }

  /** Create a savepoint within a transaction. */
  public Savepoint beginSavepoint(Transaction transaction, String name, CallOption... options) {
    Preconditions.checkArgument(transaction.getTransactionId().length != 0, "Transaction must be initialized");
    ActionBeginSavepointRequest request = ActionBeginSavepointRequest.newBuilder()
        .setTransactionId(ByteString.copyFrom(transaction.getTransactionId()))
        .setName(name)
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_BEGIN_SAVEPOINT.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    final ActionBeginSavepointResult result = FlightSqlUtils.unpackAndParseOrThrow(
        preparedStatementResults.next().getBody(),
        ActionBeginSavepointResult.class);
    preparedStatementResults.forEachRemaining((ignored) -> { });
    if (result.getSavepointId().isEmpty()) {
      throw CallStatus.INTERNAL.withDescription("Server returned an empty transaction ID").toRuntimeException();
    }
    return new Savepoint(result.getSavepointId().toByteArray());
  }

  /** Commit a transaction. */
  public void commit(Transaction transaction, CallOption... options) {
    Preconditions.checkArgument(transaction.getTransactionId().length != 0, "Transaction must be initialized");
    ActionEndTransactionRequest request = ActionEndTransactionRequest.newBuilder()
        .setTransactionId(ByteString.copyFrom(transaction.getTransactionId()))
        .setActionValue(ActionEndTransactionRequest.EndTransaction.END_TRANSACTION_COMMIT.getNumber())
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    preparedStatementResults.forEachRemaining((ignored) -> { });
  }

  /** Release a savepoint. */
  public void release(Savepoint savepoint, CallOption... options) {
    Preconditions.checkArgument(savepoint.getSavepointId().length != 0, "Savepoint must be initialized");
    ActionEndSavepointRequest request = ActionEndSavepointRequest.newBuilder()
        .setSavepointId(ByteString.copyFrom(savepoint.getSavepointId()))
        .setActionValue(ActionEndSavepointRequest.EndSavepoint.END_SAVEPOINT_RELEASE.getNumber())
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    preparedStatementResults.forEachRemaining((ignored) -> { });
  }

  /** Rollback a transaction. */
  public void rollback(Transaction transaction, CallOption... options) {
    Preconditions.checkArgument(transaction.getTransactionId().length != 0, "Transaction must be initialized");
    ActionEndTransactionRequest request = ActionEndTransactionRequest.newBuilder()
        .setTransactionId(ByteString.copyFrom(transaction.getTransactionId()))
        .setActionValue(ActionEndTransactionRequest.EndTransaction.END_TRANSACTION_ROLLBACK.getNumber())
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_END_TRANSACTION.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    preparedStatementResults.forEachRemaining((ignored) -> { });
  }

  /** Rollback to a savepoint. */
  public void rollback(Savepoint savepoint, CallOption... options) {
    Preconditions.checkArgument(savepoint.getSavepointId().length != 0, "Savepoint must be initialized");
    ActionEndSavepointRequest request = ActionEndSavepointRequest.newBuilder()
        .setSavepointId(ByteString.copyFrom(savepoint.getSavepointId()))
        .setActionValue(ActionEndSavepointRequest.EndSavepoint.END_SAVEPOINT_RELEASE.getNumber())
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_END_SAVEPOINT.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    preparedStatementResults.forEachRemaining((ignored) -> { });
  }

  /**
   * Cancel execution of a distributed query.
   *
   * @param request The query to cancel.
   * @param options Call options.
   * @return The server response.
   */
  public CancelFlightInfoResult cancelFlightInfo(CancelFlightInfoRequest request, CallOption... options) {
    return client.cancelFlightInfo(request, options);
  }

  /**
   * Explicitly cancel a running query.
   * <p>
   * This lets a single client explicitly cancel work, no matter how many clients
   * are involved/whether the query is distributed or not, given server support.
   * The transaction/statement is not rolled back; it is the application's job to
   * commit or rollback as appropriate. This only indicates the client no longer
   * wishes to read the remainder of the query results or continue submitting
   * data.
   *
   * @deprecated Prefer {@link #cancelFlightInfo}.
   */
  @Deprecated
  public CancelResult cancelQuery(FlightInfo info, CallOption... options) {
    ActionCancelQueryRequest request = ActionCancelQueryRequest.newBuilder()
        .setInfo(ByteString.copyFrom(info.serialize()))
        .build();
    final Action action = new Action(
        FlightSqlUtils.FLIGHT_SQL_CANCEL_QUERY.getType(),
        Any.pack(request).toByteArray());
    final Iterator<Result> preparedStatementResults = client.doAction(action, options);
    final ActionCancelQueryResult result = FlightSqlUtils.unpackAndParseOrThrow(
        preparedStatementResults.next().getBody(),
        ActionCancelQueryResult.class);
    preparedStatementResults.forEachRemaining((ignored) -> { });
    switch (result.getResult()) {
      case CANCEL_RESULT_UNSPECIFIED:
        return CancelResult.UNSPECIFIED;
      case CANCEL_RESULT_CANCELLED:
        return CancelResult.CANCELLED;
      case CANCEL_RESULT_CANCELLING:
        return CancelResult.CANCELLING;
      case CANCEL_RESULT_NOT_CANCELLABLE:
        return CancelResult.NOT_CANCELLABLE;
      case UNRECOGNIZED:
      default:
        throw CallStatus.INTERNAL.withDescription("Unknown result: " + result.getResult()).toRuntimeException();
    }
  }

  /**
   * Request the server to extend the lifetime of a query result set.
   *
   * @param request The result set partition.
   * @param options Call options.
   * @return The new endpoint with an updated expiration time.
   */
  public FlightEndpoint renewFlightEndpoint(RenewFlightEndpointRequest request, CallOption... options) {
    return client.renewFlightEndpoint(request, options);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(client);
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

    PreparedStatement(FlightClient client, Action action, CallOption... options) {
      this.client = client;

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
      if (parameterBindingRoot == this.parameterBindingRoot) {
        // Nothing to do if we're attempting to set the same parameters again.
        return;
      }
      clearParameters();
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

    /**
     * Get the schema of the result set (should be identical to {@link #getResultSetSchema()}).
     */
    public SchemaResult fetchSchema(CallOption... options) {
      checkOpen();

      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
                  .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                  .build())
              .toByteArray());
      return client.getSchema(descriptor, options);
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
    public FlightInfo execute(final CallOption... options) {
      checkOpen();

      final FlightDescriptor descriptor = FlightDescriptor
          .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
                  .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                  .build())
              .toByteArray());

      if (parameterBindingRoot != null && parameterBindingRoot.getRowCount() > 0) {
        putParameters(descriptor, options);
      }

      return client.getInfo(descriptor, options);
    }

    private SyncPutListener putParameters(FlightDescriptor descriptor, CallOption... options) {
      final SyncPutListener putListener = new SyncPutListener();

      FlightClient.ClientStreamListener listener =
              client.startPut(descriptor, parameterBindingRoot, putListener, options);

      listener.putNext();
      listener.completed();
      listener.getResult();

      return putListener;
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
      SyncPutListener putListener = putParameters(descriptor, options);

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
      clearParameters();
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

  /** A handle for an active savepoint. */
  public static class Savepoint {
    private final byte[] transactionId;

    public Savepoint(byte[] transactionId) {
      this.transactionId = transactionId;
    }

    public byte[] getSavepointId() {
      return transactionId;
    }
  }

  /** A handle for an active transaction. */
  public static class Transaction {
    private final byte[] transactionId;

    public Transaction(byte[] transactionId) {
      this.transactionId = transactionId;
    }

    public byte[] getTransactionId() {
      return transactionId;
    }
  }

  /** A wrapper around a Substrait plan and a Substrait version. */
  public static final class SubstraitPlan {
    private final byte[] plan;
    private final String version;

    public SubstraitPlan(byte[] plan, String version) {
      this.plan = Preconditions.checkNotNull(plan);
      this.version = Preconditions.checkNotNull(version);
    }

    public byte[] getPlan() {
      return plan;
    }

    public String getVersion() {
      return version;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SubstraitPlan that = (SubstraitPlan) o;

      if (!Arrays.equals(getPlan(), that.getPlan())) {
        return false;
      }
      return getVersion().equals(that.getVersion());
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(getPlan());
      result = 31 * result + getVersion().hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "SubstraitPlan{" +
          "plan=" + Arrays.toString(plan) +
          ", version='" + version + '\'' +
          '}';
    }
  }
}
