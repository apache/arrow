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

package org.apache.arrow.flight.integration.tests;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import org.apache.arrow.flight.sql.CancelResult;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

/**
 * Hardcoded Flight SQL producer used for cross-language integration tests.
 */
public class FlightSqlScenarioProducer implements FlightSqlProducer {
  private final BufferAllocator allocator;

  public FlightSqlScenarioProducer(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Schema to be returned for mocking the statement/prepared statement results.
   * Must be the same across all languages.
   */
  static Schema getQuerySchema() {
    return new Schema(
        Collections.singletonList(
            new Field("id", new FieldType(true, new ArrowType.Int(64, true),
                null, new FlightSqlColumnMetadata.Builder()
                .tableName("test")
                .isAutoIncrement(true)
                .isCaseSensitive(false)
                .typeName("type_test")
                .schemaName("schema_test")
                .isSearchable(true)
                .catalogName("catalog_test")
                .precision(100)
                .build().getMetadataMap()), null)
        )
    );
  }

  /**
   * The expected schema for queries with transactions.
   * <p>
   * Must be the same across all languages.
   */
  static Schema getQueryWithTransactionSchema() {
    return new Schema(
        Collections.singletonList(
            new Field("pkey", new FieldType(true, new ArrowType.Int(32, true),
                null, new FlightSqlColumnMetadata.Builder()
                .tableName("test")
                .isAutoIncrement(true)
                .isCaseSensitive(false)
                .typeName("type_test")
                .schemaName("schema_test")
                .isSearchable(true)
                .catalogName("catalog_test")
                .precision(100)
                .build().getMetadataMap()), null)
        )
    );
  }

  @Override
  public void beginSavepoint(FlightSql.ActionBeginSavepointRequest request, CallContext context,
                             StreamListener<FlightSql.ActionBeginSavepointResult> listener) {
    if (!request.getName().equals(FlightSqlScenario.SAVEPOINT_NAME)) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected name '%s', not '%s'",
              FlightSqlScenario.SAVEPOINT_NAME, request.getName()))
          .toRuntimeException());
      return;
    }
    if (!Arrays.equals(request.getTransactionId().toByteArray(), FlightSqlScenario.TRANSACTION_ID)) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected transaction ID '%s', not '%s'",
              Arrays.toString(FlightSqlScenario.TRANSACTION_ID),
              Arrays.toString(request.getTransactionId().toByteArray())))
          .toRuntimeException());
      return;
    }
    listener.onNext(FlightSql.ActionBeginSavepointResult.newBuilder()
        .setSavepointId(ByteString.copyFrom(FlightSqlScenario.SAVEPOINT_ID))
        .build());
    listener.onCompleted();
  }

  @Override
  public void beginTransaction(FlightSql.ActionBeginTransactionRequest request, CallContext context,
                               StreamListener<FlightSql.ActionBeginTransactionResult> listener) {
    listener.onNext(FlightSql.ActionBeginTransactionResult.newBuilder()
        .setTransactionId(ByteString.copyFrom(FlightSqlScenario.TRANSACTION_ID))
        .build());
    listener.onCompleted();
  }

  @Override
  public void cancelQuery(FlightInfo info, CallContext context, StreamListener<CancelResult> listener) {
    final String expectedTicket = "PLAN HANDLE";
    if (info.getEndpoints().size() != 1) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected 1 endpoint, got %d", info.getEndpoints().size()))
          .toRuntimeException());
    }
    final FlightEndpoint endpoint = info.getEndpoints().get(0);
    try {
      final Any any = Any.parseFrom(endpoint.getTicket().getBytes());
      if (!any.is(FlightSql.TicketStatementQuery.class)) {
        listener.onError(CallStatus.INVALID_ARGUMENT
            .withDescription(String.format("Expected TicketStatementQuery, found '%s'", any.getTypeUrl()))
            .toRuntimeException());
      }
      final FlightSql.TicketStatementQuery ticket = any.unpack(FlightSql.TicketStatementQuery.class);
      if (!ticket.getStatementHandle().toStringUtf8().equals(expectedTicket)) {
        listener.onError(CallStatus.INVALID_ARGUMENT
            .withDescription(String.format("Expected ticket '%s'", expectedTicket))
            .toRuntimeException());
      }
      listener.onNext(CancelResult.CANCELLED);
      listener.onCompleted();
    } catch (InvalidProtocolBufferException e) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription("Invalid Protobuf:" + e)
          .withCause(e)
          .toRuntimeException());
    }
  }

  @Override
  public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
                                      CallContext context, StreamListener<Result> listener) {
    IntegrationAssertions.assertTrue("Expect to be one of the two queries used on tests",
        request.getQuery().equals("SELECT PREPARED STATEMENT") ||
            request.getQuery().equals("UPDATE PREPARED STATEMENT"));

    String text = request.getQuery();
    if (!request.getTransactionId().isEmpty()) {
      text += " WITH TXN";
    }
    text += " HANDLE";
    final FlightSql.ActionCreatePreparedStatementResult
        result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
        .setPreparedStatementHandle(ByteString.copyFromUtf8(text))
        .build();
    listener.onNext(new Result(Any.pack(result).toByteArray()));
    listener.onCompleted();
  }

  @Override
  public void createPreparedSubstraitPlan(FlightSql.ActionCreatePreparedSubstraitPlanRequest request,
                                          CallContext context,
                                          StreamListener<FlightSql.ActionCreatePreparedStatementResult> listener) {
    if (!Arrays.equals(request.getPlan().getPlan().toByteArray(), FlightSqlScenario.SUBSTRAIT_PLAN_TEXT)) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected plan '%s', not '%s'",
              Arrays.toString(FlightSqlScenario.SUBSTRAIT_PLAN_TEXT),
              Arrays.toString(request.getPlan().getPlan().toByteArray())))
          .toRuntimeException());
      return;
    }
    if (!FlightSqlScenario.SUBSTRAIT_VERSION.equals(request.getPlan().getVersion())) {
      listener.onError(CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected version '%s', not '%s'",
              FlightSqlScenario.SUBSTRAIT_VERSION,
              request.getPlan().getVersion()))
          .toRuntimeException());
      return;
    }
    final String handle = request.getTransactionId().isEmpty() ?
        "PREPARED PLAN HANDLE" : "PREPARED PLAN WITH TXN HANDLE";
    final FlightSql.ActionCreatePreparedStatementResult result =
        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
            .setPreparedStatementHandle(ByteString.copyFromUtf8(handle))
            .build();
    listener.onNext(result);
    listener.onCompleted();
  }

  @Override
  public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
                                     CallContext context, StreamListener<Result> listener) {
    final String handle = request.getPreparedStatementHandle().toStringUtf8();
    IntegrationAssertions.assertTrue("Expect to be one of the queries used on tests",
        handle.equals("SELECT PREPARED STATEMENT HANDLE") ||
            handle.equals("SELECT PREPARED STATEMENT WITH TXN HANDLE") ||
            handle.equals("UPDATE PREPARED STATEMENT HANDLE") ||
            handle.equals("UPDATE PREPARED STATEMENT WITH TXN HANDLE") ||
            handle.equals("PREPARED PLAN HANDLE") ||
            handle.equals("PREPARED PLAN WITH TXN HANDLE"));
    listener.onCompleted();
  }

  @Override
  public void endSavepoint(FlightSql.ActionEndSavepointRequest request, CallContext context,
                           StreamListener<Result> listener) {
    switch (request.getAction()) {
      case END_SAVEPOINT_RELEASE:
      case END_SAVEPOINT_ROLLBACK:
        if (!Arrays.equals(request.getSavepointId().toByteArray(), FlightSqlScenario.SAVEPOINT_ID)) {
          listener.onError(CallStatus.INVALID_ARGUMENT
              .withDescription("Unexpected ID: " + Arrays.toString(request.getSavepointId().toByteArray()))
              .toRuntimeException());
        }
        break;
      case UNRECOGNIZED:
      default: {
        listener.onError(CallStatus.INVALID_ARGUMENT
            .withDescription("Unknown action: " + request.getAction())
            .toRuntimeException());
        return;
      }
    }
    listener.onCompleted();
  }

  @Override
  public void endTransaction(FlightSql.ActionEndTransactionRequest request, CallContext context,
                             StreamListener<Result> listener) {
    switch (request.getAction()) {
      case END_TRANSACTION_COMMIT:
      case END_TRANSACTION_ROLLBACK:
        if (!Arrays.equals(request.getTransactionId().toByteArray(), FlightSqlScenario.TRANSACTION_ID)) {
          listener.onError(CallStatus.INVALID_ARGUMENT
              .withDescription("Unexpected ID: " + Arrays.toString(request.getTransactionId().toByteArray()))
              .toRuntimeException());
        }
        break;
      case UNRECOGNIZED:
      default: {
        listener.onError(CallStatus.INVALID_ARGUMENT
            .withDescription("Unknown action: " + request.getAction())
            .toRuntimeException());
        return;
      }
    }
    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
                                           CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(command.getQuery(), "SELECT STATEMENT");
    if (command.getTransactionId().isEmpty()) {
      String handle = "SELECT STATEMENT HANDLE";
      FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
          .setStatementHandle(ByteString.copyFromUtf8(handle))
          .build();
      return getFlightInfoForSchema(ticket, descriptor, getQuerySchema());
    } else {
      String handle = "SELECT STATEMENT WITH TXN HANDLE";
      FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
          .setStatementHandle(ByteString.copyFromUtf8(handle))
          .build();
      return getFlightInfoForSchema(ticket, descriptor, getQueryWithTransactionSchema());
    }
  }

  @Override
  public FlightInfo getFlightInfoSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context,
                                               FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(command.getPlan().getPlan().toByteArray(),
        FlightSqlScenario.SUBSTRAIT_PLAN_TEXT);
    IntegrationAssertions.assertEquals(command.getPlan().getVersion(), FlightSqlScenario.SUBSTRAIT_VERSION);
    String handle = command.getTransactionId().isEmpty() ?
        "PLAN HANDLE" : "PLAN WITH TXN HANDLE";
    FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
        .setStatementHandle(ByteString.copyFromUtf8(handle))
        .build();
    return getFlightInfoForSchema(ticket, descriptor, getQuerySchema());
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                                   CallContext context,
                                                   FlightDescriptor descriptor) {
    String handle = command.getPreparedStatementHandle().toStringUtf8();
    if (handle.equals("SELECT PREPARED STATEMENT HANDLE") ||
        handle.equals("PREPARED PLAN HANDLE")) {
      return getFlightInfoForSchema(command, descriptor, getQuerySchema());
    } else if (handle.equals("SELECT PREPARED STATEMENT WITH TXN HANDLE") ||
        handle.equals("PREPARED PLAN WITH TXN HANDLE")) {
      return getFlightInfoForSchema(command, descriptor, getQueryWithTransactionSchema());
    }
    throw CallStatus.INVALID_ARGUMENT.withDescription("Unknown handle: " + handle).toRuntimeException();
  }

  @Override
  public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
                                         CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(command.getQuery(), "SELECT STATEMENT");
    if (command.getTransactionId().isEmpty()) {
      return new SchemaResult(getQuerySchema());
    }
    return new SchemaResult(getQueryWithTransactionSchema());
  }

  @Override
  public SchemaResult getSchemaPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                                 FlightDescriptor descriptor) {
    String handle = command.getPreparedStatementHandle().toStringUtf8();
    if (handle.equals("SELECT PREPARED STATEMENT HANDLE") ||
        handle.equals("PREPARED PLAN HANDLE")) {
      return new SchemaResult(getQuerySchema());
    } else if (handle.equals("SELECT PREPARED STATEMENT WITH TXN HANDLE") ||
        handle.equals("PREPARED PLAN WITH TXN HANDLE")) {
      return new SchemaResult(getQueryWithTransactionSchema());
    }
    throw CallStatus.INVALID_ARGUMENT.withDescription("Unknown handle: " + handle).toRuntimeException();
  }

  @Override
  public SchemaResult getSchemaSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context,
                                             FlightDescriptor descriptor) {
    if (!Arrays.equals(command.getPlan().getPlan().toByteArray(), FlightSqlScenario.SUBSTRAIT_PLAN_TEXT)) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected plan '%s', not '%s'",
              Arrays.toString(FlightSqlScenario.SUBSTRAIT_PLAN_TEXT),
              Arrays.toString(command.getPlan().getPlan().toByteArray())))
          .toRuntimeException();
    }
    if (!FlightSqlScenario.SUBSTRAIT_VERSION.equals(command.getPlan().getVersion())) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription(String.format("Expected version '%s', not '%s'",
              FlightSqlScenario.SUBSTRAIT_VERSION,
              command.getPlan().getVersion()))
          .toRuntimeException();
    }
    if (command.getTransactionId().isEmpty()) {
      return new SchemaResult(getQuerySchema());
    }
    return new SchemaResult(getQueryWithTransactionSchema());
  }

  @Override
  public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context,
                                 ServerStreamListener listener) {
    final String handle = ticket.getStatementHandle().toStringUtf8();
    if (handle.equals("SELECT STATEMENT HANDLE") || handle.equals("PLAN HANDLE")) {
      putEmptyBatchToStreamListener(listener, getQuerySchema());
    } else if (handle.equals("SELECT STATEMENT WITH TXN HANDLE") || handle.equals("PLAN WITH TXN HANDLE")) {
      putEmptyBatchToStreamListener(listener, getQueryWithTransactionSchema());
    } else {
      listener.error(CallStatus.INVALID_ARGUMENT.withDescription("Unknown handle: " + handle).toRuntimeException());
    }
  }

  @Override
  public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                         CallContext context, ServerStreamListener listener) {
    String handle = command.getPreparedStatementHandle().toStringUtf8();
    if (handle.equals("SELECT PREPARED STATEMENT HANDLE") || handle.equals("PREPARED PLAN HANDLE")) {
      putEmptyBatchToStreamListener(listener, getQuerySchema());
    } else if (handle.equals("SELECT PREPARED STATEMENT WITH TXN HANDLE") ||
        handle.equals("PREPARED PLAN WITH TXN HANDLE")) {
      putEmptyBatchToStreamListener(listener, getQueryWithTransactionSchema());
    } else {
      listener.error(CallStatus.INVALID_ARGUMENT
          .withDescription("Unknown handle: " + handle)
          .toRuntimeException());
    }
  }

  private Runnable acceptPutReturnConstant(StreamListener<PutResult> ackStream, long value) {
    return () -> {
      final FlightSql.DoPutUpdateResult build =
          FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(value).build();

      try (final ArrowBuf buffer = allocator.buffer(build.getSerializedSize())) {
        buffer.writeBytes(build.toByteArray());
        ackStream.onNext(PutResult.metadata(buffer));
        ackStream.onCompleted();
      }
    };
  }

  @Override
  public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                     FlightStream flightStream,
                                     StreamListener<PutResult> ackStream) {
    IntegrationAssertions.assertEquals(command.getQuery(), "UPDATE STATEMENT");
    return acceptPutReturnConstant(ackStream,
        command.getTransactionId().isEmpty() ? FlightSqlScenario.UPDATE_STATEMENT_EXPECTED_ROWS :
            FlightSqlScenario.UPDATE_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
  }

  @Override
  public Runnable acceptPutSubstraitPlan(FlightSql.CommandStatementSubstraitPlan command, CallContext context,
                                         FlightStream flightStream, StreamListener<PutResult> ackStream) {
    IntegrationAssertions.assertEquals(command.getPlan().getPlan().toByteArray(),
        FlightSqlScenario.SUBSTRAIT_PLAN_TEXT);
    IntegrationAssertions.assertEquals(command.getPlan().getVersion(), FlightSqlScenario.SUBSTRAIT_VERSION);
    return acceptPutReturnConstant(ackStream,
        command.getTransactionId().isEmpty() ? FlightSqlScenario.UPDATE_STATEMENT_EXPECTED_ROWS :
            FlightSqlScenario.UPDATE_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                   CallContext context, FlightStream flightStream,
                                                   StreamListener<PutResult> ackStream) {
    final String handle = command.getPreparedStatementHandle().toStringUtf8();
    if (handle.equals("UPDATE PREPARED STATEMENT HANDLE") ||
        handle.equals("PREPARED PLAN HANDLE")) {
      return acceptPutReturnConstant(ackStream, FlightSqlScenario.UPDATE_PREPARED_STATEMENT_EXPECTED_ROWS);
    } else if (handle.equals("UPDATE PREPARED STATEMENT WITH TXN HANDLE") ||
        handle.equals("PREPARED PLAN WITH TXN HANDLE")) {
      return acceptPutReturnConstant(
          ackStream, FlightSqlScenario.UPDATE_PREPARED_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
    }
    return () -> {
      ackStream.onError(CallStatus.INVALID_ARGUMENT
          .withDescription("Unknown handle: " + handle)
          .toRuntimeException());
    };
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                  CallContext context, FlightStream flightStream,
                                                  StreamListener<PutResult> ackStream) {
    final String handle = command.getPreparedStatementHandle().toStringUtf8();
    if (handle.equals("SELECT PREPARED STATEMENT HANDLE") ||
        handle.equals("SELECT PREPARED STATEMENT WITH TXN HANDLE") ||
        handle.equals("PREPARED PLAN HANDLE") ||
        handle.equals("PREPARED PLAN WITH TXN HANDLE")) {
      IntegrationAssertions.assertEquals(getQuerySchema(), flightStream.getSchema());
      return ackStream::onCompleted;
    }
    return () -> {
      ackStream.onError(CallStatus.INVALID_ARGUMENT
          .withDescription("Unknown handle: " + handle)
          .toRuntimeException());
    };
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context,
                                         FlightDescriptor descriptor) {
    if (request.getInfoCount() == 2) {
      // Integration test for the protocol messages
      IntegrationAssertions.assertEquals(request.getInfo(0),
          FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE);
      IntegrationAssertions.assertEquals(request.getInfo(1),
          FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE);
    }
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
  }

  @Override
  public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context,
                               ServerStreamListener listener) {
    if (command.getInfoCount() == 2) {
      // Integration test for the protocol messages
      putEmptyBatchToStreamListener(listener, Schemas.GET_SQL_INFO_SCHEMA);
      return;
    }
    SqlInfoBuilder sqlInfoBuilder = new SqlInfoBuilder()
        .withFlightSqlServerSql(false)
        .withFlightSqlServerSubstrait(true)
        .withFlightSqlServerSubstraitMinVersion("min_version")
        .withFlightSqlServerSubstraitMaxVersion("max_version")
        .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_SAVEPOINT)
        .withFlightSqlServerCancel(true)
        .withFlightSqlServerStatementTimeout(42)
        .withFlightSqlServerTransactionTimeout(7);
    sqlInfoBuilder.send(command.getInfoList(), listener);
  }

  @Override
  public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                CallContext context, ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_TYPE_INFO_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                          FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_TYPE_INFO_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoCatalogs(FlightSql.CommandGetCatalogs request, CallContext context,
                                          FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
  }

  private void putEmptyBatchToStreamListener(ServerStreamListener stream, Schema schema) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      stream.start(root);
      stream.putNext();
      stream.completed();
    }
  }

  @Override
  public void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_CATALOGS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                         FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getCatalog(), "catalog");
    IntegrationAssertions.assertEquals(request.getDbSchemaFilterPattern(),
        "db_schema_filter_pattern");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
  }

  @Override
  public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context,
                               ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_SCHEMAS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTables(FlightSql.CommandGetTables request, CallContext context,
                                        FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getCatalog(), "catalog");
    IntegrationAssertions.assertEquals(request.getDbSchemaFilterPattern(),
        "db_schema_filter_pattern");
    IntegrationAssertions.assertEquals(request.getTableNameFilterPattern(), "table_filter_pattern");
    IntegrationAssertions.assertEquals(request.getTableTypesCount(), 2);
    IntegrationAssertions.assertEquals(request.getTableTypes(0), "table");
    IntegrationAssertions.assertEquals(request.getTableTypes(1), "view");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLES_SCHEMA);
  }

  @Override
  public void getStreamTables(FlightSql.CommandGetTables command, CallContext context,
                              ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_TABLES_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request,
                                            CallContext context, FlightDescriptor descriptor) {
    return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
  }

  @Override
  public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_TABLE_TYPES_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request,
                                             CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getCatalog(), "catalog");
    IntegrationAssertions.assertEquals(request.getDbSchema(), "db_schema");
    IntegrationAssertions.assertEquals(request.getTable(), "table");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_PRIMARY_KEYS_SCHEMA);
  }

  @Override
  public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                   ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_PRIMARY_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request,
                                              CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getCatalog(), "catalog");
    IntegrationAssertions.assertEquals(request.getDbSchema(), "db_schema");
    IntegrationAssertions.assertEquals(request.getTable(), "table");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_EXPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request,
                                              CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getCatalog(), "catalog");
    IntegrationAssertions.assertEquals(request.getDbSchema(), "db_schema");
    IntegrationAssertions.assertEquals(request.getTable(), "table");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_IMPORTED_KEYS_SCHEMA);
  }

  @Override
  public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request,
                                                CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getPkCatalog(), "pk_catalog");
    IntegrationAssertions.assertEquals(request.getPkDbSchema(), "pk_db_schema");
    IntegrationAssertions.assertEquals(request.getPkTable(), "pk_table");
    IntegrationAssertions.assertEquals(request.getFkCatalog(), "fk_catalog");
    IntegrationAssertions.assertEquals(request.getFkDbSchema(), "fk_db_schema");
    IntegrationAssertions.assertEquals(request.getFkTable(), "fk_table");

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_CROSS_REFERENCE_SCHEMA);
  }

  @Override
  public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                    ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_EXPORTED_KEYS_SCHEMA);
  }

  @Override
  public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                    ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_IMPORTED_KEYS_SCHEMA);
  }

  @Override
  public void getStreamCrossReference(FlightSql.CommandGetCrossReference command,
                                      CallContext context, ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_CROSS_REFERENCE_SCHEMA);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void listFlights(CallContext context, Criteria criteria,
                          StreamListener<FlightInfo> listener) {

  }

  private <T extends Message> FlightInfo getFlightInfoForSchema(final T request,
                                                                final FlightDescriptor descriptor,
                                                                final Schema schema) {
    final Ticket ticket = new Ticket(Any.pack(request).toByteArray());
    final List<FlightEndpoint> endpoints = Collections.singletonList(new FlightEndpoint(ticket));

    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }
}
