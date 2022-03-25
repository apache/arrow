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

import static com.google.protobuf.Any.pack;
import static java.util.Collections.singletonList;

import java.util.List;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.protobuf.ByteString;
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
        singletonList(
            new Field("id", new FieldType(true, new ArrowType.Int(64, true),
                null, new FlightSqlColumnMetadata.Builder()
                .tableName("test")
                .isAutoIncrement(true)
                .isCaseSensitive(false)
                .schemaName("schema_test")
                .isSearchable(true)
                .catalogName("catalog_test")
                .precision(100)
                .build().getMetadataMap()), null)
        )
    );
  }

  @Override
  public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request,
                                      CallContext context, StreamListener<Result> listener) {
    IntegrationAssertions.assertTrue("Expect to be one of the two queries used on tests",
        request.getQuery().equals("SELECT PREPARED STATEMENT") ||
            request.getQuery().equals("UPDATE PREPARED STATEMENT"));

    final FlightSql.ActionCreatePreparedStatementResult
        result = FlightSql.ActionCreatePreparedStatementResult.newBuilder()
        .setPreparedStatementHandle(ByteString.copyFromUtf8(request.getQuery() + " HANDLE"))
        .build();
    listener.onNext(new Result(pack(result).toByteArray()));
    listener.onCompleted();
  }

  @Override
  public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request,
                                     CallContext context, StreamListener<Result> listener) {
    IntegrationAssertions.assertTrue("Expect to be one of the two queries used on tests",
        request.getPreparedStatementHandle().toStringUtf8().equals("SELECT PREPARED STATEMENT HANDLE") ||
            request.getPreparedStatementHandle().toStringUtf8().equals("UPDATE PREPARED STATEMENT HANDLE"));

    listener.onCompleted();
  }

  @Override
  public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
                                           CallContext context, FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(command.getQuery(), "SELECT STATEMENT");

    ByteString handle = ByteString.copyFromUtf8("SELECT STATEMENT HANDLE");

    FlightSql.TicketStatementQuery ticket = FlightSql.TicketStatementQuery.newBuilder()
        .setStatementHandle(handle)
        .build();
    return getFlightInfoForSchema(ticket, descriptor, getQuerySchema());
  }

  @Override
  public FlightInfo getFlightInfoPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                                   CallContext context,
                                                   FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(command.getPreparedStatementHandle().toStringUtf8(),
        "SELECT PREPARED STATEMENT HANDLE");

    return getFlightInfoForSchema(command, descriptor, getQuerySchema());
  }

  @Override
  public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command,
                                         CallContext context, FlightDescriptor descriptor) {
    return new SchemaResult(getQuerySchema());
  }

  @Override
  public void getStreamStatement(FlightSql.TicketStatementQuery ticket, CallContext context,
                                 ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, getQuerySchema());
  }

  @Override
  public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command,
                                         CallContext context, ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, getQuerySchema());
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

    return acceptPutReturnConstant(ackStream, FlightSqlScenario.UPDATE_STATEMENT_EXPECTED_ROWS);
  }

  @Override
  public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                   CallContext context, FlightStream flightStream,
                                                   StreamListener<PutResult> ackStream) {
    IntegrationAssertions.assertEquals(command.getPreparedStatementHandle().toStringUtf8(),
        "UPDATE PREPARED STATEMENT HANDLE");

    return acceptPutReturnConstant(ackStream, FlightSqlScenario.UPDATE_PREPARED_STATEMENT_EXPECTED_ROWS);
  }

  @Override
  public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                  CallContext context, FlightStream flightStream,
                                                  StreamListener<PutResult> ackStream) {
    IntegrationAssertions.assertEquals(command.getPreparedStatementHandle().toStringUtf8(),
        "SELECT PREPARED STATEMENT HANDLE");

    IntegrationAssertions.assertEquals(getQuerySchema(), flightStream.getSchema());

    return ackStream::onCompleted;
  }

  @Override
  public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request, CallContext context,
                                         FlightDescriptor descriptor) {
    IntegrationAssertions.assertEquals(request.getInfoCount(), 2);
    IntegrationAssertions.assertEquals(request.getInfo(0),
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE);
    IntegrationAssertions.assertEquals(request.getInfo(1),
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE);

    return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
  }

  @Override
  public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context,
                               ServerStreamListener listener) {
    putEmptyBatchToStreamListener(listener, Schemas.GET_SQL_INFO_SCHEMA);
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
    final Ticket ticket = new Ticket(pack(request).toByteArray());
    final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket));

    return new FlightInfo(schema, descriptor, endpoints, -1, -1);
  }
}
