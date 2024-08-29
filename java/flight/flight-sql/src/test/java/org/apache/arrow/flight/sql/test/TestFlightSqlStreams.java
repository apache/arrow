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

package org.apache.arrow.flight.sql.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.arrow.flight.sql.util.FlightStreamUtils.getResults;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.apache.arrow.vector.types.Types.MinorType.INT;
import static org.hamcrest.CoreMatchers.is;

import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.BasicFlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.Message;

public class TestFlightSqlStreams {

  /**
   * A limited {@link FlightSqlProducer} for testing GetTables, GetTableTypes, GetSqlInfo, and limited SQL commands.
   */
  private static class FlightSqlTestProducer extends BasicFlightSqlProducer {

    // Note that for simplicity the getStream* implementations are blocking, but a proper FlightSqlProducer should
    // have non-blocking implementations of getStream*.

    private static final String FIXED_QUERY = "SELECT 1 AS c1 FROM test_table";
    private static final Schema FIXED_SCHEMA = new Schema(asList(
        Field.nullable("c1", Types.MinorType.INT.getType())));

    private BufferAllocator allocator;

    FlightSqlTestProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    protected <T extends Message> List<FlightEndpoint> determineEndpoints(T request, FlightDescriptor flightDescriptor,
                                                                          Schema schema) {
      if (request instanceof FlightSql.CommandGetTables ||
          request instanceof FlightSql.CommandGetTableTypes ||
          request instanceof FlightSql.CommandGetXdbcTypeInfo ||
          request instanceof FlightSql.CommandGetSqlInfo) {
        return Collections.singletonList(new FlightEndpoint(new Ticket(Any.pack(request).toByteArray())));
      } else if (request instanceof FlightSql.CommandStatementQuery &&
          ((FlightSql.CommandStatementQuery) request).getQuery().equals(FIXED_QUERY)) {

        // Tickets from CommandStatementQuery requests should be built using TicketStatementQuery then packed() into
        // a ticket. The content of the statement handle is specific to the FlightSqlProducer. It does not need to
        // be the query. It can be a query ID for example.
        FlightSql.TicketStatementQuery ticketStatementQuery = FlightSql.TicketStatementQuery.newBuilder()
            .setStatementHandle(((FlightSql.CommandStatementQuery) request).getQueryBytes())
            .build();
        return Collections.singletonList(new FlightEndpoint(new Ticket(Any.pack(ticketStatementQuery).toByteArray())));
      }
      throw CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException();
    }

    @Override
    public FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
                                             CallContext context, FlightDescriptor descriptor) {
      return generateFlightInfo(command, descriptor, FIXED_SCHEMA);
    }

    @Override
    public void getStreamStatement(FlightSql.TicketStatementQuery ticket,
                                   CallContext context, ServerStreamListener listener) {
      final String query = ticket.getStatementHandle().toStringUtf8();
      if (!query.equals(FIXED_QUERY)) {
        listener.error(CallStatus.UNIMPLEMENTED.withDescription("Not implemented.").toRuntimeException());
      }

      try (VectorSchemaRoot root = VectorSchemaRoot.create(FIXED_SCHEMA, allocator)) {
        root.setRowCount(1);
        ((IntVector) root.getVector("c1")).setSafe(0, 1);
        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command, CallContext context,
                                 ServerStreamListener listener) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SQL_INFO_SCHEMA, allocator)) {
        root.setRowCount(0);
        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                  CallContext context, ServerStreamListener listener) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TYPE_INFO_SCHEMA, allocator)) {
        root.setRowCount(1);
        ((VarCharVector) root.getVector("type_name")).setSafe(0, new Text("Integer"));
        ((IntVector) root.getVector("data_type")).setSafe(0, INT.ordinal());
        ((IntVector) root.getVector("column_size")).setSafe(0, 400);
        root.getVector("literal_prefix").setNull(0);
        root.getVector("literal_suffix").setNull(0);
        root.getVector("create_params").setNull(0);
        ((IntVector) root.getVector("nullable")).setSafe(0, FlightSql.Nullable.NULLABILITY_NULLABLE.getNumber());
        ((BitVector) root.getVector("case_sensitive")).setSafe(0, 1);
        ((IntVector) root.getVector("nullable")).setSafe(0, FlightSql.Searchable.SEARCHABLE_FULL.getNumber());
        ((BitVector) root.getVector("unsigned_attribute")).setSafe(0, 1);
        root.getVector("fixed_prec_scale").setNull(0);
        ((BitVector) root.getVector("auto_increment")).setSafe(0, 1);
        ((VarCharVector) root.getVector("local_type_name")).setSafe(0, new Text("Integer"));
        root.getVector("minimum_scale").setNull(0);
        root.getVector("maximum_scale").setNull(0);
        ((IntVector) root.getVector("sql_data_type")).setSafe(0, INT.ordinal());
        root.getVector("datetime_subcode").setNull(0);
        ((IntVector) root.getVector("num_prec_radix")).setSafe(0, 10);
        root.getVector("interval_precision").setNull(0);

        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void getStreamTables(FlightSql.CommandGetTables command, CallContext context,
                                ServerStreamListener listener) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
        root.setRowCount(1);
        root.getVector("catalog_name").setNull(0);
        root.getVector("db_schema_name").setNull(0);
        ((VarCharVector) root.getVector("table_name")).setSafe(0, new Text("test_table"));
        ((VarCharVector) root.getVector("table_type")).setSafe(0, new Text("TABLE"));

        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
      try (VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLE_TYPES_SCHEMA, allocator)) {
        root.setRowCount(1);
        ((VarCharVector) root.getVector("table_type")).setSafe(0, new Text("TABLE"));

        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }
  }

  private static BufferAllocator allocator;

  private static FlightServer server;
  private static FlightSqlClient sqlClient;

  @BeforeAll
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure("localhost", 0);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlTestProducer(allocator))
        .build()
        .start();

    final Location clientLocation = Location.forGrpcInsecure("localhost", server.getPort());
    sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    close(sqlClient, server);

    // Manually close all child allocators.
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    close(allocator);
  }

  @Test
  public void testGetTablesResultNoSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, null, false)
                     .getEndpoints().get(0).getTicket())) {
      Assertions.assertAll(
          () -> MatcherAssert.assertThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA)),
          () -> {
            final List<List<String>> results = getResults(stream);
            final List<List<String>> expectedResults = ImmutableList.of(
                // catalog_name | schema_name | table_name | table_type | table_schema
                asList(null, null, "test_table", "TABLE"));
            MatcherAssert.assertThat(results, is(expectedResults));
          }
      );
    }
  }

  @Test
  public void testGetTableTypesResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getTableTypes().getEndpoints().get(0).getTicket())) {
      Assertions.assertAll(
          () -> MatcherAssert.assertThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA)),
          () -> {
            final List<List<String>> tableTypes = getResults(stream);
            final List<List<String>> expectedTableTypes = ImmutableList.of(
                // table_type
                singletonList("TABLE")
            );
            MatcherAssert.assertThat(tableTypes, is(expectedTableTypes));
          }
      );
    }
  }

  @Test
  public void testGetSqlInfoResults() throws Exception {
    final FlightInfo info = sqlClient.getSqlInfo();
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      Assertions.assertAll(
          () -> MatcherAssert.assertThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA)),
          () -> MatcherAssert.assertThat(getResults(stream), is(emptyList()))
      );
    }
  }

  @Test
  public void testGetTypeInfo() throws Exception {
    FlightInfo flightInfo = sqlClient.getXdbcTypeInfo();

    try (FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

      final List<List<String>> results = getResults(stream);

      final List<List<String>> matchers = ImmutableList.of(
          asList("Integer", "4", "400", null, null, "3", "true", null, "true", null, "true",
              "Integer", null, null, "4", null, "10", null));

      MatcherAssert.assertThat(results, is(matchers));
    }
  }

  @Test
  public void testExecuteQuery() throws Exception {
    try (final FlightStream stream = sqlClient
        .getStream(sqlClient.execute(FlightSqlTestProducer.FIXED_QUERY).getEndpoints().get(0).getTicket())) {
      Assertions.assertAll(
          () -> MatcherAssert.assertThat(stream.getSchema(), is(FlightSqlTestProducer.FIXED_SCHEMA)),
          () -> MatcherAssert.assertThat(getResults(stream), is(singletonList(singletonList("1"))))
      );
    }
  }
}
