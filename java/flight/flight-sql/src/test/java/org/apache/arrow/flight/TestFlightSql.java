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

package org.apache.arrow.flight;

import static org.apache.arrow.flight.sql.FlightSqlClientUtils.getPreparedStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.flight.sql.FlightSqlClientUtils;
import org.apache.arrow.flight.sql.FlightSqlExample;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionClosePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementResult;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandPreparedStatementQuery;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ElementAddressableVectorIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {
  private static BufferAllocator allocator;
  private static FlightServer server;

  private static FlightClient client;

  protected static final Schema SCHEMA_INT_TABLE = new Schema(Arrays.asList(
          new Field("KEYNAME", new
                  FieldType(true, ArrowType.Utf8.INSTANCE, null),
                  null),
          new Field("VALUE",
                  new FieldType(true, new ArrowType.Int(32, true), null),
                  null)));

  @BeforeClass
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlExample(serverLocation)).build();
    server.start();

    final Location clientLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
    client = FlightClient.builder(allocator, clientLocation).build();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }

  /*
  @Test
  public void testGetTables() throws Exception {
    // Arrange
    final ActionGetTablesResult expected = ActionGetTablesResult.newBuilder()
            .setSchema("APP")
            .setTable("INTTABLE")
            .setTableType("TABLE")
            .setArrowMetadata(ByteString.copyFrom(SCHEMA_INT_TABLE.toByteArray()))
            .build();

    // Act
    final Iterator<Result> results = client.doAction(new Action("GetTables",
            Any.pack(ActionGetTablesRequest
                    .newBuilder()
                    .addTableTypes("TABLE")
                    .setIncludeSchema(true)
                    .build())
                    .toByteArray()));

    // Assert
    while (results.hasNext()) {
      ActionGetTablesResult actual = Any.parseFrom(results.next().getBody()).unpack(ActionGetTablesResult.class);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testGetTablesWithFlightSqlClientUtils() throws Exception {
    // Arrange
    final ActionGetTablesResult expected = ActionGetTablesResult.newBuilder()
            .setSchema("APP")
            .setTable("INTTABLE")
            .setTableType("TABLE")
            .setArrowMetadata(ByteString.copyFrom(SCHEMA_INT_TABLE.toByteArray()))
            .build();

    // Act
    final List<ActionGetTablesResult> results = getTables(client, null, null, null,
            Collections.singletonList("TABLE"), true);

    // Assert
    assertEquals(1, results.size());
    assertEquals(expected, results.get(0));
  }
  */

  @Test
  public void testSimplePrepStmt() throws Exception {
    final Iterator<Result> preparedStatementResults = client.doAction(new Action("GetPreparedStatement",
            Any.pack(ActionCreatePreparedStatementRequest
                    .newBuilder()
                    .setQuery("Select * from intTable")
                    .build())
                    .toByteArray()));

    assertTrue(preparedStatementResults.hasNext());
    final ActionCreatePreparedStatementResult preparedStatementResult =
            Any.parseFrom(preparedStatementResults.next().getBody()).unpack(ActionCreatePreparedStatementResult.class);
    assertFalse(preparedStatementResults.hasNext());

    final Schema actualSchema = Schema.deserialize(preparedStatementResult.getDatasetSchema().asReadOnlyByteBuffer());
    assertEquals(SCHEMA_INT_TABLE, actualSchema);

    final FlightDescriptor descriptor = FlightDescriptor
            .command(Any.pack(CommandPreparedStatementQuery.newBuilder()
                    .setClientExecutionHandle(ByteString.copyFrom(new byte[]{1, 2, 3, 4}))
                    .setPreparedStatementHandle(preparedStatementResult.getPreparedStatementHandle())
                    .build())
                    .toByteArray());

    final FlightInfo info = client.getInfo(descriptor);
    assertEquals(SCHEMA_INT_TABLE, info.getSchema());

    final FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    assertEquals(SCHEMA_INT_TABLE, stream.getSchema());

    List<String> actualStringResults = new ArrayList<>();
    List<Integer> actualIntResults = new ArrayList<>();
    while (stream.next()) {
      final VectorSchemaRoot root = stream.getRoot();
      final long rowCount = root.getRowCount();

      for (Field field : root.getSchema().getFields()) {
        final FieldVector fieldVector = root.getVector(field.getName());

        if (fieldVector instanceof VarCharVector) {

          final ElementAddressableVectorIterator<VarCharVector> it =
                  new ElementAddressableVectorIterator<>((VarCharVector) fieldVector);

          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            final ArrowBufPointer pt = it.next();
            final byte[] bytes = new byte[(int) pt.getLength()];
            pt.getBuf().getBytes(pt.getOffset(), bytes);

            actualStringResults.add(new String(bytes, StandardCharsets.UTF_8));
          }
        } else if (fieldVector instanceof IntVector) {
          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            actualIntResults.add(((IntVector) fieldVector).get(rowIndex));
          }
        }
      }
    }
    stream.getRoot().clear();

    assertEquals(Arrays.asList("one", "zero", "negative one"), actualStringResults);
    assertEquals(Arrays.asList(1, 0, -1), actualIntResults);

    final Iterator<Result> closePreparedStatementResults = client.doAction(new Action("ClosePreparedStatement",
            Any.pack(ActionClosePreparedStatementRequest
                    .newBuilder()
                    .setPreparedStatementHandleBytes(preparedStatementResult.getPreparedStatementHandle())
                    .build())
                    .toByteArray()));
    assertFalse(closePreparedStatementResults.hasNext());
  }

  @Test
  public void testSimplePrepStmtWithFlightSqlClientUtils() throws Exception {
    final FlightSqlClientUtils.FlightSqlPreparedStatement preparedStatement =
            getPreparedStatement(client, "Select * from intTable");

    final Schema actualSchema = preparedStatement.getResultSetSchema();
    assertEquals(SCHEMA_INT_TABLE, actualSchema);

    final FlightInfo info = preparedStatement.executeQuery();
    assertEquals(SCHEMA_INT_TABLE, info.getSchema());

    final FlightStream stream = client.getStream(info.getEndpoints().get(0).getTicket());
    assertEquals(SCHEMA_INT_TABLE, stream.getSchema());

    List<String> actualStringResults = new ArrayList<>();
    List<Integer> actualIntResults = new ArrayList<>();
    while (stream.next()) {
      final VectorSchemaRoot root = stream.getRoot();
      final long rowCount = root.getRowCount();

      for (Field field : root.getSchema().getFields()) {
        final FieldVector fieldVector = root.getVector(field.getName());

        if (fieldVector instanceof VarCharVector) {

          final ElementAddressableVectorIterator<VarCharVector> it =
                  new ElementAddressableVectorIterator<>((VarCharVector) fieldVector);

          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            final ArrowBufPointer pt = it.next();
            final byte[] bytes = new byte[(int) pt.getLength()];
            pt.getBuf().getBytes(pt.getOffset(), bytes);

            actualStringResults.add(new String(bytes, StandardCharsets.UTF_8));
          }
        } else if (fieldVector instanceof IntVector) {
          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            actualIntResults.add(((IntVector) fieldVector).get(rowIndex));
          }
        }
      }
    }
    stream.getRoot().clear();

    assertEquals(Arrays.asList("one", "zero", "negative one"), actualStringResults);
    assertEquals(Arrays.asList(1, 0, -1), actualIntResults);

    AutoCloseables.close(preparedStatement);
    assertTrue(preparedStatement.isClosed());
  }
}
