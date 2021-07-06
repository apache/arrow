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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlExample;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {
  private static BufferAllocator allocator;
  private static FlightServer server;

  private static FlightClient client;
  private static FlightSqlClient sqlClient;

  protected static final Schema SCHEMA_INT_TABLE = new Schema(Arrays.asList(
          new Field("KEYNAME", FieldType.nullable(Types.MinorType.VARCHAR.getType()), null),
          new Field("VALUE", FieldType.nullable(Types.MinorType.INT.getType()), null)));

  @BeforeClass
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, 0);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlExample(serverLocation)).build();
    server.start();

    final Location clientLocation = Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, server.getPort());
    client = FlightClient.builder(allocator, clientLocation).build();
    sqlClient = new FlightSqlClient(client);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(client, server, allocator);
  }

  @Test
  public void testGetTables() throws Exception {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, false);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      final List<List<String>> results = getResults(stream);
      Assertions.assertEquals(1, results.size());
      Assertions.assertEquals(
          Arrays.asList(null, "APP", "INTTABLE", "TABLE", SCHEMA_INT_TABLE.toJson()), results.get(0));
    }
  }

  @Test
  public void testSimplePrepStmt() throws Exception {
    final FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare("Select * from intTable");

    final Schema actualSchema = preparedStatement.getResultSetSchema();
    Assertions.assertEquals(SCHEMA_INT_TABLE, actualSchema);

    final FlightInfo info = preparedStatement.execute();
    Assertions.assertEquals(SCHEMA_INT_TABLE, info.getSchema());

    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      Assertions.assertEquals(SCHEMA_INT_TABLE, stream.getSchema());

      final List<List<String>> results = getResults(stream);
      Assertions.assertEquals(3, results.size());
      Assertions.assertEquals(Arrays.asList("one", "1"), results.get(0));
      Assertions.assertEquals(Arrays.asList("zero", "0"), results.get(1));
      Assertions.assertEquals(Arrays.asList("negative one", "-1"), results.get(2));
    }

    AutoCloseables.close(preparedStatement);
    Assertions.assertTrue(preparedStatement.isClosed());
  }

  List<List<String>> getResults(FlightStream stream) {
    final List<List<String>> results = new ArrayList<>();
    while (stream.next()) {
      final VectorSchemaRoot root = stream.getRoot();
      final long rowCount = root.getRowCount();
      for (int i = 0; i < rowCount; ++i) {
        results.add(new ArrayList<>());
      }

      for (Field field : root.getSchema().getFields()) {
        final FieldVector fieldVector = root.getVector(field.getName());

        if (fieldVector instanceof VarCharVector) {
          final VarCharVector varcharVector = (VarCharVector) fieldVector;
          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            results.get(rowIndex).add(varcharVector.getObject(rowIndex).toString());
          }
        } else if (fieldVector instanceof IntVector) {
          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            results.get(rowIndex).add(String.valueOf(((IntVector) fieldVector).get(rowIndex)));
          }
        } else if (fieldVector instanceof VarBinaryVector) {
          final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
          for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            results.get(rowIndex).add(Schema.deserialize(ByteBuffer.wrap(varbinaryVector.get(rowIndex))).toJson());
          }
        } else {
          throw new UnsupportedOperationException("Not yet implemented");
        }
      }
    }
    stream.getRoot().clear();
    return results;
  }
}
