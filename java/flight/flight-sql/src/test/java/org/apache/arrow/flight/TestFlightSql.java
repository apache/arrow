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

import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.is;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlExample;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {

  protected static final Schema SCHEMA_INT_TABLE = new Schema(asList(
      Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
      Field.nullable("VALUE", MinorType.INT.getType())));
  private static final String LOCALHOST = "localhost";
  private static int port;
  private static BufferAllocator allocator;
  private static FlightServer server;
  private static FlightClient client;
  private static FlightSqlClient sqlClient;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUp() throws Exception {
    try (final Reader reader = new BufferedReader(
        new FileReader("target/generated-test-resources/network.properties"))) {
      final Properties properties = new Properties();
      properties.load(reader);
      port = Integer.parseInt(Objects.toString(properties.get("server.port")));
    }

    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, port);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlExample(serverLocation))
        .build()
        .start();

    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    client = FlightClient.builder(allocator, clientLocation).build();
    sqlClient = new FlightSqlClient(client);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close(client, server, allocator);
  }

  @Test
  public void testGetTablesSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, false);
    final Schema expectedInfoSchema = new Schema(asList(
        Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
        Field.nullable("schema_name", MinorType.VARCHAR.getType()),
        Field.nullable("table_name", MinorType.VARCHAR.getType()),
        Field.nullable("table_type", MinorType.VARCHAR.getType()),
        Field.nullable("table_schema", MinorType.VARBINARY.getType())));
    collector.checkThat(info.getSchema(), is(expectedInfoSchema));
  }

  @Test
  public void testGetTablesResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, null, false)
                     .getEndpoints().get(0).getTicket())) {
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList("" /* TODO No catalog yet */, "SYS", "SYSALIASES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSCHECKS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSCOLPERMS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSCOLUMNS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSCONGLOMERATES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSCONSTRAINTS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSDEPENDS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSFILES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSFOREIGNKEYS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSKEYS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSPERMS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSROLES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSROUTINEPERMS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSSCHEMAS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSSEQUENCES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSSTATEMENTS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSSTATISTICS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSTABLEPERMS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSTABLES", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSTRIGGERS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSUSERS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYS", "SYSVIEWS", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "SYSIBM", "SYSDUMMY1", "SYSTEM TABLE"),
          asList("" /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testGetTablesResultFiltered() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, singletonList("TABLE"), false)
                     .getEndpoints().get(0).getTicket())) {
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList("" /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testSimplePreparedStatementSchema() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable")) {
      final Schema actualSchema = preparedStatement.getResultSetSchema();
      collector.checkThat(actualSchema, is(SCHEMA_INT_TABLE));

      final FlightInfo info = preparedStatement.execute();
      collector.checkThat(info.getSchema(), is(SCHEMA_INT_TABLE));
    }
  }

  @Test
  public void testSimplePreparedStatementResults() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.prepare("SELECT * FROM intTable")
                     .execute()
                     .getEndpoints()
                     .get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));

      final List<List<String>> result = getResults(stream);
      final List<List<String>> expected = asList(
          asList("one", "1"), asList("zero", "0"), asList("negative one", "-1"));

      collector.checkThat(result, is(expected));
    }
  }

  @Test
  public void testSimplePreparedStatementClosesProperly() {
    final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable");
    collector.checkThat(preparedStatement.isClosed(), is(false));
    preparedStatement.close();
    collector.checkThat(preparedStatement.isClosed(), is(true));
  }

  @Test
  @Ignore // TODO
  public void testGetCatalogsSchema() throws Exception {
    /*
    final FlightInfo info = sqlClient.getCatalogs();
    final Schema infoSchema = info.getSchema();
    final Schema expectedInfoSchema =
        new Schema(singletonList(Field.nullable("catalog_name", MinorType.VARCHAR.getType())));
    collector.checkThat(infoSchema, is(expectedInfoSchema));
   */
  }

  @Test
  @Ignore // TODO
  public void testGetCatalogs() throws Exception {
    /*
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket())) {
      List<List<String>> catalogs = getResults(stream);
      collector.checkThat(catalogs, is(emptyList()));
    }
    */
  }

  @Test
  @Ignore // TODO
  public void testGetTableTypesSchema() {
    /*
    final FlightInfo info = sqlClient.getTableTypes();
    final Schema infoSchema = info.getSchema();
    final Schema expectedInfoSchema =
        new Schema(singletonList(Field.nullable("table_type", MinorType.VARCHAR.getType())));
    collector.checkThat(infoSchema, is(expectedInfoSchema));
    */
  }

  @Test
  @Ignore // TODO
  public void testGetTableTypesResult() throws Exception {
    /*
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getTableTypes().getEndpoints().get(0).getTicket())) {
      List<List<String>> catalogs = getResults(stream);
      collector.checkThat(catalogs, is(allOf(notNullValue(), not(emptyList()))));
    }
    */
  }

  @Test
  @Ignore // TODO
  public void testGetSchemasSchema() {
    /*
    final FlightInfo info = sqlClient.getSchemas(null, null);
    final Schema infoSchema = info.getSchema();
    final Schema expectedSchema = new Schema(asList(
        Field.nullable("catalog_name", MinorType.VARCHAR.getType()),
        Field.nullable("schema_name", MinorType.VARCHAR.getType())));
    collector.checkThat(infoSchema, is(expectedSchema));
    */
  }

  @Test
  @Ignore // TODO
  public void testGetSchemasResult() throws Exception {
    /*
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
      final List<List<String>> schemas = getResults(stream);
      collector.checkThat(schemas, is(allOf(notNullValue(), not(emptyList()))));
    }
    */
  }

  List<List<String>> getResults(FlightStream stream) {
    final List<List<String>> results = new ArrayList<>();
    while (stream.next()) {
      try (final VectorSchemaRoot root = stream.getRoot()) {
        final long rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; ++i) {
          results.add(new ArrayList<>());
        }

        root.getSchema().getFields().forEach(field -> {
          try (final FieldVector fieldVector = root.getVector(field.getName())) {
            if (fieldVector instanceof VarCharVector) {
              final VarCharVector varcharVector = (VarCharVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object obj = varcharVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(obj) ? null : obj.toString());
              }
            } else if (fieldVector instanceof IntVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                results.get(rowIndex).add(String.valueOf(((IntVector) fieldVector).get(rowIndex)));
              }
            } else if (fieldVector instanceof VarBinaryVector) {
              final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                results.get(rowIndex).add(Schema.deserialize(wrap(varbinaryVector.get(rowIndex))).toJson());
              }
            } else {
              throw new UnsupportedOperationException("Not yet implemented");
            }
          }
        });
      }
    }

    return results;
  }
}
