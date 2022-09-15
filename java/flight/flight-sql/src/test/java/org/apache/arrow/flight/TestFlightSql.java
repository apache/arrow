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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.example.FlightSqlExample;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {

  protected static final Schema SCHEMA_INT_TABLE = new Schema(asList(
      new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
      Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
      Field.nullable("VALUE", MinorType.INT.getType()),
      Field.nullable("FOREIGNID", MinorType.INT.getType())));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY = ImmutableList.of(
      asList("1", "one", "1", "1"), asList("2", "zero", "0", "1"), asList("3", "negative one", "-1", "1"));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_PARAMETER_BINDING = ImmutableList.of(
      asList("1", "one", "1", "1"));
  private static final Map<String, String> GET_SQL_INFO_EXPECTED_RESULTS_MAP = new LinkedHashMap<>();
  private static final String LOCALHOST = "localhost";
  private static BufferAllocator allocator;
  private static FlightServer server;
  private static FlightSqlClient sqlClient;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @BeforeClass
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    server = FlightServer.builder(allocator, serverLocation, new FlightSqlExample(serverLocation))
        .build()
        .start();

    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());

    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE), "Apache Derby");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_CATALOG_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(
            Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_CASE_VALUE),
            Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE_VALUE));
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR_VALUE), "\"");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(
            Integer.toString(FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE_VALUE),
            Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE_VALUE));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    close(sqlClient, server, allocator);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(final List<? extends List<String>> results) {
    return getNonConformingResultsForGetSqlInfo(results,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
        FlightSql.SqlInfo.SQL_DDL_CATALOG,
        FlightSql.SqlInfo.SQL_DDL_SCHEMA,
        FlightSql.SqlInfo.SQL_DDL_TABLE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_CASE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
        FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(
      final List<? extends List<String>> results,
      final FlightSql.SqlInfo... args) {
    final List<List<String>> nonConformingResults = new ArrayList<>();
    if (results.size() == args.length) {
      for (int index = 0; index < results.size(); index++) {
        final List<String> result = results.get(index);
        final String providedName = result.get(0);
        final String expectedName = Integer.toString(args[index].getNumber());
        if (!(GET_SQL_INFO_EXPECTED_RESULTS_MAP.get(providedName).equals(result.get(1)) &&
            providedName.equals(expectedName))) {
          nonConformingResults.add(result);
          break;
        }
      }
    }
    return nonConformingResults;
  }

  @Test
  public void testGetTablesSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, true);
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA));
  }

  @Test
  public void testGetTablesSchemaExcludeSchema() {
    final FlightInfo info = sqlClient.getTables(null, null, null, null, false);
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
  }

  @Test
  public void testGetTablesResultNoSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, null, false)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(null /* TODO No catalog yet */, "SYS", "SYSALIASES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCHECKS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCOLPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCOLUMNS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCONGLOMERATES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSCONSTRAINTS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSDEPENDS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSFILES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSFOREIGNKEYS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSKEYS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSROLES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSROUTINEPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSCHEMAS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSEQUENCES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSTATEMENTS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSSTATISTICS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTABLEPERMS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTABLES", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSTRIGGERS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSUSERS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYS", "SYSVIEWS", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "SYSIBM", "SYSDUMMY1", "SYSTEM TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testGetTablesResultFilteredNoSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, singletonList("TABLE"), false)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA_NO_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(null /* TODO No catalog yet */, "APP", "FOREIGNTABLE", "TABLE"),
          asList(null /* TODO No catalog yet */, "APP", "INTTABLE", "TABLE"));
      collector.checkThat(results, is(expectedResults));
    }
  }

  @Test
  public void testGetTablesResultFilteredWithSchema() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(
                 sqlClient.getTables(null, null, null, singletonList("TABLE"), true)
                     .getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA));
      final List<List<String>> results = getResults(stream);
      final List<List<String>> expectedResults = ImmutableList.of(
          // catalog_name | schema_name | table_name | table_type | table_schema
          asList(
              null /* TODO No catalog yet */,
              "APP",
              "FOREIGNTABLE",
              "TABLE",
              new Schema(asList(
                  new Field("ID", new FieldType(false, MinorType.INT.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("INTEGER")
                          .schemaName("APP")
                          .tableName("FOREIGNTABLE")
                          .precision(10)
                          .scale(0)
                          .isAutoIncrement(true)
                          .build().getMetadataMap()), null),
                  new Field("FOREIGNNAME", new FieldType(true, MinorType.VARCHAR.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("VARCHAR")
                          .schemaName("APP")
                          .tableName("FOREIGNTABLE")
                          .precision(100)
                          .scale(0)
                          .isAutoIncrement(false)
                          .build().getMetadataMap()), null),
                  new Field("VALUE", new FieldType(true, MinorType.INT.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("INTEGER")
                          .schemaName("APP")
                          .tableName("FOREIGNTABLE")
                          .precision(10)
                          .scale(0)
                          .isAutoIncrement(false)
                          .build().getMetadataMap()), null))).toJson()),
          asList(
              null /* TODO No catalog yet */,
              "APP",
              "INTTABLE",
              "TABLE",
              new Schema(asList(
                  new Field("ID", new FieldType(false, MinorType.INT.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("INTEGER")
                          .schemaName("APP")
                          .tableName("INTTABLE")
                          .precision(10)
                          .scale(0)
                          .isAutoIncrement(true)
                          .build().getMetadataMap()), null),
                  new Field("KEYNAME", new FieldType(true, MinorType.VARCHAR.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("VARCHAR")
                          .schemaName("APP")
                          .tableName("INTTABLE")
                          .precision(100)
                          .scale(0)
                          .isAutoIncrement(false)
                          .build().getMetadataMap()), null),
                  new Field("VALUE", new FieldType(true, MinorType.INT.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("INTEGER")
                          .schemaName("APP")
                          .tableName("INTTABLE")
                          .precision(10)
                          .scale(0)
                          .isAutoIncrement(false)
                          .build().getMetadataMap()), null),
                  new Field("FOREIGNID", new FieldType(true, MinorType.INT.getType(), null,
                      new FlightSqlColumnMetadata.Builder()
                          .catalogName("")
                          .typeName("INTEGER")
                          .schemaName("APP")
                          .tableName("INTTABLE")
                          .precision(10)
                          .scale(0)
                          .isAutoIncrement(false)
                          .build().getMetadataMap()), null))).toJson()));
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
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable");
         final FlightStream stream = sqlClient.getStream(
             preparedStatement.execute().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));
      collector.checkThat(getResults(stream), is(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY));
    }
  }

  @Test
  public void testSimplePreparedStatementResultsWithParameterBinding() throws Exception {
    try (PreparedStatement prepare = sqlClient.prepare("SELECT * FROM intTable WHERE id = ?")) {
      final Schema parameterSchema = prepare.getParameterSchema();
      try (final VectorSchemaRoot insertRoot = VectorSchemaRoot.create(parameterSchema, allocator)) {
        insertRoot.allocateNew();

        final IntVector valueVector = (IntVector) insertRoot.getVector(0);
        valueVector.setSafe(0, 1);
        insertRoot.setRowCount(1);

        prepare.setParameters(insertRoot);
        FlightInfo flightInfo = prepare.execute();

        FlightStream stream = sqlClient.getStream(flightInfo
            .getEndpoints()
            .get(0).getTicket());

        collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));
        collector.checkThat(getResults(stream), is(EXPECTED_RESULTS_FOR_PARAMETER_BINDING));
      }
    }
  }

  @Test
  public void testSimplePreparedStatementUpdateResults() throws SQLException {
    try (PreparedStatement prepare = sqlClient.prepare("INSERT INTO INTTABLE (keyName, value ) VALUES (?, ?)");
         PreparedStatement deletePrepare = sqlClient.prepare("DELETE FROM INTTABLE WHERE keyName = ?")) {
      final Schema parameterSchema = prepare.getParameterSchema();
      try (final VectorSchemaRoot insertRoot = VectorSchemaRoot.create(parameterSchema, allocator)) {
        final VarCharVector varCharVector = (VarCharVector) insertRoot.getVector(0);
        final IntVector valueVector = (IntVector) insertRoot.getVector(1);
        final int counter = 10;
        insertRoot.allocateNew();

        final IntStream range = IntStream.range(0, counter);

        range.forEach(i -> {
          valueVector.setSafe(i, i * counter);
          varCharVector.setSafe(i, new Text("value" + i));
        });

        insertRoot.setRowCount(counter);

        prepare.setParameters(insertRoot);
        final long updatedRows = prepare.executeUpdate();

        final long deletedRows;
        try (final VectorSchemaRoot deleteRoot = VectorSchemaRoot.of(varCharVector)) {
          deletePrepare.setParameters(deleteRoot);
          deletedRows = deletePrepare.executeUpdate();
        }

        collector.checkThat(updatedRows, is(10L));
        collector.checkThat(deletedRows, is(10L));
      }
    }
  }

  @Test
  public void testSimplePreparedStatementUpdateResultsWithoutParameters() throws SQLException {
    try (PreparedStatement prepare = sqlClient
        .prepare("INSERT INTO INTTABLE (keyName, value ) VALUES ('test', 1000)");
         PreparedStatement deletePrepare = sqlClient.prepare("DELETE FROM INTTABLE WHERE keyName = 'test'")) {
      final long updatedRows = prepare.executeUpdate();

      final long deletedRows = deletePrepare.executeUpdate();

      collector.checkThat(updatedRows, is(1L));
      collector.checkThat(deletedRows, is(1L));
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
  public void testGetCatalogsSchema() {
    final FlightInfo info = sqlClient.getCatalogs();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA));
  }

  @Test
  public void testGetCatalogsResults() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getCatalogs().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA));
      List<List<String>> catalogs = getResults(stream);
      collector.checkThat(catalogs, is(emptyList()));
    }
  }

  @Test
  public void testGetTableTypesSchema() {
    final FlightInfo info = sqlClient.getTableTypes();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA));
  }

  @Test
  public void testGetTableTypesResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getTableTypes().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA));
      final List<List<String>> tableTypes = getResults(stream);
      final List<List<String>> expectedTableTypes = ImmutableList.of(
          // table_type
          singletonList("SYNONYM"),
          singletonList("SYSTEM TABLE"),
          singletonList("TABLE"),
          singletonList("VIEW")
      );
      collector.checkThat(tableTypes, is(expectedTableTypes));
    }
  }

  @Test
  public void testGetSchemasSchema() {
    final FlightInfo info = sqlClient.getSchemas(null, null);
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA));
  }

  @Test
  public void testGetSchemasResult() throws Exception {
    try (final FlightStream stream =
             sqlClient.getStream(sqlClient.getSchemas(null, null).getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA));
      final List<List<String>> schemas = getResults(stream);
      final List<List<String>> expectedSchemas = ImmutableList.of(
          // catalog_name | schema_name
          asList(null /* TODO Add catalog. */, "APP"),
          asList(null /* TODO Add catalog. */, "NULLID"),
          asList(null /* TODO Add catalog. */, "SQLJ"),
          asList(null /* TODO Add catalog. */, "SYS"),
          asList(null /* TODO Add catalog. */, "SYSCAT"),
          asList(null /* TODO Add catalog. */, "SYSCS_DIAG"),
          asList(null /* TODO Add catalog. */, "SYSCS_UTIL"),
          asList(null /* TODO Add catalog. */, "SYSFUN"),
          asList(null /* TODO Add catalog. */, "SYSIBM"),
          asList(null /* TODO Add catalog. */, "SYSPROC"),
          asList(null /* TODO Add catalog. */, "SYSSTAT"));
      collector.checkThat(schemas, is(expectedSchemas));
    }
  }

  @Test
  public void testGetPrimaryKey() {
    final FlightInfo flightInfo = sqlClient.getPrimaryKeys(TableRef.of(null, null, "INTTABLE"));
    final FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);
    collector.checkThat(results.size(), is(1));

    final List<String> result = results.get(0);

    collector.checkThat(result.get(0), is(""));
    collector.checkThat(result.get(1), is("APP"));
    collector.checkThat(result.get(2), is("INTTABLE"));
    collector.checkThat(result.get(3), is("ID"));
    collector.checkThat(result.get(4), is("1"));
    collector.checkThat(result.get(5), notNullValue());
  }

  @Test
  public void testGetSqlInfoSchema() {
    final FlightInfo info = sqlClient.getSqlInfo();
    collector.checkThat(info.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
  }

  @Test
  public void testGetSqlInfoResults() throws Exception {
    final FlightInfo info = sqlClient.getSqlInfo();
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream)), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithSingleArg() throws Exception {
    final FlightSql.SqlInfo arg = FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME;
    final FlightInfo info = sqlClient.getSqlInfo(arg);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), arg), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithTwoArgs() throws Exception {
    final FlightSql.SqlInfo[] args = {
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION};
    final FlightInfo info = sqlClient.getSqlInfo(args);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), args), is(emptyList()));
    }
  }

  @Test
  public void testGetSqlInfoResultsWithThreeArgs() throws Exception {
    final FlightSql.SqlInfo[] args = {
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
        FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR};
    final FlightInfo info = sqlClient.getSqlInfo(args);
    try (final FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA));
      collector.checkThat(getNonConformingResultsForGetSqlInfo(getResults(stream), args), is(emptyList()));
    }
  }

  @Test
  public void testGetCommandExportedKeys() {
    final FlightStream stream =
        sqlClient.getStream(
            sqlClient.getExportedKeys(TableRef.of(null, null, "FOREIGNTABLE"))
                .getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<Matcher<String>> matchers = asList(
        nullValue(String.class), // pk_catalog_name
        is("APP"), // pk_schema_name
        is("FOREIGNTABLE"), // pk_table_name
        is("ID"), // pk_column_name
        nullValue(String.class), // fk_catalog_name
        is("APP"), // fk_schema_name
        is("INTTABLE"), // fk_table_name
        is("FOREIGNID"), // fk_column_name
        is("1"), // key_sequence
        containsString("SQL"), // fk_key_name
        containsString("SQL"), // pk_key_name
        is("3"), // update_rule
        is("3")); // delete_rule

    Assert.assertEquals(1, results.size());
    for (int i = 0; i < matchers.size(); i++) {
      collector.checkThat(results.get(0).get(i), matchers.get(i));
    }
  }

  @Test
  public void testGetCommandImportedKeys() {
    final FlightStream stream =
        sqlClient.getStream(
            sqlClient.getImportedKeys(TableRef.of(null, null, "INTTABLE"))
                .getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<Matcher<String>> matchers = asList(
        nullValue(String.class), // pk_catalog_name
        is("APP"), // pk_schema_name
        is("FOREIGNTABLE"), // pk_table_name
        is("ID"), // pk_column_name
        nullValue(String.class), // fk_catalog_name
        is("APP"), // fk_schema_name
        is("INTTABLE"), // fk_table_name
        is("FOREIGNID"), // fk_column_name
        is("1"), // key_sequence
        containsString("SQL"), // fk_key_name
        containsString("SQL"), // pk_key_name
        is("3"), // update_rule
        is("3")); // delete_rule

    Assert.assertEquals(1, results.size());
    for (int i = 0; i < matchers.size(); i++) {
      collector.checkThat(results.get(0).get(i), matchers.get(i));
    }
  }

  @Test
  public void testGetTypeInfo() {
    FlightInfo flightInfo = sqlClient.getXdbcTypeInfo();

    FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<List<String>> matchers = ImmutableList.of(
        asList("BIGINT", "-5", "19", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "true",
            "BIGINT", "0", "0",
            null, null, "10", null),
        asList("LONG VARCHAR FOR BIT DATA", "-4", "32700", "X'", "'", emptyList().toString(), "1", "false", "0", "true",
            "false", "false",
            "LONG VARCHAR FOR BIT DATA", null, null, null, null, null, null),
        asList("VARCHAR () FOR BIT DATA", "-3", "32672", "X'", "'", singletonList("length").toString(), "1", "false",
            "2", "true", "false",
            "false", "VARCHAR () FOR BIT DATA", null, null, null, null, null, null),
        asList("CHAR () FOR BIT DATA", "-2", "254", "X'", "'", singletonList("length").toString(), "1", "false", "2",
            "true", "false", "false",
            "CHAR () FOR BIT DATA", null, null, null, null, null, null),
        asList("LONG VARCHAR", "-1", "32700", "'", "'", emptyList().toString(), "1", "true", "1", "true", "false",
            "false",
            "LONG VARCHAR", null, null, null, null, null, null),
        asList("CHAR", "1", "254", "'", "'", singletonList("length").toString(), "1", "true", "3", "true", "false",
            "false", "CHAR", null, null,
            null, null, null, null),
        asList("NUMERIC", "2", "31", null, null, Arrays.asList("precision", "scale").toString(), "1", "false", "2",
            "false", "true", "false",
            "NUMERIC", "0", "31", null, null, "10", null),
        asList("DECIMAL", "3", "31", null, null, Arrays.asList("precision", "scale").toString(), "1", "false", "2",
            "false", "true", "false",
            "DECIMAL", "0", "31", null, null, "10", null),
        asList("INTEGER", "4", "10", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "true",
            "INTEGER", "0", "0",
            null, null, "10", null),
        asList("SMALLINT", "5", "5", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "true",
            "SMALLINT", "0",
            "0", null, null, "10", null),
        asList("FLOAT", "6", "52", null, null, singletonList("precision").toString(), "1", "false", "2", "false",
            "false", "false", "FLOAT", null,
            null, null, null, "2", null),
        asList("REAL", "7", "23", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "false",
            "REAL", null, null,
            null, null, "2", null),
        asList("DOUBLE", "8", "52", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "false",
            "DOUBLE", null,
            null, null, null, "2", null),
        asList("VARCHAR", "12", "32672", "'", "'", singletonList("length").toString(), "1", "true", "3", "true",
            "false", "false", "VARCHAR",
            null, null, null, null, null, null),
        asList("BOOLEAN", "16", "1", null, null, emptyList().toString(), "1", "false", "2", "true", "false", "false",
            "BOOLEAN", null,
            null, null, null, null, null),
        asList("DATE", "91", "10", "DATE'", "'", emptyList().toString(), "1", "false", "2", "true", "false", "false",
            "DATE", "0", "0",
            null, null, "10", null),
        asList("TIME", "92", "8", "TIME'", "'", emptyList().toString(), "1", "false", "2", "true", "false", "false",
            "TIME", "0", "0",
            null, null, "10", null),
        asList("TIMESTAMP", "93", "29", "TIMESTAMP'", "'", emptyList().toString(), "1", "false", "2", "true", "false",
            "false",
            "TIMESTAMP", "0", "9", null, null, "10", null),
        asList("OBJECT", "2000", null, null, null, emptyList().toString(), "1", "false", "2", "true", "false", "false",
            "OBJECT", null,
            null, null, null, null, null),
        asList("BLOB", "2004", "2147483647", null, null, singletonList("length").toString(), "1", "false", "0", null,
            "false", null, "BLOB", null,
            null, null, null, null, null),
        asList("CLOB", "2005", "2147483647", "'", "'", singletonList("length").toString(), "1", "true", "1", null,
            "false", null, "CLOB", null,
            null, null, null, null, null),
        asList("XML", "2009", null, null, null, emptyList().toString(), "1", "true", "0", "false", "false", "false",
            "XML", null, null,
            null, null, null, null));
    collector.checkThat(results, is(matchers));
  }

  @Test
  public void testGetTypeInfoWithFiltering() {
    FlightInfo flightInfo = sqlClient.getXdbcTypeInfo(-5);

    FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<List<String>> matchers = ImmutableList.of(
        asList("BIGINT", "-5", "19", null, null, emptyList().toString(), "1", "false", "2", "false", "false", "true",
            "BIGINT", "0", "0",
            null, null, "10", null));
    collector.checkThat(results, is(matchers));
  }

  @Test
  public void testGetCommandCrossReference() {
    final FlightInfo flightInfo = sqlClient.getCrossReference(TableRef.of(null, null,
        "FOREIGNTABLE"), TableRef.of(null, null, "INTTABLE"));
    final FlightStream stream = sqlClient.getStream(flightInfo.getEndpoints().get(0).getTicket());

    final List<List<String>> results = getResults(stream);

    final List<Matcher<String>> matchers = asList(
        nullValue(String.class), // pk_catalog_name
        is("APP"), // pk_schema_name
        is("FOREIGNTABLE"), // pk_table_name
        is("ID"), // pk_column_name
        nullValue(String.class), // fk_catalog_name
        is("APP"), // fk_schema_name
        is("INTTABLE"), // fk_table_name
        is("FOREIGNID"), // fk_column_name
        is("1"), // key_sequence
        containsString("SQL"), // fk_key_name
        containsString("SQL"), // pk_key_name
        is("3"), // update_rule
        is("3")); // delete_rule

    Assert.assertEquals(1, results.size());
    for (int i = 0; i < matchers.size(); i++) {
      collector.checkThat(results.get(0).get(i), matchers.get(i));
    }
  }

  @Test
  public void testCreateStatementSchema() throws Exception {
    final FlightInfo info = sqlClient.execute("SELECT * FROM intTable");
    collector.checkThat(info.getSchema(), is(SCHEMA_INT_TABLE));

    // Consume statement to close connection before cache eviction
    try (FlightStream stream = sqlClient.getStream(info.getEndpoints().get(0).getTicket())) {
      while (stream.next()) {
        // Do nothing
      }
    }
  }

  @Test
  public void testCreateStatementResults() throws Exception {
    try (final FlightStream stream = sqlClient
        .getStream(sqlClient.execute("SELECT * FROM intTable").getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));
      collector.checkThat(getResults(stream), is(EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY));
    }
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
                final Text data = varcharVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : data.toString());
              }
            } else if (fieldVector instanceof IntVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object data = fieldVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof VarBinaryVector) {
              final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final byte[] data = varbinaryVector.getObject(rowIndex);
                final String output;
                try {
                  output = isNull(data) ?
                      null :
                      MessageSerializer.deserializeSchema(
                          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))).toJson();
                } catch (final IOException e) {
                  throw new RuntimeException("Failed to deserialize schema", e);
                }
                results.get(rowIndex).add(output);
              }
            } else if (fieldVector instanceof DenseUnionVector) {
              final DenseUnionVector denseUnionVector = (DenseUnionVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = denseUnionVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof ListVector) {
              for (int i = 0; i < fieldVector.getValueCount(); i++) {
                if (!fieldVector.isNull(i)) {
                  List<Text> elements = (List<Text>) ((ListVector) fieldVector).getObject(i);
                  List<String> values = new ArrayList<>();

                  for (Text element : elements) {
                    values.add(element.toString());
                  }
                  results.get(i).add(values.toString());
                }
              }

            } else if (fieldVector instanceof UInt4Vector) {
              final UInt4Vector uInt4Vector = (UInt4Vector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = uInt4Vector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof UInt1Vector) {
              final UInt1Vector uInt1Vector = (UInt1Vector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = uInt1Vector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof BitVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object data = fieldVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
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

  @Test
  public void testExecuteUpdate() {
    long insertedCount = sqlClient.executeUpdate("INSERT INTO INTTABLE (keyName, value) VALUES " +
        "('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)");
    collector.checkThat(insertedCount, is(3L));

    long updatedCount = sqlClient.executeUpdate("UPDATE INTTABLE SET keyName = 'KEYNAME1' " +
        "WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'");
    collector.checkThat(updatedCount, is(2L));

    long deletedCount = sqlClient.executeUpdate("DELETE FROM INTTABLE WHERE keyName = 'KEYNAME1'");
    collector.checkThat(deletedCount, is(3L));
  }

  @Test
  public void testQueryWithNoResultsShouldNotHang() throws Exception {
    try (final PreparedStatement preparedStatement = sqlClient.prepare("SELECT * FROM intTable WHERE 1 = 0");
         final FlightStream stream = sqlClient
             .getStream(preparedStatement.execute().getEndpoints().get(0).getTicket())) {
      collector.checkThat(stream.getSchema(), is(SCHEMA_INT_TABLE));

      final List<List<String>> result = getResults(stream);
      collector.checkThat(result, is(emptyList()));
    }
  }
}
