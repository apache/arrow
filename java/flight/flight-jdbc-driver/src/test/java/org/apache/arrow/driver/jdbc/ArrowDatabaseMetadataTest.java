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

package org.apache.arrow.driver.jdbc;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.test.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.test.adhoc.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

/**
 * Class containing the tests from the {@link ArrowDatabaseMetadata}.
 */
public class ArrowDatabaseMetadataTest {
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createNewTestRule(FLIGHT_SQL_PRODUCER);
  private static final int ROW_COUNT = 10;
  private static final List<List<Object>> EXPECTED_GET_CATALOGS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("catalog #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLE_TYPES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("table_type #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("schema_name #%d", i),
              format("table_name #%d", i),
              format("table_type #%d", i),
              // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
              null, null, null, null, null, null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_SCHEMAS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("schema_name #%d", i),
              format("table_name #%d", i),
              format("column_name #%d", i),
              i,
              format("key_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("pk_catalog_name #%d", i),
              format("pk_schema_name #%d", i),
              format("pk_table_name #%d", i),
              format("pk_column_name #%d", i),
              format("fk_catalog_name #%d", i),
              format("fk_schema_name #%d", i),
              format("fk_table_name #%d", i),
              format("fk_column_name #%d", i),
              i,
              format("fk_key_name #%d", i),
              format("pk_key_name #%d", i),
              i,
              i,
              // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
              null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_PRIMARY_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("schema_name #%d", i),
              format("table_name #%d", i),
              format("column_name #%d", i),
              String.valueOf(i),
              format("key_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private static Connection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException {
    connection = FLIGHT_SERVER_TEST_RULE.getConnection();
    final Message commandGetCatalogs = CommandGetCatalogs.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetCatalogsResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        range(0, ROW_COUNT).forEach(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetCatalogs, commandGetCatalogsResultProducer);

    final Message commandGetTableTypes = CommandGetTableTypes.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetTableTypesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLE_TYPES_SCHEMA, allocator)) {
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT).forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTableTypes, commandGetTableTypesResultProducer);

    final Message commandGetTables = CommandGetTables.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetTablesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTables, commandGetTablesResultProducer);

    final Message commandGetSchemas = CommandGetSchemas.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetSchemasResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .forEach(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetSchemas, commandGetSchemasResultProducer);

    final Message commandGetExportedKeys = CommandGetExportedKeys.newBuilder().setTable("Test").build();
    final Message commandGetImportedKeys = CommandGetImportedKeys.newBuilder().setTable("Test").build();
    final Consumer<ServerStreamListener> commandGetExportedAndImportedKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_IMPORTED_AND_EXPORTED_KEYS_SCHEMA,
               allocator)) {
        final VarCharVector pkCatalogName = (VarCharVector) root.getVector("pk_catalog_name");
        final VarCharVector pkSchemaName = (VarCharVector) root.getVector("pk_schema_name");
        final VarCharVector pkTableName = (VarCharVector) root.getVector("pk_table_name");
        final VarCharVector pkColumnName = (VarCharVector) root.getVector("pk_column_name");
        final VarCharVector fkCatalogName = (VarCharVector) root.getVector("fk_catalog_name");
        final VarCharVector fkSchemaName = (VarCharVector) root.getVector("fk_schema_name");
        final VarCharVector fkTableName = (VarCharVector) root.getVector("fk_table_name");
        final VarCharVector fkColumnName = (VarCharVector) root.getVector("fk_column_name");
        final IntVector keySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector fkKeyName = (VarCharVector) root.getVector("fk_key_name");
        final VarCharVector pkKeyName = (VarCharVector) root.getVector("pk_key_name");
        final UInt1Vector updateRule = (UInt1Vector) root.getVector("update_rule");
        final UInt1Vector deleteRule = (UInt1Vector) root.getVector("delete_rule");
        range(0, ROW_COUNT)
            .peek(i -> pkCatalogName.setSafe(i, new Text(format("pk_catalog_name #%d", i))))
            .peek(i -> pkSchemaName.setSafe(i, new Text(format("pk_schema_name #%d", i))))
            .peek(i -> pkTableName.setSafe(i, new Text(format("pk_table_name #%d", i))))
            .peek(i -> pkColumnName.setSafe(i, new Text(format("pk_column_name #%d", i))))
            .peek(i -> fkCatalogName.setSafe(i, new Text(format("fk_catalog_name #%d", i))))
            .peek(i -> fkSchemaName.setSafe(i, new Text(format("fk_schema_name #%d", i))))
            .peek(i -> fkTableName.setSafe(i, new Text(format("fk_table_name #%d", i))))
            .peek(i -> fkColumnName.setSafe(i, new Text(format("fk_column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .peek(i -> fkKeyName.setSafe(i, new Text(format("fk_key_name #%d", i))))
            .peek(i -> pkKeyName.setSafe(i, new Text(format("pk_key_name #%d", i))))
            .peek(i -> updateRule.setSafe(i, i))
            .forEach(i -> deleteRule.setSafe(i, i));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetExportedKeys, commandGetExportedAndImportedKeysResultProducer);
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetImportedKeys, commandGetExportedAndImportedKeysResultProducer);

    final Message commandGetPrimaryKeys = CommandGetPrimaryKeys.newBuilder().setTable("Test").build();
    final Consumer<ServerStreamListener> commandGetPrimaryKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector columnName = (VarCharVector) root.getVector("column_name");
        final IntVector keySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector keyName = (VarCharVector) root.getVector("key_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .peek(i -> columnName.setSafe(i, new Text(format("column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .forEach(i -> keyName.setSafe(i, new Text(format("key_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetPrimaryKeys, commandGetPrimaryKeysResultProducer);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(connection, FLIGHT_SERVER_TEST_RULE, FLIGHT_SQL_PRODUCER);
  }


  @Test
  public void testGetCatalogsCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      testData(resultSet, EXPECTED_GET_CATALOGS_RESULTS);
    }
  }

  @Test
  public void testGetCatalogsCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      testData(resultSet, singletonList("TABLE_CAT"), EXPECTED_GET_CATALOGS_RESULTS);
    }
  }

  @Test
  public void testTableTypesCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      testData(resultSet, EXPECTED_GET_TABLE_TYPES_RESULTS);
    }
  }

  @Test
  public void testTableTypesCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      testData(resultSet, singletonList("TABLE_TYPE"), EXPECTED_GET_TABLE_TYPES_RESULTS);
    }
  }

  @Test
  public void testGetTablesCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      testData(resultSet, EXPECTED_GET_TABLES_RESULTS);
    }
  }

  @Test
  public void testGetTablesCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      testData(
          resultSet,
          ImmutableList.of(
              "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
              "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM",
              "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION"),
          EXPECTED_GET_TABLES_RESULTS
      );
    }
  }

  @Test
  public void testGetSchemasCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      testData(resultSet, EXPECTED_GET_SCHEMAS_RESULTS);
    }
  }

  @Test
  public void testGetSchemasCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      testData(resultSet, ImmutableList.of("TABLE_SCHEM", "TABLE_CAT"), EXPECTED_GET_SCHEMAS_RESULTS);
    }
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, "Test")) {
      testData(resultSet, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, "Test")) {
      testData(
          resultSet,
          ImmutableList.of(
              "PKTABLE_CAT",
              "PKTABLE_NAME",
              "FKTABLE_CAT",
              "FKTABLE_NAME",
              "KEY_SEQ",
              "PK_NAME",
              "DELETE_RULE"),
          EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS
      );
    }
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, "Test")) {
      testData(resultSet, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, "Test")) {
      testData(resultSet,
          ImmutableList.of("PKTABLE_CAT",
              "PKTABLE_NAME",
              "FKTABLE_CAT",
              "FKTABLE_NAME",
              "KEY_SEQ",
              "PK_NAME",
              "DELETE_RULE"),
          EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS
      );
    }
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Test")) {
      testData(resultSet, EXPECTED_PRIMARY_KEYS_RESULTS);
    }
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Test")) {
      testData(resultSet,
          ImmutableList.of(
              "PKTABLE_CAT",
              "TABLE_CAT",
              "TABLE_NAME",
              "KEY_SEQ"),
          EXPECTED_PRIMARY_KEYS_RESULTS
      );
    }
  }

  private <T> void testData(final ResultSet resultSet, final List<List<T>> expectedResults) throws SQLException {
    testData(resultSet, range(0, resultSet.getMetaData().getColumnCount()).toArray(), expectedResults);
  }

  @SuppressWarnings("unchecked")
  private <T> void testData(final ResultSet resultSet, final List<String> columnNames,
                            final List<List<T>> expectedResults) throws SQLException {
    testData(
        resultSet,
        data -> {
          final List<T> columns = new ArrayList<>();
          for (final String columnName : columnNames) {
            try {
              columns.add((T) resultSet.getObject(columnName));
            } catch (final SQLException e) {
              collector.addError(e);
            }
          }
          return columns;
        },
        expectedResults);
  }

  @SuppressWarnings("unchecked")
  private <T> void testData(final ResultSet resultSet, final int[] columnIndices,
                            final List<List<T>> expectedResults) throws SQLException {
    testData(
        resultSet,
        data -> {
          final List<T> columns = new ArrayList<>();
          for (final int columnIndex : columnIndices) {
            try {
              columns.add((T) resultSet.getObject(columnIndex));
            } catch (final SQLException e) {
              collector.addError(e);
            }
          }
          return columns;
        },
        expectedResults);
  }

  private <T> void testData(final ResultSet resultSet, final Function<ResultSet, List<T>> dataConsumer,
                            final List<List<T>> expectedResults) throws SQLException {
    final List<List<T>> actualResults = new ArrayList<>();
    while (resultSet.next()) {
      actualResults.add(dataConsumer.apply(resultSet));
    }
    collector.checkThat(actualResults, is(expectedResults));
  }
}

