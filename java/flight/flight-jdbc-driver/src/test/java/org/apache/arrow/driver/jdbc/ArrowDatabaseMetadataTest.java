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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;

/**
 * Class containing the tests from the {@link ArrowDatabaseMetadata}.
 */
public class ArrowDatabaseMetadataTest {
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createNewTestRule(FLIGHT_SQL_PRODUCER);

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
        final int rows = 10;
        range(0, rows).forEach(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))));
        root.setRowCount(rows);
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
        final int rows = 10;
        range(0, rows).forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(rows);
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
        final int rows = 10;
        range(0, rows)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(rows);
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
        final int rows = 10;
        range(0, rows)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .forEach(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))));
        root.setRowCount(rows);
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
        final int rows = 10;
        range(0, rows)
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
        root.setRowCount(rows);
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
        final int rows = 10;
        range(0, rows)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .peek(i -> columnName.setSafe(i, new Text(format("column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .forEach(i -> keyName.setSafe(i, new Text(format("key_name #%d", i))));
        root.setRowCount(rows);
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
    final List<List<String>> expectedCatalogs =
        range(0, 10)
            .mapToObj(i -> format("catalog #%d", i))
            .map(Collections::singletonList)
            .collect(toList());
    final List<List<String>> actualCatalogs = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualCatalogs.add(columns);
      }
    }
    collector.checkThat(actualCatalogs, is(expectedCatalogs));
  }

  @Test
  public void testGetCatalogsCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedCatalogs =
        range(0, 10)
            .mapToObj(i -> format("catalog #%d", i))
            .map(Collections::singletonList)
            .collect(toList());
    final List<List<String>> actualCatalogs = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      while (resultSet.next()) {
        actualCatalogs.add(singletonList(resultSet.getString("TABLE_CAT")));
      }
    }
    collector.checkThat(actualCatalogs, is(expectedCatalogs));
  }

  @Test
  public void testTableTypesCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedTableTypes =
        range(0, 10)
            .mapToObj(i -> format("table_type #%d", i))
            .map(Collections::singletonList)
            .collect(toList());
    final List<List<String>> actualTableTypes = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualTableTypes.add(columns);
      }
    }
    collector.checkThat(actualTableTypes, is(expectedTableTypes));
  }

  @Test
  public void testTableTypesCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedTableTypes =
        range(0, 10).mapToObj(i -> format("table_type #%d", i)).map(Collections::singletonList).collect(toList());
    final List<List<String>> actualTableTypes = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      while (resultSet.next()) {
        actualTableTypes.add(singletonList(resultSet.getString("TABLE_TYPE")));
      }
    }
    collector.checkThat(actualTableTypes, is(expectedTableTypes));
  }

  @Test
  public void testGetTablesCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedTables =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("catalog_name #%d", i),
                format("schema_name #%d", i),
                format("table_name #%d", i),
                format("table_type #%d", i),
                // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
                null, null, null, null, null, null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualTables = new ArrayList<>();
    // FIXME Seems to be broken. Should accept null results...
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      while (resultSet.next()) {
        final List<String> tables = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          tables.add(resultSet.getString(column + 1));
        }
        actualTables.add(tables);
      }
    }
    collector.checkThat(actualTables, is(expectedTables));
  }

  @Test
  public void testGetTablesCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedTables =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("catalog_name #%d", i),
                format("schema_name #%d", i),
                format("table_name #%d", i),
                format("table_type #%d", i),
                // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
                null, null, null, null, null, null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualTables = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      while (resultSet.next()) {
        actualTables.add(
            ImmutableList.of(
                resultSet.getString("TABLE_CAT"), resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"), resultSet.getString("TABLE_TYPE"),
                resultSet.getString("REMARKS"), resultSet.getString("TYPE_CAT"),
                resultSet.getString("TYPE_SCHEM"), resultSet.getString("TYPE_NAME"),
                resultSet.getString("SELF_REFERENCING_COL_NAME"), resultSet.getString("REF_GENERATION")));
      }
    }
    collector.checkThat(actualTables, is(expectedTables));
  }

  @Test
  public void testGetSchemasCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedSchemas =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("schema_name #%d", i), format("catalog_name #%d", i)})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualSchemas.add(columns);
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }

  @Test
  public void testGetSchemasCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedSchemas =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("schema_name #%d", i), format("catalog_name #%d", i)})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      while (resultSet.next()) {
        actualSchemas.add(
            ImmutableList.of(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_CATALOG")));
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedKeys =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("pk_catalog_name #%d", i),
                format("pk_schema_name #%d", i),
                format("pk_table_name #%d", i),
                format("pk_column_name #%d", i),
                format("fk_catalog_name #%d", i),
                format("fk_schema_name #%d", i),
                format("fk_table_name #%d", i),
                format("fk_column_name #%d", i),
                String.valueOf(i),
                format("fk_key_name #%d", i),
                format("pk_key_name #%d", i),
                String.valueOf(i),
                String.valueOf(i),
                // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
                null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualKeys.add(columns);
      }
    }
    collector.checkThat(actualKeys, is(expectedKeys));
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedKeys =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("pk_catalog_name #%d", i),
                format("pk_schema_name #%d", i),
                format("pk_table_name #%d", i),
                format("pk_column_name #%d", i),
                format("fk_catalog_name #%d", i),
                format("fk_schema_name #%d", i),
                format("fk_table_name #%d", i),
                format("fk_column_name #%d", i),
                String.valueOf(i),
                format("fk_key_name #%d", i),
                format("pk_key_name #%d", i),
                String.valueOf(i),
                String.valueOf(i),
                // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
                null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        actualKeys.add(
            ImmutableList.of(
                resultSet.getString("PKTABLE_CAT"), resultSet.getString("PKTABLE_SCHEM"),
                resultSet.getString("PKTABLE_NAME"), resultSet.getString("PKCOLUMN_NAME"),
                resultSet.getString("FKTABLE_CAT"), resultSet.getString("FKTABLE_SCHEM"),
                resultSet.getString("FKTABLE_NAME"), resultSet.getString("FKCOLUMN_NAME"),
                resultSet.getString("KEY_SEQ"), resultSet.getString("FK_NAME"),
                resultSet.getString("PK_NAME"), resultSet.getString("UPDATE_RULE"),
                resultSet.getString("DELETE_RULE"), resultSet.getString("DEFERRABILITY")));
      }
    }
    collector.checkThat(actualKeys, is(expectedKeys));
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedKeys =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("pk_catalog_name #%d", i),
                format("pk_schema_name #%d", i),
                format("pk_table_name #%d", i),
                format("pk_column_name #%d", i),
                format("fk_catalog_name #%d", i),
                format("fk_schema_name #%d", i),
                format("fk_table_name #%d", i),
                format("fk_column_name #%d", i),
                String.valueOf(i),
                format("fk_key_name #%d", i),
                format("pk_key_name #%d", i),
                String.valueOf(i),
                String.valueOf(i),
                // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
                null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualKeys.add(columns);
      }
    }
    collector.checkThat(actualKeys, is(expectedKeys));
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedKeys =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("pk_catalog_name #%d", i),
                format("pk_schema_name #%d", i),
                format("pk_table_name #%d", i),
                format("pk_column_name #%d", i),
                format("fk_catalog_name #%d", i),
                format("fk_schema_name #%d", i),
                format("fk_table_name #%d", i),
                format("fk_column_name #%d", i),
                String.valueOf(i),
                format("fk_key_name #%d", i),
                format("pk_key_name #%d", i),
                String.valueOf(i),
                String.valueOf(i),
                // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
                null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        actualKeys.add(
            ImmutableList.of(
                resultSet.getString("PKTABLE_CAT"), resultSet.getString("PKTABLE_SCHEM"),
                resultSet.getString("PKTABLE_NAME"), resultSet.getString("PKCOLUMN_NAME"),
                resultSet.getString("FKTABLE_CAT"), resultSet.getString("FKTABLE_SCHEM"),
                resultSet.getString("FKTABLE_NAME"), resultSet.getString("FKCOLUMN_NAME"),
                resultSet.getString("KEY_SEQ"), resultSet.getString("FK_NAME"),
                resultSet.getString("PK_NAME"), resultSet.getString("UPDATE_RULE"),
                resultSet.getString("DELETE_RULE"), resultSet.getString("DEFERRABILITY")));
      }
    }
    collector.checkThat(actualKeys, is(expectedKeys));
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByIndices() throws SQLException {
    final List<List<String>> expectedSchemas =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("catalog_name #%d", i),
                format("schema_name #%d", i),
                format("table_name #%d", i),
                format("column_name #%d", i),
                String.valueOf(i),
                format("key_name #%d", i)})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> columns = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          columns.add(resultSet.getString(column + 1));
        }
        actualKeys.add(columns);
      }
    }
    collector.checkThat(actualKeys, is(expectedSchemas));
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByNames() throws SQLException {
    final List<List<String>> expectedSchemas =
        range(0, 10)
            .mapToObj(i -> new String[]{
                format("catalog_name #%d", i),
                format("schema_name #%d", i),
                format("table_name #%d", i),
                format("column_name #%d", i),
                String.valueOf(i),
                format("key_name #%d", i)})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualKeys = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Test")) {
      while (resultSet.next()) {
        actualKeys.add(
            ImmutableList.of(
                resultSet.getString("TABLE_CAT"), resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"), resultSet.getString("COLUMN_NAME"),
                resultSet.getString("KEY_SEQ"), resultSet.getString("PK_NAME")));
      }
    }
    collector.checkThat(actualKeys, is(expectedSchemas));
  }
}

